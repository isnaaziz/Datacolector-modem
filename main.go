package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/joho/godotenv"
	_ "github.com/lib/pq"  // PostgreSQL driver
)

var (
	mqttBroker    string
	mqttUser      string
	mqttPassword  string
	mqttSubscribe string
	dbHost        string
	dbPort        string
	dbName        string
	dbUser        string
	dbPassword    string
	apiKey        string
)

type EventMessage struct {
	EventName string      `json:"event"`
	Tag       string      `json:"tag"`
	Value     interface{} `json:"value"`
	Status    bool        `json:"status"`
	Msg       string      `json:"msg"`
	Time      int64       `json:"time"`
	Sumber    string      `json:"sumber"`
}

var eventState sync.Map // A map to track the state of events for each sender



func getCurrentTimeMillis() int64 {
	return time.Now().UnixNano() / int64(time.Millisecond)
}

func setupDatabase() (*sql.DB, error) {


	postgresDSN := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		dbHost, dbPort, dbUser, dbPassword, dbName)

	db, err := sql.Open("postgres", postgresDSN)
	if err != nil {
		return nil, fmt.Errorf("error connecting to database: %v", err)
	}

	query := `
        CREATE TABLE IF NOT EXISTS mqtt_data (
            id SERIAL PRIMARY KEY,
            sender_id TEXT,
            message TEXT,
            timestamp TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
        )
    `
	_, err = db.Exec(query)
	if err != nil {
		return nil, fmt.Errorf("failed to create table: %v", err)
	}

	log.Println("Connected to PostgreSQL and ensured mqtt_data table exists")
	return db, nil
}

// Handel geolocation
func handleGeolocationEvent(db *sql.DB, messageStr string, senderID string, event string) {
	var messageData map[string]interface{}
	err := json.Unmarshal([]byte(messageStr), &messageData)
	if err != nil {
		log.Printf("Error unmarshaling message: %v", err)
		return
	}

	geolocationMessage, ok := messageData["message"].(string)
	if !ok {
		log.Println("Geolocation message not found in MQTT data.")
		return
	}

	log.Printf("Received geolocation message: %s\n", geolocationMessage)

	// Assuming the message format includes comma-separated sets of coordinates
	re := regexp.MustCompile(`\[(\d+),(\d+),([A-Fa-f0-9]+),([A-Fa-f0-9]+)\]`)
	matches := re.FindAllStringSubmatch(geolocationMessage, -1)

	if len(matches) == 0 {
		log.Println("No valid coordinate sets found.")
		return
	}

	cellTowers := make([]map[string]interface{}, 0, len(matches))
	for _, match := range matches {
		if len(match) == 5 {
			mcc := match[1]       // Mobile Country Code
			mnc := match[2]       // Mobile Network Code
			lacHex := match[3]    // Location Area Code in hex
			cellIDHex := match[4] // Cell ID in hex

			// Convert hex strings to integers
			lac, err := strconv.ParseInt(lacHex, 16, 64)
			if err != nil {
				log.Printf("Error parsing LAC: %v", err)
				continue
			}

			cellID, err := strconv.ParseInt(cellIDHex, 16, 64)
			if err != nil {
				log.Printf("Error parsing Cell ID: %v", err)
				continue
			}

			cellTower := map[string]interface{}{
				"cellId":            cellID,
				"locationAreaCode":  lac,
				"mobileCountryCode": mcc,
				"mobileNetworkCode": mnc,
			}

			log.Printf("Parsed Cell Tower - MCC: %s, MNC: %s, LAC: %d, CellID: %d\n", mcc, mnc, lac, cellID)

			cellTowers = append(cellTowers, cellTower)
		}
	}

	if len(cellTowers) == 0 {
		log.Println("Failed to parse any valid coordinate sets.")
		return
	}

	log.Printf("Parsed Cell Towers: %+v", cellTowers)

	// Example of how to proceed with sending to geolocation API and saving to database
	url := fmt.Sprintf("https://www.googleapis.com/geolocation/v1/geolocate?key=%s", apiKey)
	data := map[string]interface{}{
		"cellTowers": cellTowers,
	}

	dataBytes, err := json.Marshal(data)
	if err != nil {
		log.Printf("Error marshaling geolocation data: %v", err)
		return
	}

	log.Printf("Sending request to URL: %s with data: %s", url, string(dataBytes))

	resp, err := http.Post(url, "application/json", bytes.NewBuffer(dataBytes))
	if err != nil {
		log.Printf("Failed to send geolocation request: %v", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		var locationData map[string]interface{}
		err = json.NewDecoder(resp.Body).Decode(&locationData)
		if err != nil {
			log.Printf("Error decoding geolocation response: %v", err)
			return
		}

		fmt.Println("Geolocation Result:")
		if location, ok := locationData["location"].(map[string]interface{}); ok {
			if lat, ok := location["lat"].(float64); ok {
				if lng, ok := location["lng"].(float64); ok {
					fmt.Printf("Latitude: %f, Longitude: %f\n", lat, lng)
				}
			}
		} else {
			log.Println("Location data not found in response.")
		}

		// Format data point
		geolocationMessage := EventMessage{
			EventName: event,
			Tag:       fmt.Sprintf("geolocation_%s", senderID),
			Value:     locationData,
			Status:    true,
			Sumber:    senderID,
		}

		sendDataPoint(geolocationMessage)

		_, err = db.Exec("INSERT INTO mqtt_data (sender_id, message) VALUES ($1, $2)", senderID, string(dataBytes))
		if err != nil {
			log.Printf("Error saving geolocation data to database: %v", err)
		}
	} else {
		log.Printf("Failed to retrieve geolocation, status code: %d", resp.StatusCode)

		var responseBody map[string]interface{}
		err = json.NewDecoder(resp.Body).Decode(&responseBody)
		if err != nil {
			log.Printf("Error decoding error response: %v", err)
		} else {
			log.Printf("Error response: %+v", responseBody)
		}
	}
}

// Handel Temperature
func handleTemperatureEvent(db *sql.DB, senderID, message string, event string) {
	var msgData map[string]interface{}
	if err := json.Unmarshal([]byte(message), &msgData); err != nil {
		log.Printf("Error unmarshalling temperature event message: %v", err)
		return
	}

	msg, ok := msgData["message"]
	if !ok {
		log.Println("Error: 'message' field not found in msgData")
		return
	}

	timestampStr, ok := msgData["timestamp"].(string)
	if !ok {
		log.Println("Error: 'timestamp' field not found or not a string in msgData")
		return
	}
	timestampFloat, err := strconv.ParseFloat(timestampStr, 64)
	if err != nil {
		log.Printf("Error converting 'timestamp' to float64: %v", err)
		return
	}
	timestamp := int64(timestampFloat)

	// Convert 10-digit Unix timestamp to 13-digit timestamp in milliseconds
	if len(timestampStr) == 10 {
		timestamp *= 1000
	}

	temperatureMessage := EventMessage{
		EventName: event,
		Tag:       fmt.Sprintf("temperature_%s", senderID),
		Value:     msg,
		Status:    true,
		Msg:       message,
		Time:      timestamp,
		Sumber:    senderID,
	}

	if temperatureMessage != (EventMessage{}) {
		processAndSaveData(db, temperatureMessage)
		sendDataPoint(temperatureMessage)
	} else {
		log.Println("Temperature message not found in MQTT data.")
	}
}

// Handel Backup Mode
func handlePowerBackupModeEvent(db *sql.DB, senderID, message, event string) {
	var msgData map[string]interface{}
	if err := json.Unmarshal([]byte(message), &msgData); err != nil {
		log.Printf("Error unmarshalling power backup mode event message: %v", err)
		return
	}

	timestampStr, ok := msgData["timestamp"].(string)
	if !ok {
		log.Println("Error: 'timestamp' field not found or not a string in msgData")
		return
	}
	timestampFloat, err := strconv.ParseFloat(timestampStr, 64)
	if err != nil {
		log.Printf("Error converting 'timestamp' to float64: %v", err)
		return
	}
	timestamp := int64(timestampFloat)

	// Convert 10-digit Unix timestamp to 13-digit timestamp in milliseconds
	if len(timestampStr) == 10 {
		timestamp *= 1000
	}

	powerBackupMessage := EventMessage{
		EventName: event,
		Tag:       fmt.Sprintf("power_modem_%s", senderID),
		Value:     1,
		Status:    true,
		Msg:       message,
		Time:      timestamp,
		Sumber:    senderID,
	}

	if powerBackupMessage != (EventMessage{}) {
		processAndSaveData(db, powerBackupMessage)
		sendDataPoint(powerBackupMessage)
		eventState.Store(senderID+"_POWER_BACKUP_MODE", true)
		checkCombinedConditions(db, senderID, message, event)
	} else {
		log.Println("Power backup mode message not found in MQTT data.")
	}
}

// Handel Power Restore
func handlePowerRestoreModeEvent(db *sql.DB, senderID, message string, event string) {
	var msgData map[string]interface{}
	if err := json.Unmarshal([]byte(message), &msgData); err != nil {
		log.Printf("Error unmarshalling power restore mode event message: %v", err)
		return
	}

	timestampStr, ok := msgData["timestamp"].(string)
	if !ok {
		log.Println("Error: 'timestamp' field not found or not a string in msgData")
		return
	}
	timestampFloat, err := strconv.ParseFloat(timestampStr, 64)
	if err != nil {
		log.Printf("Error converting 'timestamp' to float64: %v", err)
		return
	}
	timestamp := int64(timestampFloat)

	// Convert 10-digit Unix timestamp to 13-digit timestamp in milliseconds
	if len(timestampStr) == 10 {
		timestamp *= 1000
	}

	powerRestoreMessage := EventMessage{
		EventName: event,
		Tag:       fmt.Sprintf("power_modem_%s", senderID),
		Value:     0,
		Status:    true,
		Msg:       message,
		Time:      timestamp,
		Sumber:    senderID,
	}

	if powerRestoreMessage != (EventMessage{}) {
		processAndSaveData(db, powerRestoreMessage)
		sendDataPoint(powerRestoreMessage)
		eventState.Store(senderID+"_POWER_RESTORE_MODE", true)
		checkCombinedConditions(db, senderID, message, event)
	} else {
		log.Println("Power restore mode message not found in MQTT data.")
	}

}

// Handel Status Modem On
func handleStatusModemOn(db *sql.DB, senderID, message string, event string) {
	var msgData map[string]interface{}
	if err := json.Unmarshal([]byte(message), &msgData); err != nil {
		log.Printf("Error unmarshalling status modem on  event message: %v", err)
		return
	}

	timestampStr, ok := msgData["timestamp"].(string)
	if !ok {
		log.Println("Error: 'timestamp' field not found or not a string in msgData")
		return
	}
	timestampFloat, err := strconv.ParseFloat(timestampStr, 64)
	if err != nil {
		log.Printf("Error converting 'timestamp' to float64: %v", err)
		return
	}
	timestamp := int64(timestampFloat)

	// Convert 10-digit Unix timestamp to 13-digit timestamp in milliseconds
	if len(timestampStr) == 10 {
		timestamp *= 1000
	}

	statusModemOnMessage := EventMessage{
		EventName: event,
		Tag:       fmt.Sprintf("status_modem_%s", senderID),
		Value:     1,
		Status:    true,
		Msg:       message,
		Time:      timestamp,
		Sumber:    senderID,
	}

	if statusModemOnMessage != (EventMessage{}) {
		processAndSaveData(db, statusModemOnMessage)
		sendDataPoint(statusModemOnMessage)
	} else {
		log.Println("Power restore mode message not found in MQTT data.")
	}
}

// Handel Status Modem Off
func handleStatusModemOff(db *sql.DB, senderID, message string, event string) {
	var msgData map[string]interface{}
	if err := json.Unmarshal([]byte(message), &msgData); err != nil {
		log.Printf("Error unmarshalling status modem off event message: %v", err)
		return
	}

	timestampStr, ok := msgData["timestamp"].(string)
	if !ok {
		log.Println("Error: 'timestamp' field not found or not a string in msgData")
		return
	}
	timestampFloat, err := strconv.ParseFloat(timestampStr, 64)
	if err != nil {
		log.Printf("Error converting 'timestamp' to float64: %v", err)
		return
	}
	timestamp := int64(timestampFloat)

	// Convert 10-digit Unix timestamp to 13-digit timestamp in milliseconds
	if len(timestampStr) == 10 {
		timestamp *= 1000
	}

	statusModemOffMessage := EventMessage{
		EventName: event,
		Tag:       fmt.Sprintf("status_modem_%s", senderID),
		Value:     0,
		Status:    true,
		Msg:       message,
		Time:      timestamp,
		Sumber:    senderID,
	}

	if statusModemOffMessage != (EventMessage{}) {
		processAndSaveData(db, statusModemOffMessage)
		sendDataPoint(statusModemOffMessage)
	} else {
		log.Println("Status Modem OFF message not found in MQTT data.")
	}
}

// Combined Condition Check Function Power PLN
func checkCombinedConditions(db *sql.DB, senderID, message, event string) {
	alarmEvent, _ := eventState.Load(senderID + "_ALARM_METER_DEVICE")
	powerEvent, _ := eventState.Load(senderID + "_POWER_BACKUP_MODE")

	if alarmEvent != nil && powerEvent != nil {
		connectionMissing := alarmEvent.(bool)
		powerBackupMode := powerEvent.(bool)

		if connectionMissing && powerBackupMode {
			log.Println("Both POWER_BACKUP_MODE and CONNECTION_MISSING detected.")
			handlePowerPln(db, senderID, message, event)
			// Reset the state after processing

		} else {
			log.Println("POWER_BACKUP_MODE detected without CONNECTION_MISSING.")
		}
	}
}

// handlePowerPln processes POWER_BACKUP_MODE events and checks for CONNECTION_MISSING from ALARM_METER_DEVICE events
func handlePowerPln(db *sql.DB, senderID, message, event string) {
	var msgData map[string]interface{}
	if err := json.Unmarshal([]byte(message), &msgData); err != nil {
		log.Printf("Error unmarshalling status modem off event message: %v", err)
		return
	}

	timestampStr, ok := msgData["timestamp"].(string)
	if !ok {
		log.Println("Error: 'timestamp' field not found or not a string in msgData")
		return
	}
	timestampFloat, err := strconv.ParseFloat(timestampStr, 64)
	if err != nil {
		log.Printf("Error converting 'timestamp' to float64: %v", err)
		return
	}
	timestamp := int64(timestampFloat)

	// Convert 10-digit Unix timestamp to 13-digit timestamp in milliseconds
	if len(timestampStr) == 10 {
		timestamp *= 1000
	}

	statusPowerPlnMessage := EventMessage{
		EventName: "POWER_PLN",
		Tag:       fmt.Sprintf("power_pln_%s", senderID),
		Value:     1,
		Status:    true,
		Msg:       message,
		Time:      timestamp,
		Sumber:    senderID,
	}

	if event == "POWER_BACKUP_MODE" || event == "ALARM_METER_DEVICE" {
		if event == "POWER_BACKUP_MODE" {
			eventState.Store(senderID+"_POWER_BACKUP_MODE", true)
		} else if event == "ALARM_METER_DEVICE" {
			eventState.Store(senderID+"_ALARM_METER_DEVICE", true)
		}

		alarmEvent, _ := eventState.Load(senderID + "_ALARM_METER_DEVICE")
		powerEvent, _ := eventState.Load(senderID + "_POWER_BACKUP_MODE")

		connectionMissing := alarmEvent != nil && alarmEvent.(bool)
		powerBackupMode := powerEvent != nil && powerEvent.(bool)

		if connectionMissing && powerBackupMode {
			log.Println("Both POWER_BACKUP_MODE and CONNECTION_MISSING detected.")
			processAndSaveData(db, statusPowerPlnMessage)
			sendDataPoint(statusPowerPlnMessage)

			// Call handleClearPowerPlnEvent for related events

		} else {
			log.Println("POWER_BACKUP_MODE detected without CONNECTION_MISSING.")
		}
	} else if event == "POWER_RESTORE_MODE" || event == "CLEAR_ALARM_METER_DEVICE" {
		handleClearPowerPlnEvent(db, senderID, message, event)
	} else {
		log.Println("Unhandled event type in handlePowerPln.")
	}
}

// Handel Clear Power Pln
func handleClearPowerPlnEvent(db *sql.DB, senderID, message string, event string) {
	log.Printf("Received message: %s, event: %s", message, event)

	var msgData map[string]interface{}
	if err := json.Unmarshal([]byte(message), &msgData); err != nil {
		log.Printf("Error unmarshalling clear power pln event message: %v", err)
		return
	}

	timestampStr, ok := msgData["timestamp"].(string)
	if !ok {
		log.Println("Error: 'timestamp' field not found or not a string in msgData")
		return
	}
	timestampFloat, err := strconv.ParseFloat(timestampStr, 64)
	if err != nil {
		log.Printf("Error converting 'timestamp' to float64: %v", err)
		return
	}
	timestamp := int64(timestampFloat)

	// Convert 10-digit Unix timestamp to 13-digit timestamp in milliseconds
	if len(timestampStr) == 10 {
		timestamp *= 1000
	}

	statusClearPowerPlnMessage := EventMessage{
		EventName: "POWER_PLN",
		Tag:       fmt.Sprintf("power_pln_%s", senderID),
		Value:     0,
		Status:    true,
		Msg:       message,
		Time:      timestamp,
		Sumber:    senderID,
	}

	switch event {
	case "POWER_RESTORE_MODE":
		eventState.Store(senderID+"_POWER_RESTORE_MODE", true)
		log.Println("POWER_RESTORE_MODE event detected and stored.")
	case "CLEAR_ALARM_METER_DEVICE":
		eventState.Store(senderID+"_CLEAR_ALARM_METER_DEVICE", true)
		log.Println("CLEAR_ALARM_METER_DEVICE event detected and stored.")
	default:
		log.Printf("Unhandled event type in handleClearPowerPlnEvent: %s", event)
		return
	}

	// Log to check if eventState contains the correct values
	alarmEvent, alarmEventOk := eventState.Load(senderID + "_CLEAR_ALARM_METER_DEVICE")
	powerEvent, powerEventOk := eventState.Load(senderID + "_POWER_RESTORE_MODE")

	clearAlarmMeterDevice := alarmEventOk && alarmEvent.(bool)
	powerRestoreMode := powerEventOk && powerEvent.(bool)

	log.Printf("Loaded states - clearAlarmMeterDevice: %v, powerRestoreMode: %v", clearAlarmMeterDevice, powerRestoreMode)

	if clearAlarmMeterDevice || powerRestoreMode {
		log.Println("Either POWER_RESTORE_MODE or CLEAR_ALARM_METER_DEVICE detected. Processing data.")

		processAndSaveData(db, statusClearPowerPlnMessage)
		sendDataPoint(statusClearPowerPlnMessage)

		// Reset the state after processing
		if clearAlarmMeterDevice {
			eventState.Delete(senderID + "_CLEAR_ALARM_METER_DEVICE")
			log.Println("Resetting state for CLEAR_ALARM_METER_DEVICE")
		}
		if powerRestoreMode {
			eventState.Delete(senderID + "_POWER_RESTORE_MODE")
			log.Println("Resetting state for POWER_RESTORE_MODE")
		}
	} else {
		log.Println("No relevant state detected for POWER_RESTORE_MODE or CLEAR_ALARM_METER_DEVICE.")
	}
}

// Handel Alarm Temper
func handleAlarmMeterDeviceTemperEvent(db *sql.DB, senderID, message string, event string) {
	var msgData map[string]interface{}
	if err := json.Unmarshal([]byte(message), &msgData); err != nil {
		log.Printf("Error unmarshalling temperature event message: %v", err)
		return
	}

	timestampStr, ok := msgData["timestamp"].(string)
	if !ok {
		log.Println("Error: 'timestamp' field not found or not a string in msgData")
		return
	}
	timestampFloat, err := strconv.ParseFloat(timestampStr, 64)
	if err != nil {
		log.Printf("Error converting 'timestamp' to float64: %v", err)
		return
	}
	timestamp := int64(timestampFloat)

	// Convert 10-digit Unix timestamp to 13-digit timestamp in milliseconds
	if len(timestampStr) == 10 {
		timestamp *= 1000
	}

	alarmTemperMessage := EventMessage{
		EventName: event,
		Tag:       fmt.Sprintf("alarm_meter_temper_%s", senderID),
		Value:     1,
		Status:    true,
		Msg:       message,
		Time:      timestamp,
		Sumber:    senderID,
	}

	if alarmTemperMessage != (EventMessage{}) {
		processAndSaveData(db, alarmTemperMessage)
		sendDataPoint(alarmTemperMessage)
	} else {
		log.Println("Alarm meter device temper message not found in MQTT data.")
	}

}

// Handel Clear Alarm Temper
func handleClearAlarmMeterDeviceTemperEvent(db *sql.DB, senderID, message string, event string) {
	var msgData map[string]interface{}
	if err := json.Unmarshal([]byte(message), &msgData); err != nil {
		log.Printf("Error unmarshalling Clear Alarm Meter Temper event message: %v", err)
		return
	}

	timestampStr, ok := msgData["timestamp"].(string)
	if !ok {
		log.Println("Error: 'timestamp' field not found or not a string in msgData")
		return
	}
	timestampFloat, err := strconv.ParseFloat(timestampStr, 64)
	if err != nil {
		log.Printf("Error converting 'timestamp' to float64: %v", err)
		return
	}
	timestamp := int64(timestampFloat)

	// Convert 10-digit Unix timestamp to 13-digit timestamp in milliseconds
	if len(timestampStr) == 10 {
		timestamp *= 1000
	}

	clearAlarmTemperMessage := EventMessage{
		EventName: event,
		Tag:       fmt.Sprintf("alarm_meter_temper_%s", senderID),
		Value:     0,
		Status:    true,
		Msg:       message,
		Time:      timestamp,
		Sumber:    senderID,
	}

	if clearAlarmTemperMessage != (EventMessage{}) {
		processAndSaveData(db, clearAlarmTemperMessage)
		sendDataPoint(clearAlarmTemperMessage)
	} else {
		log.Println("Clear alarm meter device temper message not found in MQTT data.")
	}

}

var alarmSuhu int

// Handel Alarm Temperature
func handleAlarmTemperatureEvent(db *sql.DB, senderID, message string, event string) {
	var msgData map[string]interface{}
	if err := json.Unmarshal([]byte(message), &msgData); err != nil {
		log.Printf("Error unmarshalling alarm temperature event message: %v", err)
		return
	}

	timestampStr, ok := msgData["timestamp"].(string)
	if !ok {
		log.Println("Error: 'timestamp' field not found or not a string in msgData")
		return
	}
	timestampFloat, err := strconv.ParseFloat(timestampStr, 64)
	if err != nil {
		log.Printf("Error converting 'timestamp' to float64: %v", err)
		return
	}
	timestamp := int64(timestampFloat)

	// Convert 10-digit Unix timestamp to 13-digit timestamp in milliseconds
	if len(timestampStr) == 10 {
		timestamp *= 1000
	}

	alarmTemperatureMessage := EventMessage{
		EventName: event,
		Tag:       fmt.Sprintf("alarm_temperature_%s", senderID),
		Value:     1,
		Status:    true,
		Msg:       message,
		Time:      timestamp,
		Sumber:    senderID,
	}

	if alarmTemperatureMessage != (EventMessage{}) {
		processAndSaveData(db, alarmTemperatureMessage)
		sendDataPoint(alarmTemperatureMessage)
	} else {
		log.Println("Alarm temperature mode message not found in MQTT data.")
	}
}

// Handel Clear Alarm Temperature
func handleClearAlarmTemperatureEvent(db *sql.DB, senderID, message string, event string) {
	var msgData map[string]interface{}
	if err := json.Unmarshal([]byte(message), &msgData); err != nil {
		log.Printf("Error unmarshalling clear alarm temperature event message: %v", err)
		return
	}

	timestampStr, ok := msgData["timestamp"].(string)
	if !ok {
		log.Println("Error: 'timestamp' field not found or not a string in msgData")
		return
	}
	timestampFloat, err := strconv.ParseFloat(timestampStr, 64)
	if err != nil {
		log.Printf("Error converting 'timestamp' to float64: %v", err)
		return
	}
	timestamp := int64(timestampFloat)

	// Convert 10-digit Unix timestamp to 13-digit timestamp in milliseconds
	if len(timestampStr) == 10 {
		timestamp *= 1000
	}

	clearAlarmTemperatureMessage := EventMessage{
		EventName: event,
		Tag:       fmt.Sprintf("alarm_temperature_%s", senderID),
		Value:     0,
		Status:    true,
		Msg:       message,
		Time:      timestamp,
		Sumber:    senderID,
	}

	if clearAlarmTemperatureMessage != (EventMessage{}) {
		processAndSaveData(db, clearAlarmTemperatureMessage)
		sendDataPoint(clearAlarmTemperatureMessage)
	} else {
		log.Println("Clear Alarm temperature mode message not found in MQTT data.")
	}
}

// Handel Set Temperature
func handleSetTemperatureEvents(db *sql.DB, senderID, message string) {
	var msgData map[string]interface{}
	if err := json.Unmarshal([]byte(message), &msgData); err != nil {
		log.Printf("Error unmarshalling status modem on  event message: %v", err)
		return
	}

	timestampStr, ok := msgData["timestamp"].(string)
	if !ok {
		log.Println("Error: 'timestamp' field not found or not a string in msgData")
		return
	}
	timestampFloat, err := strconv.ParseFloat(timestampStr, 64)
	if err != nil {
		log.Printf("Error converting 'timestamp' to float64: %v", err)
		return
	}
	timestamp := int64(timestampFloat)

	// Convert 10-digit Unix timestamp to 13-digit timestamp in milliseconds
	if len(timestampStr) == 10 {
		timestamp *= 1000
	}

	setTemperatureMessage := EventMessage{
		Tag:    fmt.Sprintf("%s_set_temperature", senderID),
		Value:  findNumbersInSentences(msgData["message"].(string)),
		Status: true,
		Msg:    message,
		Time:   timestamp,
		Sumber: senderID,
	}

	if setTemperatureMessage != (EventMessage{}) {
		processAndSaveData(db, setTemperatureMessage)
		sendDataPoint(setTemperatureMessage)
	} else {
		log.Println("Set temperature message not found in MQTT data.")
	}
}

// Handel Alarm Connection Missing
func handleAlarmMeterDeviceEvent(db *sql.DB, senderID, message, event string) {
	var msgData map[string]interface{}
	if err := json.Unmarshal([]byte(message), &msgData); err != nil {
		log.Printf("Error unmarshalling ALARM_METER_DEVICE event message: %v", err)
		return
	}

	timestampStr, ok := msgData["timestamp"].(string)
	if !ok {
		log.Println("Error: 'timestamp' field not found or not a string in msgData")
		return
	}
	timestampFloat, err := strconv.ParseFloat(timestampStr, 64)
	if err != nil {
		log.Printf("Error converting 'timestamp' to float64: %v", err)
		return
	}
	timestamp := int64(timestampFloat)

	// Convert 10-digit Unix timestamp to 13-digit timestamp in milliseconds
	if len(timestampStr) == 10 {
		timestamp *= 1000
	}

	alarmMeterDeviceMessage := EventMessage{
		EventName: event,
		Tag:       fmt.Sprintf("alarm_connection_missing_%s", senderID),
		Value:     1,
		Status:    true,
		Msg:       message,
		Time:      timestamp,
		Sumber:    senderID,
	}

	if alarmMeterDeviceMessage != (EventMessage{}) {
		processAndSaveData(db, alarmMeterDeviceMessage)
		sendDataPoint(alarmMeterDeviceMessage)
		eventState.Store(senderID+"_ALARM_METER_DEVICE", true)
		checkCombinedConditions(db, senderID, message, event)
	} else {
		log.Println("Alarm meter device mode message not found in MQTT data.")
	}
}

// Handel Clear Alarm Connection Missing
func handleClearAlarmMeterDeviceEvent(db *sql.DB, senderID, message, event string) {
	var msgData map[string]interface{}
	if err := json.Unmarshal([]byte(message), &msgData); err != nil {
		log.Printf("Error unmarshalling CLEAR_ALARM_METER_DEVICE event message: %v", err)
		return
	}

	timestampStr, ok := msgData["timestamp"].(string)
	if !ok {
		log.Println("Error: 'timestamp' field not found or not a string in msgData")
		return
	}
	timestampFloat, err := strconv.ParseFloat(timestampStr, 64)
	if err != nil {
		log.Printf("Error converting 'timestamp' to float64: %v", err)
		return
	}
	timestamp := int64(timestampFloat)

	// Convert 10-digit Unix timestamp to 13-digit timestamp in milliseconds
	if len(timestampStr) == 10 {
		timestamp *= 1000
	}

	clearAlarmMeterDeviceMessage := EventMessage{
		EventName: event,
		Tag:       fmt.Sprintf("alarm_connection_missing_%s", senderID),
		Value:     0,
		Status:    true,
		Msg:       message,
		Time:      timestamp,
		Sumber:    senderID,
	}

	if clearAlarmMeterDeviceMessage != (EventMessage{}) {
		processAndSaveData(db, clearAlarmMeterDeviceMessage)
		sendDataPoint(clearAlarmMeterDeviceMessage)
		eventState.Store(senderID+"_ALARM_METER_DEVICE", true)
		checkCombinedConditions(db, senderID, message, event)
	} else {
		log.Println("Alarm meter device mode message not found in MQTT data.")
	}
}

func findNumbersInSentences(s string) int {
	re := regexp.MustCompile(`\d+`)
	matches := re.FindAllString(s, -1)
	if len(matches) > 0 {
		value, err := strconv.Atoi(matches[0])
		if err == nil {
			return value
		}
	}
	return 0
}

func processAndSaveData(db *sql.DB, data EventMessage) {
	// Convert the timestamp from milliseconds to seconds before passing it to the SQL query
	_, err := db.Exec("INSERT INTO mqtt_data (sender_id, message, timestamp) VALUES ($1, $2, to_timestamp($3 / 1000.0))",
		data.Sumber, data.Msg, data.Time)
	if err != nil {
		log.Printf("Error saving data to database: %v", err)
	} else {
		log.Println("Data saved successfully")
	}
}

func sendDataPoint(message EventMessage) {
	datapoints := map[string]interface{}{
		"event":    message.EventName,
		"tag":      message.Tag,
		"value":    message.Value,
		"time":     message.Time,
		"id_modem": message.Sumber,
	}

	log.Printf("Data to send: %v", datapoints)

	payload, err := json.Marshal(datapoints)
	if err != nil {
		log.Printf("Failed to marshal datapoint: %v", err)
		return
	}

	token := mqttClient.Publish("DATAPOINTS", 0, false, payload)
	token.Wait()
	if token.Error() != nil {
		log.Printf("Failed to send datapoint: %v", token.Error())
	}
}

var mqttClient mqtt.Client

func main() {


	// Load environment variables from .env file
	err := godotenv.Load()
	if err != nil {
		log.Fatalf("Error loading .env file: %v", err)
	}

	// Initialize global variables from environment variables
	mqttBroker = os.Getenv("MQTT_BROKER")
	mqttUser = os.Getenv("MQTT_USER")
	mqttPassword = os.Getenv("MQTT_PASSWORD")
	mqttSubscribe = os.Getenv("MQTT_SUBSCRIBE")
	dbHost = os.Getenv("DB_HOST")
	dbPort = os.Getenv("DB_PORT")
	dbName = os.Getenv("DB_NAME")
	dbUser = os.Getenv("DB_USER")
	dbPassword = os.Getenv("DB_PASSWORD")
	apiKey = os.Getenv("API_KEY")

	// Setup database connection
	db, err := setupDatabase()
	if err != nil {
		log.Fatalf("Failed to set up database: %v", err)
	}
	defer db.Close()

	opts := mqtt.NewClientOptions().AddBroker(mqttBroker).SetClientID("modem_client")
	opts.SetUsername(mqttUser)
	opts.SetPassword(mqttPassword)
	opts.SetDefaultPublishHandler(func(client mqtt.Client, msg mqtt.Message) {
		log.Printf("Received message: %s from topic: %s\n", msg.Payload(), msg.Topic())
	})

	mqttClient = mqtt.NewClient(opts)
	if token := mqttClient.Connect(); token.Wait() && token.Error() != nil {
		log.Fatalf("Failed to connect to MQTT broker: %v", token.Error())
	}

	if token := mqttClient.Subscribe(mqttSubscribe, 1, func(client mqtt.Client, msg mqtt.Message) {
		log.Printf("Message received on topic %s: %s\n", msg.Topic(), msg.Payload())

		var msgData map[string]interface{}
		if err := json.Unmarshal(msg.Payload(), &msgData); err != nil {
			log.Printf("Error unmarshalling MQTT message: %v\nPayload: %s", err, msg.Payload())
			return
		}

		event, ok := msgData["event"].(string)
		if !ok {
			log.Printf("Event type not found in message: %s\n", msg.Payload())
			return
		}
		msgData["event"] = event
		senderID := strings.Split(msg.Topic(), "/")[2]
		message := string(msg.Payload())

		timestamp, err := getTimestamp(msgData)
		if err != nil {
			log.Printf("Error processing timestamp: %v\nMessage Data: %+v", err, msgData)
			return
		}

		log.Printf("Processed timestamp: %v", timestamp)

		switch event {
		case "TEMPERATURE":
			handleTemperatureEvent(db, senderID, message, event)
		case "ALARM_METER_TEMPER":
			handleAlarmMeterDeviceTemperEvent(db, senderID, message, event)
		case "CLEAR_ALARM_METER_TEMPER":
			handleClearAlarmMeterDeviceTemperEvent(db, senderID, message, event)
		case "POWER_BACKUP_MODE":
			handlePowerBackupModeEvent(db, senderID, message, event)
		case "POWER_RESTORE_MODE":
			handlePowerRestoreModeEvent(db, senderID, message, event)
		case "STATUS_MODEM_ON":
			handleStatusModemOn(db, senderID, message, event)
		case "STATUS_MODEM_OFF":
			handleStatusModemOff(db, senderID, message, event)
		case "ALARM_TEMPERATURE":
			handleAlarmTemperatureEvent(db, senderID, message, event)
		case "CLEAR_ALARM_TEMPERATURE":
			handleClearAlarmTemperatureEvent(db, senderID, message, event)
		case "SET_TEMPERATURE":
			handleSetTemperatureEvents(db, senderID, message)
		case "ALARM_METER_DEVICE":
			handleAlarmMeterDeviceEvent(db, senderID, message, event)
		case "CLEAR_ALARM_METER_DEVICE":
			handleClearAlarmMeterDeviceEvent(db, senderID, message, event)
		case "GEOLOCATION":
			handleGeolocationEvent(db, message, senderID, event)
		default:
			log.Printf("Unhandled message type in topic %s: %s\n", msg.Topic(), msg.Payload())
		}

	}); token.Wait() && token.Error() != nil {
		log.Fatalf("Failed to subscribe to topic: %v", token.Error())
	}

	select {}
}

func getTimestamp(msgData map[string]interface{}) (interface{}, error) {
	if timestamp, ok := msgData["timestamp"].(float64); ok {
		return int64(timestamp), nil
	} else if timestamp, ok := msgData["timestamp"].(string); ok {
		return timestamp, nil
	} else {
		return nil, fmt.Errorf("'timestamp' field not found or not a valid type in msgData")
	}
}
