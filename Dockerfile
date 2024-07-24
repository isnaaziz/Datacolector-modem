# Gunakan image golang sebagai base image
FROM golang:1.19

# Atur direktori kerja dalam container
WORKDIR /modem_go

# Salin go.mod dan go.sum
COPY go.mod go.sum ./

# Unduh dependency Go
RUN go mod download

# Salin kode sumber aplikasi
COPY . .

# Build aplikasi
RUN go build -o /modem_go/main .

# Eksekusi aplikasi
CMD ["/modem_go/main"]
