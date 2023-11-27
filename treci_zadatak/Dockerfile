FROM golang:alpine AS build
WORKDIR /app
COPY go.mod .
COPY simple_code.go .
RUN go build -o main
CMD ["/app/main"]
