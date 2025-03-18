package main

import (
	"encoding/binary"
	"fmt"
	"net"
	"os"
)

// Entry point of the Kafka clone
func main() {
	// Start listening on TCP port 9092 (default Kafka port)
	l, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		fmt.Println("Failed to bind to port 9092")
		os.Exit(1)
	}
	fmt.Println("Kafka clone is running on port 9092...")

	for {
		conn, err := l.Accept() // Accept new connections
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			continue
		}
		go handleConnection(conn) // Handle each connection concurrently
	}
}

// Function to handle client connections
func handleConnection(conn net.Conn) {
	defer conn.Close() // Ensure the connection is closed when done
	for {
		req := make([]byte, 1024)
		n, err := conn.Read(req)
		if err != nil {
			fmt.Println("Connection closed by client")
			return
		}

		// Process request and generate response
		res := handleRequest(req[:n])
		conn.Write(res)
	}
}

// Struct to store Kafka request headers
type RequestHeader struct {
	requestApiKey     int16
	requestApiVersion int16
	correlationId     int32
}

// Function to parse headers from Kafka-like requests
func parseHeaders(req []byte) *RequestHeader {
	return &RequestHeader{
		requestApiKey:     int16(binary.BigEndian.Uint16(req[4:6])),
		requestApiVersion: int16(binary.BigEndian.Uint16(req[6:8])),
		correlationId:     int32(binary.BigEndian.Uint32(req[8:12])),
	}
}

// Function to create Kafka-like responses
func makeResponse(reqHeaders *RequestHeader) []byte {
	res := make([]byte, 8)
	binary.BigEndian.PutUint32(res[4:8], uint32(reqHeaders.correlationId))

	// If API version is unsupported, return error
	if reqHeaders.requestApiVersion < 0 || reqHeaders.requestApiVersion > 4 {
		res = append(res, 0, 35) // Error code 35 (unsupported version)
		setMessageSize(res)
		return res
	}

	// Handle API key 18 (GetApiVersions request)
	if reqHeaders.requestApiKey == 18 {
		res = append(res,
			0, 0, // No error
			3,    // Number of API keys
			0, 18, 0, 3, 0, 4, // API key 18 (GetApiVersions), versions 3-4
			0, 75, 0, 0, 0, 0, // API key 75 (DescribeTopicPartitions)
			0, 0, 0, 0, // Throttle time
			0, // Tagged fields
		)
	}

	setMessageSize(res)
	return res
}

// Function to set the message size at the beginning of the response
func setMessageSize(res []byte) {
	binary.BigEndian.PutUint32(res[0:4], uint32(len(res)-4))
}

// Function to process client requests
func handleRequest(req []byte) []byte {
	headers := parseHeaders(req)
	return makeResponse(headers)
}
