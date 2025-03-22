package main

import (
	"encoding/binary"
	"net"
	"testing"
)

// Test TCP server binding
func TestTCPConnection(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:9093")
	if err != nil {
		t.Fatalf("Failed to bind to test port: %v", err)
	}
	defer ln.Close()
}

// Test parsing request headers
func TestParseHeaders(t *testing.T) {
	// Valid request bytes
	validReq := make([]byte, 12)
	binary.BigEndian.PutUint16(validReq[4:6], 18)  // API Key
	binary.BigEndian.PutUint16(validReq[6:8], 3)   // API Version
	binary.BigEndian.PutUint32(validReq[8:12], 123) // Correlation ID

	headers := parseHeaders(validReq)
	if headers == nil {
		t.Fatal("parseHeaders returned nil for a valid request")
	}
	if headers.requestApiKey != 18 {
		t.Errorf("Expected API Key 18, got %d", headers.requestApiKey)
	}
	if headers.requestApiVersion != 3 {
		t.Errorf("Expected API Version 3, got %d", headers.requestApiVersion)
	}
	if headers.correlationId != 123 {
		t.Errorf("Expected Correlation ID 123, got %d", headers.correlationId)
	}

	// Invalid request (too short)
	shortReq := make([]byte, 10)
	headers = parseHeaders(shortReq)
	if headers != nil {
		t.Error("Expected nil for short request, got non-nil")
	}

	// Edge Case: Empty request
	emptyReq := []byte{}
	headers = parseHeaders(emptyReq)
	if headers != nil {
		t.Error("Expected nil for empty request, got non-nil")
	}

	// Edge Case: API version out of supported range
	invalidVersionReq := make([]byte, 12)
	binary.BigEndian.PutUint16(invalidVersionReq[4:6], 18)
	binary.BigEndian.PutUint16(invalidVersionReq[6:8], 999) // Unsupported API Version
	binary.BigEndian.PutUint32(invalidVersionReq[8:12], 123)

	headers = parseHeaders(invalidVersionReq)
	if headers.requestApiVersion != 999 {
		t.Errorf("Expected API Version 999, got %d", headers.requestApiVersion)
	}
}

// Test makeResponse function
func TestMakeResponse(t *testing.T) {
	headers := &RequestHeader{
		requestApiKey:     18,
		requestApiVersion: 3,
		correlationId:     123,
	}

	res := makeResponse(headers)
	if len(res) < 8 {
		t.Fatalf("Response is too short: %d bytes", len(res))
	}

	if binary.BigEndian.Uint32(res[4:8]) != uint32(123) {
		t.Errorf("Expected Correlation ID 123 in response, got %d", binary.BigEndian.Uint32(res[4:8]))
	}

	// Check error response for unsupported API key
	headers.requestApiKey = 999
	res = makeResponse(headers)
	if binary.BigEndian.Uint16(res[8:10]) != 42 {
		t.Errorf("Expected error code 42 for unsupported API key, got %d", binary.BigEndian.Uint16(res[8:10]))
	}

	// Edge Case: Minimum API Key Value
	headers.requestApiKey = 0
	res = makeResponse(headers)
	if binary.BigEndian.Uint16(res[8:10]) != 42 {
		t.Errorf("Expected error code 42 for unsupported API key 0, got %d", binary.BigEndian.Uint16(res[8:10]))
	}

	// Edge Case: Maximum API Key Value
	headers.requestApiKey = 32767
	res = makeResponse(headers)
	if binary.BigEndian.Uint16(res[8:10]) != 42 {
		t.Errorf("Expected error code 42 for unsupported max API key, got %d", binary.BigEndian.Uint16(res[8:10]))
	}
}

// Test request handling
func TestHandleRequest(t *testing.T) {
	// Valid request bytes
	validReq := make([]byte, 12)
	binary.BigEndian.PutUint16(validReq[4:6], 18)  // API Key
	binary.BigEndian.PutUint16(validReq[6:8], 3)   // API Version
	binary.BigEndian.PutUint32(validReq[8:12], 123) // Correlation ID

	res := handleRequest(validReq)
	if len(res) < 8 {
		t.Fatalf("handleRequest response too short: %d bytes", len(res))
	}
	if binary.BigEndian.Uint32(res[4:8]) != 123 {
		t.Errorf("Expected Correlation ID 123 in response, got %d", binary.BigEndian.Uint32(res[4:8]))
	}

	// Invalid request (too short)
	shortReq := make([]byte, 10)
	res = handleRequest(shortReq)
	if len(res) < 8 {
		t.Fatal("Expected valid error response for short request")
	}
	if binary.BigEndian.Uint16(res[8:10]) != 42 {
		t.Errorf("Expected error code 42 for invalid request, got %d", binary.BigEndian.Uint16(res[8:10]))
	}

	// Corrupt request data
	corruptReq := make([]byte, 12)
	for i := range corruptReq {
		corruptReq[i] = 0xFF
	}
	res = handleRequest(corruptReq)
	if len(res) < 8 {
		t.Fatal("Expected valid error response for corrupt request")
	}

	// Oversized request
	oversizedReq := make([]byte, 2048)
	binary.BigEndian.PutUint16(oversizedReq[4:6], 18)
	binary.BigEndian.PutUint16(oversizedReq[6:8], 3)
	binary.BigEndian.PutUint32(oversizedReq[8:12], 123)
	res = handleRequest(oversizedReq)
	if len(res) < 8 {
		t.Fatal("Expected valid response for oversized request")
	}
}
