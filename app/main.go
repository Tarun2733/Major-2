package main
import (
	"encoding/binary"
	"fmt"
	"net"
	"os"
)
func main() {
	l, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		fmt.Println("Failed to bind to port 9092")
		os.Exit(1)
	}
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		go handleConnection(conn)
	}
}
func handleConnection(conn net.Conn) {
	for {
		req := make([]byte, 1024)
		conn.Read(req)
		res := hadleRequest(req)
		conn.Write(res)
	}
}
type RequestHeader struct {
	requestApiKey     int16
	requestApiVersion int16
	correlationId     int32
	clientId          []byte
	taggedFields      []byte
}
func parseHeaders(req []byte) *RequestHeader {
	return &RequestHeader{
		requestApiKey:     int16(binary.BigEndian.Uint16(req[4:8])),
		requestApiVersion: int16(binary.BigEndian.Uint16(req[6:8])),
		correlationId:     int32(binary.BigEndian.Uint32(req[8:12])),
	}
}
func makeResponse(reqHeaders *RequestHeader) []byte {
	res := make([]byte, 8)
	binary.BigEndian.PutUint32(res[4:8], uint32(reqHeaders.correlationId))
	// check api version
	if reqHeaders.requestApiVersion < 0 || reqHeaders.requestApiVersion > 4 {
		res = append(res, 0, 35)
		setMessageSize(res)
		return res
	}
	// ApiVersions
	if reqHeaders.requestApiKey == 18 {
		// https://forum.codecrafters.io/t/question-about-handle-apiversions-requests-stage/1743/4
		// ApiVersions Response (Version: CodeCrafters) =>
		//		error_code num_of_api_keys [api_keys] throttle_time_ms TAG_BUFFER
		//
		// error_code => INT16
		// num_of_api_keys => INT8
		// api_keys => api_key min_version max_version
		//   api_key => INT16
		// 	 min_version => INT16
		//   max_version => INT16
		// _tagged_fields
		// throttle_time_ms => INT32
		// _tagged_fields
		res = append(res,
			0, 0, // error_code no error
			3,     // num of api keys + 1
			0, 18, // api key GetApiVersions
			0, 3, // min version
			0, 4, // max version
			0,     // tagged fields
			0, 75, // api key DescribeTopicPartitions
			0, 0, // min version
			0, 0, // max version
			0,          // tagged fields
			0, 0, 0, 0, // throttle time
			0, //tagged fields
		)
	}
	setMessageSize(res)
	return res
}
func setMessageSize(res []byte) {
	binary.BigEndian.PutUint32(res[0:4], uint32(len(res)-4))
}
func hadleRequest(req []byte) []byte {
	headers := parseHeaders(req)
	return makeResponse(headers)
}