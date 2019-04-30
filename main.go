package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"time"

	nats "github.com/nats-io/go-nats"
	uuid "github.com/satori/go.uuid"
)

type IOTData struct {
	MsgId     string
	DeviceId  string
	Reading1  int
	Reading2  int
	CreatedAt int64
}

var iotDataList = make([]*IOTData, 0)

func main() {

	//Device Simulation
	//Device publishing NATS messages at every 2 seconds
	go func() {
		natClient, _ := nats.Connect(nats.DefaultURL)
		for {
			id := uuid.NewV4()
			msgId := id.String()
			iotData := &IOTData{
				MsgId:     msgId,
				DeviceId:  "AWQ123",
				Reading1:  rand.Intn(1000),
				Reading2:  rand.Intn(100),
				CreatedAt: time.Now().Unix()}

			data, _ := json.Marshal(iotData)
			natClient.Publish("IOT.deviceL", data)
			time.Sleep(2 * time.Second)
			fmt.Println("Ticker after 2 second")
		}
	}()

	//NATS Message Subscriber
	//Listening message and inserting new data to the iotDataList (a mock database)
	go func() {
		var natServer *nats.Conn
		var err1 error
		for {
			natServer, err1 = nats.Connect(nats.DefaultURL)
			if err1 != nil {
				fmt.Printf("Error while connecting to NATS, backing off for a sec... (error: %s)", err1)
				time.Sleep(1 * time.Second)
				continue
			}
			fmt.Println("Connected NATS")
			break

		}
		natServer.Subscribe("IOT.deviceL", func(m *nats.Msg) {
			iotData := IOTData{}
			_ = json.Unmarshal(m.Data, &iotData)
			iotDataList = append(iotDataList, &iotData)
			fmt.Println("Received MsgId:", iotData.MsgId, " DeviceId:", iotData.DeviceId, " Reading1:", iotData.Reading1, " Reading2:", iotData.Reading2)
		})
	}()
	http.HandleFunc("/device1-data", IOTServer)
	http.ListenAndServe(":8086", nil) // set listen port
}

func IOTServer(rw http.ResponseWriter, req *http.Request) {

	//Writing response with all the data in iotDataList in the JSON format.
	jsonIOTDataList, _ := json.Marshal(iotDataList)
	rw.Write(jsonIOTDataList)
}
