package memstore

import (
	"fmt"
	"github.com/eclipse/paho.mqtt.golang/packets"
	"testing"
	"time"
)

func BenchmarkMemStore(t *testing.B) {
	memStore := NewLinkMemStore(10)
	memStore.Open()

	index := 1

	for i := 0; i < 65535; i++ {
		packet := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
		packet.MessageID = uint16(index)
		index++
		memStore.Put(inboundKeyFromMID(packet.MessageID), packet)
		if i%1000 == 0 {
			time.Sleep(time.Second)
		}
	}
	t.Log(t.N)

}

func inboundKeyFromMID(id uint16) string {
	return fmt.Sprintf("%s%d", "i.", id)
}
