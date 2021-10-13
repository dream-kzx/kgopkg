package memstore

import (
	"context"
	"log"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/eclipse/paho.mqtt.golang/packets"
)

var _ mqtt.Store = (*MemStore)(nil)

// NewMemStore timeout表示每个packet的超时时间，单位为秒
func NewMemStore(timeout int) *MemStore {
	return &MemStore{
		messages: make(map[string]messageNode),
		timeList: make([]timeNode, 0),
		timeout:  time.Duration(timeout) * time.Second,
	}
}

type MemStore struct {
	sync.RWMutex
	messages map[string]messageNode
	timeList []timeNode
	timeout  time.Duration
	opened   bool
	done     context.CancelFunc
}

type messageNode struct {
	packet packets.ControlPacket
	idx    int
}

type timeNode struct {
	T   time.Time
	Mid string
}

func (m *MemStore) Open() {
	m.Lock()
	defer m.Unlock()
	if !m.opened {
		var ctx context.Context
		ctx, m.done = context.WithCancel(context.Background())
		go func() {
			for {
				time.Sleep(m.timeout / 2)
				select {
				case <-ctx.Done():
					break
				default:
					m.lazyTimeoutDel()
				}
			}
		}()
	}
	m.opened = true

}

func (m *MemStore) Put(key string, message packets.ControlPacket) {
	m.Lock()
	m.Unlock()
	if !m.opened {
		log.Println("Trying to use memory store, but not open")
		return
	}

	if msg, ok := m.messages[key]; ok {
		m.timeList = append(m.timeList[0:msg.idx], m.timeList[msg.idx+1:]...)
	}

	tNode := timeNode{
		T:   time.Now(),
		Mid: key,
	}
	m.timeList = append(m.timeList, tNode)

	msg := messageNode{
		packet: message,
		idx:    len(m.timeList) - 1,
	}
	m.messages[key] = msg
}

func (m *MemStore) Get(key string) packets.ControlPacket {
	m.RLock()
	defer m.RUnlock()
	if !m.opened {
		log.Println("Trying to use memory store, but not open")
		return nil
	}

	msg, ok := m.messages[key]
	if ok {
		return msg.packet
	} else {
		return nil
	}
}

func (m *MemStore) All() []string {
	m.RLock()
	defer m.RUnlock()
	if !m.opened {
		log.Println("Trying to use memory store, but not open")
		return nil
	}

	keys := make([]string, 0, len(m.timeList))
	for _, n := range m.timeList {
		keys = append(keys, n.Mid)
	}
	return keys
}

func (m *MemStore) Del(key string) {
	m.Lock()
	defer m.Unlock()
	if !m.opened {
		log.Println("Trying to use memory store, but not open")
		return
	}

	msg, ok := m.messages[key]
	if !ok {
		return
	}

	idx := msg.idx
	delete(m.messages, key)
	m.timeList = append(m.timeList[0:idx], m.timeList[idx+1:]...)
}

func (m *MemStore) Close() {
	m.Lock()
	defer m.Unlock()
	if !m.opened {

		return
	}
	m.opened = false
}

func (m *MemStore) Reset() {
	m.Lock()
	defer m.Unlock()
	if !m.opened {
		log.Println("Trying to reset memory store, but not open")
	}
	m.messages = make(map[string]messageNode)
	m.timeList = make([]timeNode, 0)
}

func (m *MemStore) lazyTimeoutDel() {
	m.Lock()
	defer m.Unlock()
	index := 0
	for _, n := range m.timeList {
		if time.Now().Sub(n.T) < m.timeout {
			break
		}

		delete(m.messages, n.Mid)
		index++
	}
	m.timeList = m.timeList[index:]
}
