package memstore

import (
	"context"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/eclipse/paho.mqtt.golang/packets"
	"log"
	"sync"
	"time"
)

var _ mqtt.Store = (*LinkMemStore)(nil)

// NewLinkMemStore timeout表示每个packet的超时时间，单位为秒
func NewLinkMemStore(timeout int) *LinkMemStore {
	linkHead := &msgLinkNode{}
	linkEnd := &msgLinkNode{}
	linkHead.Next = linkEnd
	linkEnd.Pre = linkHead

	return &LinkMemStore{
		messages: make(map[string]*msgLinkNode),
		timeout:  time.Duration(timeout) * time.Second,
		linkHead: linkHead,
		linkEnd:  linkEnd,
	}
}

type LinkMemStore struct {
	sync.RWMutex
	messages map[string]*msgLinkNode
	linkHead *msgLinkNode
	linkEnd  *msgLinkNode
	timeout  time.Duration
	opened   bool
	done     context.CancelFunc
}

type msgLinkNode struct {
	Pre    *msgLinkNode
	Next   *msgLinkNode
	Packet packets.ControlPacket
	T      time.Time
	Mid    string
}

func (m *LinkMemStore) Open() {
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

func (m *LinkMemStore) Put(key string, message packets.ControlPacket) {
	m.Lock()
	m.Unlock()
	if !m.opened {
		log.Println("Trying to use memory store, but not open")
		return
	}

	var tmp *msgLinkNode
	msg, ok := m.messages[key]
	if ok {
		tmp = msg
		tmp.Pre.Next = tmp.Next
		tmp.Next.Pre = tmp.Pre
	}

	if tmp == nil {
		tmp = &msgLinkNode{}
	}
	tmp.T = time.Now()
	tmp.Mid = key
	tmp.Packet = message

	m.linkEnd.Pre.Next = tmp
	tmp.Pre = m.linkEnd.Pre
	tmp.Next = m.linkEnd
	m.linkEnd.Pre = tmp

	m.messages[key] = tmp
}

func (m *LinkMemStore) Get(key string) packets.ControlPacket {
	m.RLock()
	defer m.RUnlock()
	if !m.opened {
		log.Println("Trying to use memory store, but not open")
		return nil
	}

	msg, ok := m.messages[key]
	if ok {
		return msg.Packet
	} else {
		return nil
	}
}

func (m *LinkMemStore) All() []string {
	m.RLock()
	defer m.RUnlock()
	if !m.opened {
		log.Println("Trying to use memory store, but not open")
		return nil
	}

	keys := make([]string, 0, len(m.messages))
	for key, _ := range m.messages {
		keys = append(keys, key)
	}
	return keys
}

func (m *LinkMemStore) Del(key string) {
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

	delete(m.messages, key)
	msg.Pre.Next = msg.Next
	msg.Next.Pre = msg.Pre
	msg.Pre, msg.Next = nil, nil
}

func (m *LinkMemStore) Close() {
	m.Lock()
	defer m.Unlock()
	if !m.opened {
		return
	}
	m.done()
	m.opened = false
}

func (m *LinkMemStore) Reset() {
	m.Lock()
	defer m.Unlock()
	if !m.opened {
		log.Println("Trying to reset memory store, but not open")
	}
	m.messages = make(map[string]*msgLinkNode)
	m.linkHead.Next = m.linkEnd
	m.linkEnd.Pre = m.linkHead
}

func (m *LinkMemStore) lazyTimeoutDel() {
	m.Lock()
	defer m.Unlock()

	now := m.linkHead.Next
	for now != m.linkEnd {
		if time.Now().Sub(now.T) < m.timeout {
			break
		}

		tmp := now.Next
		now.Pre.Next = now.Next
		now.Next.Pre = now.Pre
		now.Pre, now.Next = nil, nil
		delete(m.messages, now.Mid)
		now = tmp
	}
}
