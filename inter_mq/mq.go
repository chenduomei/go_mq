package inter_mq

import (
	"errors"
	"sync"
	"time"
)

type Broker interface {
	publish(topic string, msg interface{}) error               // 进行消息的推送，有两个参数即topic、msg，分别是订阅的主题、要传递的消息
	subscribe(topic string) (<-chan interface{}, error)        //消息的订阅，传入订阅的主题，即可完成订阅，并返回对应的channel通道用来接收数据
	unsubscribe(topic string, sub <-chan interface{}) error    //取消订阅，传入订阅的主题和对应的通道
	close()                                                    //关闭消息队列
	broadcast(msg interface{}, subscribers []chan interface{}) //对推送的消息进行广播，保证每一个订阅者都可以收到
	setConditions(capacity int)                                //控制消息队列的大小
}



type BrokerImpl struct {
	exitCh     chan bool //关闭消息队列
	capacity int       //设置消息队列的容量
	topics       map[string][]chan interface{} // 一个topic可以有多个订阅者,一个订阅者对应着一个通道
	sync.RWMutex                               //读写锁,防止并发情况下，数据的推送出现错误
}

// BrokerImpl interface
func (b *BrokerImpl) publish(topic string, pub interface{}) error {
	select {
	case <-b.exitCh:
		return errors.New("broker closed")
	default:
	}
	b.RLock()
	subscribers, ok := b.topics[topic]
	b.RUnlock()
	if !ok {
		return nil
	}

	b.broadcast(pub, subscribers)
	return nil
}

func (b *BrokerImpl) broadcast(msg interface{}, subscribers []chan interface{}) {
	count := len(subscribers)
	concurrency := 1

	switch {
	case count > 500:
		concurrency = 3
	case count > 50:
		concurrency = 2
	default:
		concurrency = 1
	}
	pub := func(start int) {
		idleDuration := 5 * time.Millisecond
		idleTimeout := time.NewTimer(idleDuration)
		defer idleTimeout.Stop()
		for j := start; j < count; j += concurrency {
			if !idleTimeout.Stop(){
				select {
				case <- idleTimeout.C:
				default:
				}
			}
			idleTimeout.Reset(idleDuration)
			select {
			case subscribers[j] <- msg:
			case <-idleTimeout.C:
			case <-b.exitCh:
				return
			}
		}
	}
	for i := 0; i < concurrency; i++ {
		go pub(i)
	}
}

func (b *BrokerImpl) subscribe(topic string) (<-chan interface{}, error) {
	select {
	case <-b.exitCh:
		return nil, errors.New("broker closed")
	default:
	}

	ch := make(chan interface{}, b.capacity)
	b.Lock()
	b.topics[topic] = append(b.topics[topic], ch)
	b.Unlock()
	return ch, nil
}
func (b *BrokerImpl) unsubscribe(topic string, sub <-chan interface{}) error {
	select {
	case <-b.exitCh:
		return errors.New("broker closed")
	default:
	}

	b.RLock()
	subscribers, ok := b.topics[topic]
	b.RUnlock()

	if !ok {
		return nil
	}
	// delete subscriber
	var newSubs []chan interface{}
	for _, subscriber := range subscribers {
		if subscriber == sub {
			continue
		}
		newSubs = append(newSubs, subscriber)
	}

	b.Lock()
	b.topics[topic] = newSubs
	b.Unlock()
	return nil
}

func (b *BrokerImpl) close() {
	select {
	case <-b.exitCh:
		return
	default:
		close(b.exitCh)
		b.Lock()
		b.topics = make(map[string][]chan interface{})
		b.Unlock()
	}
	return
}

func (b *BrokerImpl) setConditions(capacity int) {
	b.capacity = capacity
}
