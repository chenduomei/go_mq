package inter_mq

import (
	"fmt"
	"sync"
	"testing"
)

func TestInter(t *testing.T) {
	client := NewClient()
	client.SetConditions(10)
	var wg sync.WaitGroup

	for i := 0; i < 10; i++ {
		topic := fmt.Sprintf("test--> %d", i)
		payload := fmt.Sprintf("TEST--> %d", i)
		ch, err := client.Subscribe(topic)
		if err != nil {
			t.Fatal(err)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			gpl := client.GetPayLoad(ch)
			if gpl != payload {
				t.Fatalf("%s expected %s but get %s", topic, payload, gpl)
			}
			if err := client.Unsubscribe(topic, ch); err != nil {
				t.Fatal(err)
			}
		}()

		if err := client.Publish(topic, payload); err != nil {
			t.Fatal(err)
		}
	}

	wg.Wait()
}

