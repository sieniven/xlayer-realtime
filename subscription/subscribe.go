package subscription

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/eth/filters"
	"github.com/ethereum/go-ethereum/log"
	kafkaTypes "github.com/ethereum/go-ethereum/realtime/kafka/types"
	realtimeTypes "github.com/ethereum/go-ethereum/realtime/types"
)

const (
	DefaultChannelSize          = 1000
	DefaultSubscribeChannelSize = 256

	// Limit the number of subscriptions on the node
	MaxSubscriptionsCount = 100
)

type RealtimeSubMessage struct {
	BlockMsg *realtimeTypes.BlockInfo
	TxMsg    *kafkaTypes.TransactionMessage
}

type RealtimeSubscription struct {
	currHeight atomic.Uint64
	rtSubs     *SyncMap[SubID, Sub[RealtimeSubMessage]]
	logsSubs   *SyncMap[SubID, *LogsFilter]
	newMsgChan chan RealtimeSubMessage
}

func NewRealtimeSubscription() *RealtimeSubscription {
	return &RealtimeSubscription{
		currHeight: atomic.Uint64{},
		rtSubs:     NewSyncMap[SubID, Sub[RealtimeSubMessage]](),
		logsSubs:   NewSyncMap[SubID, *LogsFilter](),
		newMsgChan: make(chan RealtimeSubMessage, DefaultChannelSize),
	}
}

func (ff *RealtimeSubscription) Start(ctx context.Context) {
	var wg sync.WaitGroup
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case msg := <-ff.newMsgChan:
				wg.Add(1)
				go func() {
					defer wg.Done()
					ff.handleRealtimeMsgs(ctx, msg)
				}()
				if msg.TxMsg != nil {
					wg.Add(1)
					go func() {
						defer wg.Done()
						ff.handleRealtimeLogMsgs(ctx, msg.TxMsg)
					}()
				}
				wg.Wait()
			}
		}
	}()
}

func (ff *RealtimeSubscription) handleRealtimeMsgs(ctx context.Context, msg RealtimeSubMessage) {
	msgHeight := uint64(0)
	if msg.BlockMsg != nil {
		msgHeight = msg.BlockMsg.Header.Number.Uint64()
	} else if msg.TxMsg != nil {
		msgHeight = msg.TxMsg.BlockNumber
	}

	if msgHeight < ff.currHeight.Load() {
		// Ignore msg from previous blocks
		log.Debug(fmt.Sprintf("[Realtime] Subscription ignoring msg from previous block. msgHeight: %d, currHeight: %d", msgHeight, ff.currHeight.Load()))
		return
	}
	if msgHeight > ff.currHeight.Load() {
		ff.currHeight.Store(msgHeight)
	}

	ff.rtSubs.Range(func(k SubID, v Sub[RealtimeSubMessage]) error {
		select {
		case <-ctx.Done():
			return nil
		default:
		}
		v.Send(msg)
		return nil
	})
}

func (ff *RealtimeSubscription) handleRealtimeLogMsgs(ctx context.Context, txMsg *kafkaTypes.TransactionMessage) {
	logs := txMsg.Receipt.Logs
	for _, log := range logs {
		ff.logsSubs.Range(func(k SubID, filter *LogsFilter) error {
			select {
			case <-ctx.Done():
				return nil
			default:
			}

			if filter.allAddrs == 0 {
				_, addrOk := filter.addrs[log.Address]
				if !addrOk {
					return nil
				}
			}
			if filter.allTopics == 0 {
				if !chooseTopics(filter, log.Topics) {
					return nil
				}
			}
			filter.sender.Send(log)
			return nil
		})
	}
}

func (ff *RealtimeSubscription) BroadcastNewMsg(blockMsg *realtimeTypes.BlockInfo, txMsg *kafkaTypes.TransactionMessage) {
	msg := RealtimeSubMessage{
		BlockMsg: blockMsg,
		TxMsg:    txMsg,
	}
	ff.newMsgChan <- msg
}

func (ff *RealtimeSubscription) SubscribeRealtime() (<-chan RealtimeSubMessage, SubID, error) {
	if ff.rtSubs.Len()+ff.logsSubs.Len() > MaxSubscriptionsCount {
		return nil, "", fmt.Errorf("max subscriptions count reached")
	}

	id := SubID(generateSubID())
	sub := newChanSub[RealtimeSubMessage](DefaultSubscribeChannelSize)
	ff.rtSubs.Put(id, sub)
	return sub.ch, id, nil
}

func (ff *RealtimeSubscription) UnsubscribeRealtime(id SubID) bool {
	ch, ok := ff.rtSubs.Get(id)
	if !ok {
		return false
	}
	ch.Close()
	if _, ok = ff.rtSubs.Delete(id); !ok {
		return false
	}
	return true
}

func (ff *RealtimeSubscription) SubscribeRealtimeLogs(crit filters.FilterCriteria) (<-chan *types.Log, SubID, error) {
	if ff.rtSubs.Len()+ff.logsSubs.Len() > MaxSubscriptionsCount {
		return nil, "", fmt.Errorf("max subscriptions count reached")
	}

	id := SubID(generateSubID())
	sub := newChanSub[*types.Log](DefaultSubscribeChannelSize)
	filter := &LogsFilter{addrs: map[common.Address]int{}, topics: map[common.Hash]int{}, sender: sub}
	filter.addrs = map[common.Address]int{}
	if len(crit.Addresses) == 0 {
		filter.allAddrs = 1
	} else {
		for _, addr := range crit.Addresses {
			filter.addrs[addr] = 1
		}
	}
	filter.topics = map[common.Hash]int{}
	if len(crit.Topics) == 0 {
		filter.allTopics = 1
	} else {
		for _, topics := range crit.Topics {
			for _, topic := range topics {
				filter.topics[topic] = 1
			}
		}
	}
	filter.topicsOriginal = crit.Topics
	ff.logsSubs.Put(id, filter)

	return sub.ch, id, nil
}

func (ff *RealtimeSubscription) UnsubscribeRealtimeLogs(id SubID) bool {
	filter, ok := ff.logsSubs.Get(id)
	if !ok {
		return false
	}
	filter.Close()
	if _, ok = ff.logsSubs.Delete(id); !ok {
		return false
	}
	return true
}
