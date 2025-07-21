package realtimeapi

import (
	"context"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/eth/filters"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rpc"
	realtimeSub "github.com/sieniven/xlayer-realtime/subscription"
)

type RealtimeResult struct {
	Header   *types.Header      `json:"Header,omitempty"`
	TxHash   string             `json:"TxHash,omitempty"`
	TxData   *types.Transaction `json:"TxData,omitempty"`
	Receipt  *types.Receipt     `json:"Receipt,omitempty"`
	InnerTxs []*types.InnerTx   `json:"InnerTxs,omitempty"`
}

// Realtime send a notification each time when a transaction was received in real-time.
func (api *RealtimeAPIImpl) Realtime(ctx context.Context, criteria realtimeSub.StreamCriteria) (*rpc.Subscription, error) {
	if !api.enableFlag || api.cacheDB == nil {
		return &rpc.Subscription{}, ErrRealtimeNotEnabled
	}

	if api.subService == nil {
		return &rpc.Subscription{}, rpc.ErrNotificationsUnsupported
	}

	notifier, supported := rpc.NotifierFromContext(ctx)
	if !supported {
		return &rpc.Subscription{}, rpc.ErrNotificationsUnsupported
	}

	rpcSub := notifier.CreateSubscription()
	msgChan, id, err := api.subService.SubscribeRealtime()
	if err != nil {
		return &rpc.Subscription{}, err
	}

	go func() {
		defer api.subService.UnsubscribeRealtime(id)

		for {
			select {
			case msg, ok := <-msgChan:
				if !ok {
					log.Warn("[realtime subscription] realtime txMsg channel closed")
					return
				}

				if criteria.NewHeads && msg.BlockMsg != nil {
					header, _, err := msg.BlockMsg.GetBlockInfo()
					if err != nil {
						log.Warn("[realtime subscription] error getting block info", "err", err)
					}

					result := RealtimeResult{Header: header}
					err = notifier.Notify(rpcSub.ID, result)
					if err != nil {
						log.Warn("[realtime subscription] error while notifying subscription", "err", err)
					}
				}

				if msg.TxMsg != nil {
					result := RealtimeResult{}
					_, tx, receipt, innerTxs, err := msg.TxMsg.GetAllTxData()
					if err != nil {
						log.Warn("[realtime subscription] error getting tx data", "err", err)
					}
					result.TxHash = tx.Hash().Hex()

					// Add tx data according to stream criteria
					if criteria.TransactionExtraInfo {
						result.TxData = tx
					}
					if criteria.TransactionReceipt {
						result.Receipt = receipt
					}
					if criteria.TransactionInnerTxs {
						result.InnerTxs = innerTxs
					}

					err = notifier.Notify(rpcSub.ID, result)
					if err != nil {
						log.Warn("[realtime subscription] error while notifying subscription", "err", err)
					}
				}
			case <-rpcSub.Err():
				return
			}
		}
	}()

	return rpcSub, nil
}

// Logs send a notification each time a new log appears in real-time.
func (api *RealtimeAPIImpl) Logs(ctx context.Context, crit filters.FilterCriteria) (*rpc.Subscription, error) {
	if !api.enableFlag || api.cacheDB == nil {
		return &rpc.Subscription{}, ErrRealtimeNotEnabled
	}

	if api.subService == nil {
		return &rpc.Subscription{}, rpc.ErrNotificationsUnsupported
	}

	notifier, supported := rpc.NotifierFromContext(ctx)
	if !supported {
		return &rpc.Subscription{}, rpc.ErrNotificationsUnsupported
	}

	rpcSub := notifier.CreateSubscription()
	logCh, id, err := api.subService.SubscribeRealtimeLogs(crit)
	if err != nil {
		return &rpc.Subscription{}, err
	}

	go func() {
		defer api.subService.UnsubscribeRealtimeLogs(id)

		for {
			select {
			case h, ok := <-logCh:
				if h != nil {
					if h.Topics == nil {
						h.Topics = make([]common.Hash, 0)
					}
					err := notifier.Notify(rpcSub.ID, h)
					if err != nil {
						log.Warn("[realtime rpc] error while notifying subscription", "err", err)
					}
				}
				if !ok {
					log.Warn("[realtime rpc] realtime log channel was closed")
					return
				}
			case <-rpcSub.Err():
				return
			}
		}
	}()

	return rpcSub, nil
}
