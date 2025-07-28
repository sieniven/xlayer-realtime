package realtimeapi

import (
	"context"
	"fmt"

	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rpc"
)

func (api *RealtimeAPIImpl) DebugDumpCache(ctx context.Context) error {
	if api.cacheDB == nil || !api.cacheDB.ReadyFlag.Load() {
		// Custom for realtime
		return ErrRealtimeNotEnabled
	}

	if err := api.cacheDB.DebugDumpToFile(); err != nil {
		log.Error("[Realtime] Failed to dump state cache", "error", err)
		return fmt.Errorf("failed to dump state cache: %v", err)
	}

	return nil
}

func (api *RealtimeAPIImpl) DebugCompareStateCache(ctx context.Context) (*RealtimeDebugResult, error) {
	if api.cacheDB == nil || !api.cacheDB.ReadyFlag.Load() {
		// Custom for realtime
		return nil, ErrRealtimeNotEnabled
	}

	statedb, _, err := api.b.StateAndHeaderByNumber(ctx, rpc.LatestBlockNumber)
	if err != nil {
		return nil, err
	}

	return &RealtimeDebugResult{
		ConfirmHeight:   api.cacheDB.GetHighestConfirmHeight(),
		ExecutionHeight: api.cacheDB.GetExecutionHeight(),
		Mismatches:      api.cacheDB.State.DebugCompare(statedb),
	}, nil
}
