package realtimeapi

import (
	"context"
	"fmt"

	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rpc"
)

func (api *RealtimeAPIImpl) DebugDumpCache(ctx context.Context) error {
	if !api.enableFlag || api.cacheDB == nil {
		return ErrRealtimeNotEnabled
	}

	if err := api.cacheDB.DebugDumpToFile(); err != nil {
		log.Error("[Realtime] Failed to dump state cache", "error", err)
		return fmt.Errorf("failed to dump state cache: %v", err)
	}

	return nil
}

func (api *RealtimeAPIImpl) DebugCompareStateCache(ctx context.Context) ([]string, error) {
	if !api.enableFlag || api.cacheDB == nil || api.cacheDB.State == nil {
		return nil, ErrRealtimeNotEnabled
	}

	statedb, _, err := api.b.StateAndHeaderByNumber(ctx, rpc.LatestBlockNumber)
	if err != nil {
		return nil, err
	}

	mismatches := api.cacheDB.State.DebugCompare(statedb)
	return mismatches, nil
}
