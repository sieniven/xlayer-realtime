package realtimeapi

import (
	"context"
	"fmt"

	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/internal/ethapi"
	"github.com/ethereum/go-ethereum/internal/ethapi/override"
	"github.com/ethereum/go-ethereum/rpc"
)

// Call implements realtime_call.
// Executes a new message call immediately without creating a transaction on the block chain.
// Note that realtime API only supports execution on the latest block.
func (api *RealtimeAPIImpl) Call(ctx context.Context, args ethapi.TransactionArgs, overrides *override.StateOverride) (hexutil.Bytes, error) {
	if !api.enableFlag || api.cacheDB == nil {
		return nil, ErrRealtimeNotEnabled
	}

	blockNumber, _, err := api.getBlockNumber(rpc.PendingBlockNumber)
	if err != nil {
		return nil, err
	}
	header, _, ok := api.cacheDB.Stateless.GetHeader(blockNumber)
	if !ok {
		return nil, fmt.Errorf("header not found for block number %d", blockNumber)
	}

	statedb, err := api.GetStateDB(ctx)
	if err != nil {
		return nil, err
	}

	return ethapi.CallRealtime(ctx, api.b, args, overrides, statedb, header, api.b.RPCEVMTimeout(), api.b.RPCGasCap())
}
