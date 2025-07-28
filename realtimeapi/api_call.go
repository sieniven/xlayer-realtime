package realtimeapi

import (
	"context"
	"errors"
	"fmt"

	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/internal/ethapi"
	"github.com/ethereum/go-ethereum/internal/ethapi/override"
	"github.com/ethereum/go-ethereum/rpc"
)

// Call implements realtime_call.
// Executes a new message call immediately without creating a transaction on the block chain.
// Note that realtime API only supports execution on the latest block.
func (api *RealtimeAPIImpl) Call(ctx context.Context, args ethapi.TransactionArgs, blockNrOrHash *rpc.BlockNumberOrHash, overrides *override.StateOverride) (hexutil.Bytes, error) {
	if api.cacheDB == nil || !api.cacheDB.ReadyFlag.Load() {
		backend := ethapi.NewBlockChainAPI(api.b)
		return backend.Call(ctx, args, blockNrOrHash, overrides, nil)
	}

	if blockNrOrHash.BlockNumber != nil && *blockNrOrHash.BlockNumber == rpc.PendingBlockNumber {
		// Realtime supported only for pending tags
		return api.doRealtimeCall(ctx, args, overrides)
	}

	backend := ethapi.NewBlockChainAPI(api.b)
	return backend.Call(ctx, args, blockNrOrHash, overrides, nil)
}

func (api *RealtimeAPIImpl) doRealtimeCall(ctx context.Context, args ethapi.TransactionArgs, overrides *override.StateOverride) (hexutil.Bytes, error) {
	blockNumber, _, err := api.getBlockNumber(rpc.PendingBlockNumber)
	if err != nil {
		return nil, fmt.Errorf("failed to get block number: %w", err)
	}
	header, _, _, ok := api.cacheDB.Stateless.GetHeader(blockNumber)
	if !ok {
		return nil, fmt.Errorf("header not found for block number %d", blockNumber)
	}

	statedb, err := api.GetStateDB(ctx)
	if err != nil {
		return nil, err
	}

	result, err := ethapi.DoCallRealtime(ctx, api.b, args, overrides, nil, api.b.RPCEVMTimeout(), api.b.RPCGasCap(), statedb, header)
	if err != nil {
		return nil, err
	}
	if errors.Is(result.Err, vm.ErrExecutionReverted) {
		return nil, ethapi.NewRevertError(result.Revert())
	}
	return result.Return(), result.Err
}
