package realtimeapi

import (
	"context"

	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/internal/ethapi"
	"github.com/ethereum/go-ethereum/rpc"
)

// BlockNumber implements realtime_blockNumber.
// Returns the block number of the most recent confirmed/pending block.
func (api *RealtimeAPIImpl) BlockNumber(ctx context.Context, tag *RealtimeTag) (hexutil.Uint64, error) {
	if api.cacheDB == nil || !api.cacheDB.ReadyFlag.Load() {
		backend := ethapi.NewBlockChainAPI(api.b)
		return backend.BlockNumber(), nil
	}

	if tag == nil {
		backend := ethapi.NewBlockChainAPI(api.b)
		return backend.BlockNumber(), nil
	}

	blockNumber, _, err := api.getBlockNumber(rpc.BlockNumber(*tag))
	if err != nil {
		// Do not redirect to default eth api as block number with tag is custom for realtime
		backend := ethapi.NewBlockChainAPI(api.b)
		return backend.BlockNumber(), nil
	}
	return hexutil.Uint64(blockNumber), nil
}

// GetBlockTransactionCountByNumber implements realtime_getBlockTransactionCountByNumber.
// Returns the number of transactions in a block given the block's block number.
func (api *RealtimeAPIImpl) GetBlockTransactionCountByNumber(ctx context.Context, blockNr rpc.BlockNumber) (*hexutil.Uint, error) {
	if api.cacheDB == nil || !api.cacheDB.ReadyFlag.Load() {
		backend := ethapi.NewTransactionAPI(api.b, nil)
		return backend.GetBlockTransactionCountByNumber(ctx, blockNr)
	}

	blockNum, _, err := api.getBlockNumber(blockNr)
	if err != nil {
		backend := ethapi.NewTransactionAPI(api.b, nil)
		return backend.GetBlockTransactionCountByNumber(ctx, blockNr)
	}

	_, _, _, ok := api.cacheDB.Stateless.GetHeader(blockNum)
	if !ok {
		backend := ethapi.NewTransactionAPI(api.b, nil)
		return backend.GetBlockTransactionCountByNumber(ctx, blockNr)
	}

	txs, ok := api.cacheDB.Stateless.GetBlockTxs(blockNum)
	if !ok {
		backend := ethapi.NewTransactionAPI(api.b, nil)
		return backend.GetBlockTransactionCountByNumber(ctx, blockNr)
	}
	numOfTx := hexutil.Uint(len(txs))
	return &numOfTx, nil
}
