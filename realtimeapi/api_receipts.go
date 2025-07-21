package realtimeapi

import (
	"context"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/internal/ethapi"
)

// GetTransactionReceipt implements realtime_getTransactionReceipt.
// Returns the receipt of a transaction given the transaction's hash.
func (api *RealtimeAPIImpl) GetTransactionReceipt(ctx context.Context, hash common.Hash) (map[string]interface{}, error) {
	if !api.enableFlag || api.cacheDB == nil {
		return nil, ErrRealtimeNotEnabled
	}

	txn, receipt, _, _, ok := api.cacheDB.Stateless.GetTxInfo(hash)
	if !ok {
		backend := ethapi.NewTransactionAPI(api.b, nil)
		return backend.GetTransactionReceipt(ctx, hash)
	}
	header, _, ok := api.cacheDB.Stateless.GetHeader(receipt.BlockNumber.Uint64())
	if !ok {
		backend := ethapi.NewTransactionAPI(api.b, nil)
		return backend.GetTransactionReceipt(ctx, hash)
	}

	signer := types.MakeSigner(api.b.ChainConfig(), header.Number, header.Time)
	return ethapi.MarshalReceiptRealtime(receipt, header.Number.Uint64(), signer, txn, api.b.ChainConfig()), nil
}

// GetInternalTransactions implements realtime_getInternalTransactions.
// Returns the internal transactions of a transaction given the transaction's hash.
func (api *RealtimeAPIImpl) GetInternalTransactions(ctx context.Context, hash common.Hash) ([]*types.InnerTx, error) {
	if !api.enableFlag || api.cacheDB == nil {
		return nil, ErrRealtimeNotEnabled
	}

	_, _, _, innerTxs, ok := api.cacheDB.Stateless.GetTxInfo(hash)
	if !ok {
		backend := ethapi.NewTransactionAPI(api.b, nil)
		return backend.GetInternalTransactions(ctx, hash)
	}

	return innerTxs, nil
}
