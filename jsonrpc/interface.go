package jsonrpc

import (
	"context"

	libcommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/eth/filters"
	"github.com/ethereum/go-ethereum/rpc"
	realtimeTypes "github.com/sieniven/xlayer-realtime/types"
)

// Local type definitions to avoid importing internal packages
type TransactionArgs interface{}
type StateOverride interface{}

type RealtimeAPI interface {
	// Block related (see ./realtime_blocks_xlayer.go)
	BlockNumber(ctx context.Context) (hexutil.Uint64, error)
	GetBlockTransactionCountByNumber(ctx context.Context, blockNr rpc.BlockNumber) (*hexutil.Uint, error)

	// Transaction related (see ./realtime_txs_xlayer.go)
	GetTransactionByHash(ctx context.Context, hash libcommon.Hash, includeExtraInfo *bool) (interface{}, error)
	GetRawTransactionByHash(ctx context.Context, hash libcommon.Hash) (hexutil.Bytes, error)

	// Receipt related (see ./realtime_receipts_xlayer.go)
	GetTransactionReceipt(ctx context.Context, hash libcommon.Hash) (map[string]interface{}, error)
	GetInternalTransactions(ctx context.Context, hash libcommon.Hash) ([]*realtimeTypes.InnerTx, error)

	// Account related (see ./realtime_accounts_xlayer.go)
	GetBalance(ctx context.Context, address libcommon.Address) (*hexutil.Big, error)
	GetTransactionCount(ctx context.Context, address libcommon.Address) (*hexutil.Uint64, error)
	GetCode(ctx context.Context, address libcommon.Address) (hexutil.Bytes, error)
	GetStorageAt(ctx context.Context, address libcommon.Address, index string) (string, error)

	// Sending related (see ./realtime_call_xlayer.go)
	Call(ctx context.Context, args TransactionArgs, overrides *StateOverride) (hexutil.Bytes, error)

	// Debug related (see ./realtime_debug.go)
	DebugDumpCache(ctx context.Context) error
	DebugCompareStateCache(ctx context.Context) ([]string, error)
}

type RealtimeSubscriptionAPI interface {
	// Ws subscription related (see ./realtime_filters_xlayer.go)
	RealtimeTransactions(ctx context.Context, criteria StreamCriteria) (*rpc.Subscription, error)
	Logs(ctx context.Context, crit filters.FilterCriteria) (*rpc.Subscription, error)
}
