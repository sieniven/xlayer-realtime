package realtimeapi

import (
	"context"
	"fmt"
	"math/big"

	libcommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/internal/ethapi"
	"github.com/ethereum/go-ethereum/rpc"
)

func (api *RealtimeAPIImpl) GetBalance(ctx context.Context, address libcommon.Address) (*hexutil.Big, error) {
	if !api.enableFlag || api.cacheDB == nil {
		return nil, ErrRealtimeNotEnabled
	}

	acc, err := api.cacheDB.State.Account(address)
	if err != nil {
		return nil, fmt.Errorf("cant get a balance for account %x: %w", address.String(), err)
	}
	if acc == nil {
		// Special case - non-existent account is assumed to have zero balance
		return (*hexutil.Big)(big.NewInt(0)), nil
	}

	return (*hexutil.Big)(acc.Balance.ToBig()), nil
}

func (api *RealtimeAPIImpl) GetTransactionCount(ctx context.Context, address libcommon.Address) (*hexutil.Uint64, error) {
	if !api.enableFlag || api.cacheDB == nil {
		return nil, ErrRealtimeNotEnabled
	}

	backend := ethapi.NewTransactionAPI(api.b, nil)
	ethNonce, err := backend.GetTransactionCount(ctx, address, rpc.BlockNumberOrHashWithNumber(rpc.LatestBlockNumber))
	if err != nil {
		ethNonce = nil
	}

	var cacheNonce *hexutil.Uint64
	acc, err := api.cacheDB.State.Account(address)
	if err != nil {
		cacheNonce = nil
	} else if acc != nil {
		nonce := hexutil.Uint64(acc.Nonce)
		cacheNonce = &nonce
	}

	if ethNonce == nil && cacheNonce == nil {
		return nil, fmt.Errorf("failed to get transaction count for account %x from both sources", address)
	}

	if ethNonce == nil {
		return cacheNonce, nil
	}
	if cacheNonce == nil {
		return ethNonce, nil
	}

	if *ethNonce > *cacheNonce {
		return ethNonce, nil
	}
	return cacheNonce, nil
}

func (api *RealtimeAPIImpl) GetCode(ctx context.Context, address libcommon.Address) (hexutil.Bytes, error) {
	if !api.enableFlag || api.cacheDB == nil {
		return nil, ErrRealtimeNotEnabled
	}

	acc, err := api.cacheDB.State.Account(address)
	if acc == nil || err != nil {
		return hexutil.Bytes(""), nil
	}
	res, _ := api.cacheDB.State.Code(address, libcommon.BytesToHash(acc.CodeHash))
	if res == nil {
		return hexutil.Bytes(""), nil
	}
	return res, nil
}

func (api *RealtimeAPIImpl) GetStorageAt(ctx context.Context, address libcommon.Address, index string) (string, error) {
	if !api.enableFlag || api.cacheDB == nil {
		return "", ErrRealtimeNotEnabled
	}

	var empty []byte

	acc, err := api.cacheDB.State.Account(address)
	if acc == nil || err != nil {
		return hexutil.Encode(libcommon.LeftPadBytes(empty, 32)), err
	}

	res, err := api.cacheDB.State.Storage(address, libcommon.HexToHash(index))
	if err != nil {
		res = libcommon.BytesToHash(empty)
	}
	return hexutil.Encode(libcommon.LeftPadBytes(res[:], 32)), err
}
