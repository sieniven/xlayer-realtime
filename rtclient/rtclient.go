package rtclient

import (
	"context"
	"errors"
	"fmt"
	"math/big"

	ethereum "github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rpc"
)

type rpcTransaction struct {
	tx *types.Transaction
	txExtraInfo
}

type txExtraInfo struct {
	BlockNumber *string         `json:"blockNumber,omitempty"`
	BlockHash   *common.Hash    `json:"blockHash,omitempty"`
	From        *common.Address `json:"from,omitempty"`
}

type RealtimeClient struct {
	c *rpc.Client
}

// Dial connects a client to the given URL.
func Dial(rawurl string) (*RealtimeClient, error) {
	return DialContext(context.Background(), rawurl)
}

// DialContext connects a client to the given URL with context.
func DialContext(ctx context.Context, rawurl string) (*RealtimeClient, error) {
	c, err := rpc.DialContext(ctx, rawurl)
	if err != nil {
		return nil, err
	}
	return NewClient(c), nil
}

// NewClient creates a client that uses the given RPC client.
func NewClient(c *rpc.Client) *RealtimeClient {
	return &RealtimeClient{c}
}

// RealtimeBlockNumber returns the number of the most recent block in real-time
func (rc *RealtimeClient) RealtimeBlockNumber(ctx context.Context) (uint64, error) {
	var result hexutil.Uint64
	err := rc.c.CallContext(ctx, &result, "realtime_blockNumber")
	return uint64(result), err
}

// RealtimeGetBlockTransactionCountByNumber returns the number of transactions in a block by number in real-time
func (rc *RealtimeClient) RealtimeGetBlockTransactionCountByNumber(ctx context.Context, blockNumber uint64) (uint64, error) {
	var result hexutil.Uint64
	err := rc.c.CallContext(ctx, &result, "realtime_getBlockTransactionCountByNumber")
	return uint64(result), err
}

// RealtimeGetTransactionByHash returns the information about a transaction requested by transaction hash in real-time
func (rc *RealtimeClient) RealtimeGetTransactionByHash(ctx context.Context, txHash common.Hash, includeExtraInfo *bool) (*types.Transaction, error) {
	var json *rpcTransaction
	err := rc.c.CallContext(ctx, &json, "eth_getTransactionByHash", txHash)
	if err != nil {
		return nil, err
	} else if json == nil {
		return nil, ethereum.NotFound
	} else if _, r, _ := json.tx.RawSignatureValues(); r == nil {
		return nil, errors.New("server returned transaction without signature")
	}
	if json.From != nil && json.BlockHash != nil {
		setSenderFromServer(json.tx, *json.From, *json.BlockHash)
	}
	return json.tx, nil
}

// RealtimeGetTransactionByHash returns raw information about a transaction requested by transaction hash in real-time
func (rc *RealtimeClient) RealtimeGetRawTransactionByHash(ctx context.Context, txHash common.Hash) ([]byte, error) {
	var result hexutil.Bytes
	err := rc.c.CallContext(ctx, &result, "realtime_getRawTransactionByHash", txHash)
	return result, err
}

// RealtimeGetTransactionReceipt returns the receipt of a transaction by transaction hash in real-time
func (rc *RealtimeClient) RealtimeGetTransactionReceipt(ctx context.Context, txHash common.Hash) (*types.Receipt, error) {
	var r *types.Receipt
	err := rc.c.CallContext(ctx, &r, "realtime_getTransactionReceipt", txHash)
	if err == nil && r == nil {
		return nil, ethereum.NotFound
	}
	return r, err
}

// RealtimeGetInternalTransactions returns the internal transactions for a given transaction hash in real-time
func (rc *RealtimeClient) RealtimeGetInternalTransactions(ctx context.Context, txHash common.Hash) ([]types.InnerTx, error) {
	var innerTxs []types.InnerTx
	err := rc.c.CallContext(ctx, &innerTxs, "realtime_getInternalTransactions", txHash)
	if err != nil {
		return nil, err
	}
	return innerTxs, err
}

// RealtimeGetBalance returns the balance of an account in real-time
func (rc *RealtimeClient) RealtimeGetBalance(ctx context.Context, address common.Address) (*big.Int, error) {
	var result hexutil.Big
	err := rc.c.CallContext(ctx, &result, "realtime_getBalance", address)
	return (*big.Int)(&result), err
}

// RealtimeGetCode returns the code at a given address in real-time
func (rc *RealtimeClient) RealtimeGetCode(ctx context.Context, address common.Address) (string, error) {
	var result hexutil.Bytes
	err := rc.c.CallContext(ctx, &result, "realtime_getCode", address)
	return hexutil.Encode(result), err
}

// RealtimeGetTransactionCount returns the number of transactions sent from an address in real-time
func (rc *RealtimeClient) RealtimeGetTransactionCount(ctx context.Context, address common.Address) (uint64, error) {
	var result hexutil.Uint64
	err := rc.c.CallContext(ctx, &result, "realtime_getTransactionCount", address)
	return uint64(result), err
}

// RealtimeGetStorageAt returns the value from a storage position at a given address in real-time
func (rc *RealtimeClient) RealtimeGetStorageAt(ctx context.Context, address common.Address, position string) (string, error) {
	var result hexutil.Bytes
	err := rc.c.CallContext(ctx, &result, "realtime_getStorageAt", address, position)
	return hexutil.Encode(result), err
}

// RealtimeCall executes a new message call immediately without creating a transaction in real-time
func (rc *RealtimeClient) RealtimeCall(ctx context.Context, from, to common.Address, gas string, gasPrice string, value string, data string) (string, error) {
	txParams := map[string]any{
		"from":     from,
		"to":       to,
		"gas":      gas,
		"gasPrice": gasPrice,
		"value":    value,
		"data":     data,
	}

	var hex hexutil.Bytes
	err := rc.c.CallContext(ctx, &hex, "realtime_call", txParams)
	if err != nil {
		return "", err
	}
	return hexutil.Encode(hex), nil
}

func (rc *RealtimeClient) RealtimeGetTokenBalance(
	ctx context.Context,
	fromAddress common.Address,
	toAddress common.Address,
	erc20Addr common.Address,
) (*big.Int, error) {
	// Pack the balanceOf function call
	data, err := erc20ABI.Pack("balanceOf", toAddress)
	if err != nil {
		return nil, fmt.Errorf("failed to pack balanceOf call: %v", err)
	}

	// Make the realtime_call
	result, err := rc.RealtimeCall(ctx, fromAddress, erc20Addr, "0x100000", "0x1", "0x0", fmt.Sprintf("0x%x", data))
	if err != nil {
		return nil, fmt.Errorf("failed to call contract: %v", err)
	}

	// Parse the hex result
	if len(result) > 2 && (result[:2] == "0x" || result[:2] == "0X") {
		result = result[2:]
	}

	balance := new(big.Int)
	balance, ok := balance.SetString(result, 16)
	if !ok {
		return nil, fmt.Errorf("failed to convert hex to big.Int: %s", result)
	}

	return balance, nil
}

// RealtimeDumpStateCache dumps the state cache
func (rc *RealtimeClient) RealtimeDumpCache(ctx context.Context) error {
	var result interface{}
	err := rc.c.CallContext(ctx, &result, "realtime_debugDumpCache")
	if err != nil {
		return err
	}
	return nil
}

func (rc *RealtimeClient) EthGetTokenBalance(
	ctx context.Context,
	c ethclient.Client,
	addr common.Address,
	erc20Addr common.Address,
) (*big.Int, error) {
	// Pack the balanceOf function call
	data, err := erc20ABI.Pack("balanceOf", addr)
	if err != nil {
		return nil, fmt.Errorf("failed to pack balanceOf call: %v", err)
	}

	// Make the eth_call
	result, err := c.CallContract(ctx, ethereum.CallMsg{
		To:   &erc20Addr,
		Data: data,
	}, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to call contract: %v", err)
	}

	// Unpack the result
	var balance *big.Int
	err = erc20ABI.UnpackIntoInterface(&balance, "balanceOf", result)
	if err != nil {
		return nil, fmt.Errorf("failed to unpack result: %v", err)
	}

	return balance, nil
}
