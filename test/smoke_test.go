package test

import (
	"context"
	"fmt"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/log"
	"github.com/sieniven/xlayer-realtime/rtclient"
	"github.com/stretchr/testify/require"
)

const Gwei = 1000000000

func TestRealtimeRPC(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	// Preapre to deploy a ERC20 contract
	ctx := context.Background()
	ec, err := ethclient.Dial(DefaultL2NetworkURL)
	require.NoError(t, err)
	client, err := rtclient.NewRealtimeClient(ctx, ec, DefaultL2NetworkURL)
	require.NoError(t, err)
	blockNumber := setupRealtimeTestEnvironment(t, ctx, client)

	privateKey, err := crypto.HexToECDSA(strings.TrimPrefix(DefaultL2AdminPrivateKey, "0x"))
	require.NoError(t, err)
	fromAddress := common.HexToAddress(DefaultL2AdminAddress)
	log.Info(fmt.Sprintf("Sender: %s", fromAddress))

	// Default test address for tests that require an address
	testAddress := common.HexToAddress("0x1234567890123456789012345678901234567890")

	// Used to check whether the result returned by the interface call is correct
	time.Sleep(1 * time.Second)
	originNonce, err := client.RealtimeGetTransactionCount(ctx, fromAddress)
	require.NoError(t, err)
	originBalance, err := client.RealtimeGetBalance(ctx, testAddress)
	require.NoError(t, err)

	// Transfer native token
	txHash := transToken(t, context.Background(), client, big.NewInt(Gwei), testAddress.String())

	// Deploy the contract
	erc20Address := deployERC20Contract(t, ctx, privateKey, client)

	t.Run("RealtimeBlockNumber", func(t *testing.T) {
		blockNumber, err := client.RealtimeBlockNumber(ctx)
		require.NoError(t, err)
		log.Info(fmt.Sprintf("RealtimeBlockNumber result: %d", blockNumber))
	})

	t.Run("RealtimePendingBlockNumber", func(t *testing.T) {
		blockNumber, err := client.RealtimePendingBlockNumber(ctx)
		require.NoError(t, err)
		log.Info(fmt.Sprintf("RealtimePendingBlockNumber result: %d", blockNumber))
	})

	t.Run("RealtimeGetBlockTransactionCountByNumber", func(t *testing.T) {
		transactionCount, err := client.RealtimeGetBlockTransactionCountByNumber(ctx, blockNumber)
		require.NoError(t, err)
		log.Info(fmt.Sprintf("RealtimeGetBlockTransactionCountByNumber result: %d", transactionCount))
	})

	t.Run("RealtimeGetLatestBlockTransactionCount", func(t *testing.T) {
		transactionCount, err := client.RealtimeGetLatestBlockTransactionCount(ctx)
		require.NoError(t, err)
		log.Info(fmt.Sprintf("RealtimeGetLatestBlockTransactionCount result: %d", transactionCount))
	})

	t.Run("RealtimeGetPendingBlockTransactionCount", func(t *testing.T) {
		transactionCount, err := client.RealtimeGetPendingBlockTransactionCount(ctx)
		require.NoError(t, err)
		log.Info(fmt.Sprintf("RealtimeGetPendingBlockTransactionCount result: %d", transactionCount))
	})

	t.Run("RealtimeGetTransactionByHash", func(t *testing.T) {
		result, err := client.RealtimeGetTransactionByHash(ctx, common.HexToHash(txHash))
		require.NoError(t, err)
		log.Info(fmt.Sprintf("RealtimeGetTransactionByHash result type: %T", result))
	})

	t.Run("RealtimeGetRawTransactionByHash", func(t *testing.T) {
		result, err := client.RealtimeGetRawTransactionByHash(ctx, common.HexToHash(txHash))
		require.NoError(t, err)
		log.Info(fmt.Sprintf("RealtimeGetRawTransactionByHash result type: %T", result))
	})

	t.Run("RealtimeGetTransactionReceipt", func(t *testing.T) {
		receipt, err := client.RealtimeGetTransactionReceipt(ctx, common.HexToHash(txHash))
		require.NoError(t, err)
		require.NotNil(t, receipt)
		log.Info(fmt.Sprintf("RealtimeGetTransactionReceipt result type: %T", receipt))
	})

	t.Run("RealtimeGetInternalTransactions", func(t *testing.T) {
		tx, err := client.RealtimeGetInternalTransactions(ctx, common.HexToHash(txHash))
		require.NoError(t, err)
		log.Info(fmt.Sprintf("RealtimeGetInternalTransactions result type: %T", tx))
	})

	t.Run("RealtimeGetBalance", func(t *testing.T) {
		balance, err := client.RealtimeGetBalance(ctx, testAddress)
		require.NoError(t, err)
		require.Equal(t, originBalance.Add(originBalance, big.NewInt(Gwei)).String(), balance.String(), "Balance should increase by 1 Gwei")
		log.Info(fmt.Sprintf("RealtimeGetBalance result for test address: %s", balance.String()))
	})

	t.Run("RealtimeGetTransactionCount", func(t *testing.T) {
		nonce, err := client.RealtimeGetTransactionCount(ctx, fromAddress)
		require.NoError(t, err)
		require.Equal(t, originNonce+2, nonce)
		log.Info(fmt.Sprintf("RealtimeGetTransactionCount result for sender address: %d", nonce))
	})

	t.Run("RealtimeGetCode", func(t *testing.T) {
		code, err := client.RealtimeGetCode(ctx, erc20Address)
		require.NoError(t, err)
		require.NotEmpty(t, code, "Contract code should not be empty")
		log.Info(fmt.Sprintf("RealtimeGetCode result for erc20 contract %s: %s", erc20Address, code))
	})

	t.Run("RealtimeGetStorageAt", func(t *testing.T) {
		// 0x2 is refered to _totalSupply field
		value, err := client.RealtimeGetStorageAt(ctx, erc20Address, "0x2")
		require.NoError(t, err)
		require.Equal(t, "0x00000000000000000000000000000000000000000052b7d2dcc80cd2e4000000", value, "Storage at index 0x2 should be equal to 1000000000000000000000")
		log.Info(fmt.Sprintf("RealtimeGetStorageAt result for erc20 contract %s at index %s: %s", erc20Address, "0x2", value))
	})

	t.Run("RealtimeCall", func(t *testing.T) {
		data, err := erc20ABI.Pack("balanceOf", fromAddress)
		require.NoError(t, err)
		value, err := client.RealtimeCall(ctx, testAddress, erc20Address, "0x100000", "0x1", "0x0", fmt.Sprintf("0x%x", data))
		require.NoError(t, err)
		require.Equal(t, "0x00000000000000000000000000000000000000000052b7d2dcc80cd2e4000000", value, fmt.Sprintf("Balance of %s should be equal to 1000000000000000000000", fromAddress))
		log.Info(fmt.Sprintf("RealtimeCall result for erc20 contract %s calling method balanceOf %s: %s", erc20Address, fromAddress, value))
	})
}
