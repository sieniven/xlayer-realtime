package test

import (
	"context"
	"fmt"
	"math/big"
	"strings"
	"testing"
	"time"

	ethereum "github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/sieniven/xlayer-realtime/rtclient"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

var (
	Iterations = 20
)

func TestRealtimeBenchmarkNativeTransfer(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	ctx := context.Background()
	rtclient, err := rtclient.Dial(DefaultL2NetworkURL)
	require.NoError(t, err)
	ethclient, err := ethclient.Dial(DefaultL2NetworkURL)
	require.NoError(t, err)
	blockNumber := setupRealtimeTestEnvironment(t, ctx, rtclient)

	t.Run("RealtimeGetBlockTransactionCountByNumber", func(t *testing.T) {
		transactionCount, err := rtclient.RealtimeGetBlockTransactionCountByNumber(ctx, blockNumber)
		require.NoError(t, err)
		log.Info(fmt.Sprintf("RealtimeGetBlockTransactionCountByNumber result: %d", transactionCount))
	})

	// Default test address for tests that require an address
	testAddress := common.HexToAddress("0x1234567890123456789012345678901234567890")

	// Benchmark variables
	var totalRealtimeDuration, totalEthDuration time.Duration
	var totalRealtimeBalanceDuration, totalEthBalanceDuration time.Duration

	// Benchmark transfer tx to test address
	for i := 0; i < Iterations; i++ {
		balance, err := ethclient.BalanceAt(ctx, testAddress, nil)
		require.NoError(t, err)
		realtimeBalance, err := rtclient.RealtimeGetBalance(ctx, testAddress)
		require.NoError(t, err)
		require.Equal(t, balance.String(), realtimeBalance.String())

		// Send tx
		signedTx := nativeTransferTx(t, context.Background(), ethclient, big.NewInt(Gwei), testAddress.String())

		// Run stateless benchmark
		g, ctx := errgroup.WithContext(ctx)
		var realtimeDuration, ethDuration time.Duration
		g.Go(func() error {
			duration, err := WaitCallback(ctx, rtclient, *ethclient, signedTx, common.Address{}, *signedTx.To(), balance, DefaultTimeoutTxToBeMined, WaitMinedRealtime)
			if err != nil {
				return err
			}
			realtimeDuration = duration
			return nil
		})

		g.Go(func() error {
			duration, err := WaitCallback(ctx, rtclient, *ethclient, signedTx, common.Address{}, *signedTx.To(), balance, DefaultTimeoutTxToBeMined, WaitMinedEth)
			if err != nil {
				return err
			}
			ethDuration = duration
			return nil
		})

		// Run state benchmark
		var realtimeBalanceDuration, ethBalanceDuration time.Duration
		g.Go(func() error {
			duration, err := WaitCallback(ctx, rtclient, *ethclient, signedTx, common.Address{}, *signedTx.To(), balance, DefaultTimeoutTxToBeMined, WaitAccountBalanceRealtime)
			if err != nil {
				return err
			}
			realtimeBalanceDuration = duration
			return nil
		})

		g.Go(func() error {
			duration, err := WaitCallback(ctx, rtclient, *ethclient, signedTx, common.Address{}, *signedTx.To(), balance, DefaultTimeoutTxToBeMined, WaitAccountBalanceEth)
			if err != nil {
				return err
			}
			ethBalanceDuration = duration
			return nil
		})

		// Wait for all goroutines to complete
		err = g.Wait()
		require.NoError(t, err)

		totalRealtimeDuration += realtimeDuration
		totalEthDuration += ethDuration
		totalRealtimeBalanceDuration += realtimeBalanceDuration
		totalEthBalanceDuration += ethBalanceDuration

		fmt.Printf("Iteration %v:\n", i)
		fmt.Printf("Realtime stateless duration: %s\n", realtimeDuration)
		fmt.Printf("Eth stateless duration: %s\n", ethDuration)
		fmt.Printf("Realtime state duration: %s\n", realtimeBalanceDuration)
		fmt.Printf("Eth state duration: %s\n", ethBalanceDuration)
	}

	avgRealtimeDuration := time.Duration(int64(totalRealtimeDuration) / int64(Iterations))
	avgEthDuration := time.Duration(int64(totalEthDuration) / int64(Iterations))
	avgRealtimeBalanceDuration := time.Duration(int64(totalRealtimeBalanceDuration) / int64(Iterations))
	avgEthBalanceDuration := time.Duration(int64(totalEthBalanceDuration) / int64(Iterations))

	// Log out metrics
	fmt.Printf("Avg realtime stateless native tx transfer confirmation duration: %s\n", avgRealtimeDuration)
	fmt.Printf("Avg eth stateless native tx transfer confirmation duration: %s\n", avgEthDuration)
	fmt.Printf("Avg realtime state native tx transfer confirmation duration: %s\n", avgRealtimeBalanceDuration)
	fmt.Printf("Avg eth state native tx transfer confirmation duration: %s\n", avgEthBalanceDuration)
}

func TestRealtimeBenchmarkERC20Transfer(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	ctx := context.Background()
	rtclient, err := rtclient.Dial(DefaultL2NetworkURL)
	require.NoError(t, err)
	ethclient, err := ethclient.Dial(DefaultL2NetworkURL)
	require.NoError(t, err)
	blockNumber := setupRealtimeTestEnvironment(t, ctx, rtclient)

	t.Run("RealtimeGetBlockTransactionCountByNumber", func(t *testing.T) {
		transactionCount, err := rtclient.RealtimeGetBlockTransactionCountByNumber(ctx, blockNumber)
		require.NoError(t, err)
		log.Info(fmt.Sprintf("RealtimeGetBlockTransactionCountByNumber result: %d", transactionCount))
	})

	privateKey, err := crypto.HexToECDSA(strings.TrimPrefix(DefaultL2AdminPrivateKey, "0x"))
	require.NoError(t, err)

	// Default test address for tests that require an address
	fromAddress := common.HexToAddress(DefaultL2AdminAddress)
	testAddress := common.HexToAddress("0x1234567890123456789012345678901234567890")

	// Deploy the contract
	erc20Address := deployERC20Contract(t, ctx, privateKey, ethclient)
	transferAmount := new(big.Int).Mul(big.NewInt(1), big.NewInt(1e18)) // Adjust for token decimals (18 in this case)

	startNonce, err := ethclient.PendingNonceAt(context.Background(), fromAddress)
	require.NoError(t, err)

	// Benchmark variables
	var totalRealtimeDuration, totalEthDuration time.Duration
	var totalRealtimeBalanceDuration, totalEthBalanceDuration time.Duration

	// Benchmark erc20 transfer tx
	for i := 0; i < Iterations; i++ {
		balance, err := rtclient.EthGetTokenBalance(ctx, *ethclient, testAddress, erc20Address)
		require.NoError(t, err)
		realtimeBalance, err := rtclient.RealtimeGetTokenBalance(ctx, fromAddress, testAddress, erc20Address)
		require.NoError(t, err)
		require.Equal(t, balance.String(), realtimeBalance.String())

		signedTx := erc20TransferTx(t, ctx, privateKey, ethclient, transferAmount, testAddress, erc20Address, startNonce+uint64(i))

		// Run stateless benchmark
		g, ctx := errgroup.WithContext(ctx)
		var realtimeDuration, ethDuration time.Duration
		g.Go(func() error {
			duration, err := WaitCallback(ctx, rtclient, *ethclient, signedTx, common.Address{}, *signedTx.To(), balance, DefaultTimeoutTxToBeMined, WaitMinedRealtime)
			if err != nil {
				return err
			}
			realtimeDuration = duration
			return nil
		})

		g.Go(func() error {
			duration, err := WaitCallback(ctx, rtclient, *ethclient, signedTx, common.Address{}, *signedTx.To(), balance, DefaultTimeoutTxToBeMined, WaitMinedEth)
			if err != nil {
				return err
			}
			ethDuration = duration
			return nil
		})

		// Run state benchmark
		var realtimeBalanceDuration, ethBalanceDuration time.Duration
		g.Go(func() error {
			duration, err := WaitCallback(ctx, rtclient, *ethclient, signedTx, fromAddress, testAddress, balance, DefaultTimeoutTxToBeMined, WaitTokenBalanceRealtime)
			if err != nil {
				return err
			}
			realtimeBalanceDuration = duration
			return nil
		})

		g.Go(func() error {
			duration, err := WaitCallback(ctx, rtclient, *ethclient, signedTx, fromAddress, testAddress, balance, DefaultTimeoutTxToBeMined, WaitTokenBalanceEth)
			if err != nil {
				return err
			}
			ethBalanceDuration = duration
			return nil
		})

		// Wait for all goroutines to complete
		err = g.Wait()
		require.NoError(t, err)

		totalRealtimeDuration += realtimeDuration
		totalEthDuration += ethDuration
		totalRealtimeBalanceDuration += realtimeBalanceDuration
		totalEthBalanceDuration += ethBalanceDuration

		fmt.Printf("Iteration %v:\n", i)
		fmt.Printf("Realtime stateless duration: %s\n", realtimeDuration)
		fmt.Printf("Eth stateless duration: %s\n", ethDuration)
		fmt.Printf("Realtime state duration: %s\n", realtimeBalanceDuration)
		fmt.Printf("Eth state duration: %s\n", ethBalanceDuration)
	}

	avgRealtimeDuration := time.Duration(int64(totalRealtimeDuration) / int64(Iterations))
	avgEthDuration := time.Duration(int64(totalEthDuration) / int64(Iterations))
	avgRealtimeBalanceDuration := time.Duration(int64(totalRealtimeBalanceDuration) / int64(Iterations))
	avgEthBalanceDuration := time.Duration(int64(totalEthBalanceDuration) / int64(Iterations))

	// Log out metrics
	fmt.Printf("Avg realtime stateless erc20 tx transfer confirmation duration: %s\n", avgRealtimeDuration)
	fmt.Printf("Avg eth stateless erc20 tx transfer confirmation duration: %s\n", avgEthDuration)
	fmt.Printf("Avg realtime state erc20 tx transfer confirmation duration: %s\n", avgRealtimeBalanceDuration)
	fmt.Printf("Avg eth state erc20 tx transfer confirmation duration: %s\n", avgEthBalanceDuration)
}

func TestRealtimeBenchmarNewHeadsSubscription(t *testing.T) {
	ctx := context.Background()
	wsClient, err := rpc.Dial(DefaultL2NetworkWSURL)
	require.NoError(t, err)

	// Benchmark variables
	var totalSubTimeDiff time.Duration

	realtimeMsgCh := make(chan RealtimeResult)
	realtimeSub, err := wsClient.Subscribe(ctx, "realtime", realtimeMsgCh, "realtime", map[string]bool{"NewHeads": true, "TransactionExtraInfo": false, "TransactionReceipt": false, "TransactionInnerTxs": false})
	require.NoError(t, err)
	defer realtimeSub.Unsubscribe()

	ethMsgCh := make(chan types.Header)
	ethSub, err := wsClient.Subscribe(ctx, "eth", ethMsgCh, "newHeads")
	require.NoError(t, err)
	defer ethSub.Unsubscribe()

	// Benchmark realtime vs eth subscibe new block headers
	heights := make(map[int64]time.Time)
	count := 0
	for count < Iterations {
		select {
		case msg := <-realtimeMsgCh:
			if msg.Header != nil {
				fmt.Printf("Realtime subscription message: %+v\n", msg)
				height := msg.Header.Number.Int64()
				heights[height] = time.Now()
			}
		case msg := <-ethMsgCh:
			fmt.Printf("Eth subscription message: %+v\n", msg)
			height := msg.Number.Int64()
			if _, ok := heights[height]; ok {
				timeDiff := time.Since(heights[height])
				totalSubTimeDiff += timeDiff
				count++
				fmt.Printf("Count: %v\n", count)
			}
		case err := <-realtimeSub.Err():
			t.Fatal(err)
		}
	}

	avgTimeDiff := time.Duration(int64(totalSubTimeDiff) / int64(Iterations))
	fmt.Printf("Avg realtime subscription newHeads is faster than eth subscription newHeads by: %s\n", avgTimeDiff)
}

func TestRealtimeBenchmarNewTransactionSubscription(t *testing.T) {
	ctx := context.Background()
	client, err := ethclient.Dial(DefaultL2NetworkURL)
	require.NoError(t, err)

	wsClient, err := rpc.Dial(DefaultL2NetworkWSURL)
	require.NoError(t, err)

	// Default test address for tests that require an address
	testAddress := common.HexToAddress("0x1234567890123456789012345678901234567890")

	// Benchmark variables
	var totalRealtimeDuration time.Duration

	realtimeMsgCh := make(chan RealtimeResult)
	realtimeSub, err := wsClient.Subscribe(ctx, "realtime", realtimeMsgCh, "realtime", map[string]bool{"NewHeads": false, "TransactionExtraInfo": false, "TransactionReceipt": false, "TransactionInnerTxs": false})
	require.NoError(t, err)
	defer realtimeSub.Unsubscribe()

	for i := 0; i < Iterations; i++ {
		// Send tx
		signedTx := nativeTransferTx(t, ctx, client, big.NewInt(Gwei), testAddress.String())

		g, _ := errgroup.WithContext(ctx)
		var subDuration time.Duration

		// realtime subscription
		g.Go(func() error {
			startTime := time.Now()

			for {
				select {
				case msg := <-realtimeMsgCh:
					if msg.TxHash == signedTx.Hash().String() {
						subDuration = time.Since(startTime)
						return nil
					}
				case err := <-realtimeSub.Err():
					return err
				case <-time.After(DefaultTimeoutTxToBeMined):
					return fmt.Errorf("realtime subscription timeout")
				}
			}
		})

		// Wait for all goroutines to complete
		err = g.Wait()
		require.NoError(t, err)

		totalRealtimeDuration += subDuration

		fmt.Printf("Iteration %v:\n", i)
		fmt.Printf("Realtime transaction subscription duration: %s\n", subDuration)
	}

	avgDuration := time.Duration(int64(totalRealtimeDuration) / int64(Iterations))

	// Log out metrics
	fmt.Printf("Avg realtime transaction subscription duration: %s\n", avgDuration)
}

func TestRealtimeBenchmarkLogSubscription(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	ctx := context.Background()
	rtclient, err := rtclient.Dial(DefaultL2NetworkURL)
	require.NoError(t, err)
	ethclient, err := ethclient.Dial(DefaultL2NetworkURL)
	require.NoError(t, err)
	blockNumber := setupRealtimeTestEnvironment(t, ctx, rtclient)

	t.Run("RealtimeGetBlockTransactionCountByNumber", func(t *testing.T) {
		transactionCount, err := rtclient.RealtimeGetBlockTransactionCountByNumber(ctx, blockNumber)
		require.NoError(t, err)
		log.Info(fmt.Sprintf("RealtimeGetBlockTransactionCountByNumber result: %d", transactionCount))
	})

	realtimeWSClient, err := rpc.Dial(DefaultL2NetworkWSURL)
	require.NoError(t, err)
	ethWSClient, err := rpc.Dial(DefaultL2NetworkWSURL)
	require.NoError(t, err)

	privateKey, err := crypto.HexToECDSA(strings.TrimPrefix(DefaultL2AdminPrivateKey, "0x"))
	require.NoError(t, err)

	// Default test address for tests that require an address
	testAddress := common.HexToAddress("0x1234567890123456789012345678901234567890")

	// Deploy the contract
	erc20Address := deployERC20Contract(t, ctx, privateKey, ethclient)
	transferAmount := new(big.Int).Mul(big.NewInt(1), big.NewInt(1e18)) // Adjust for token decimals (18 in this case)

	fromAddress := common.HexToAddress(DefaultL2AdminAddress)
	startNonce, err := ethclient.PendingNonceAt(context.Background(), fromAddress)
	require.NoError(t, err)

	// Benchmark variables
	var totalRealtimeDuration, totalEthDuration time.Duration

	// Subscirbed topic
	erc20TransferTopic := common.HexToHash(erc20TransferTopicHex)
	q, err := toLogFilterArg(ethereum.FilterQuery{Topics: [][]common.Hash{{erc20TransferTopic}}})
	require.NoError(t, err)

	realtimeCh := make(chan *types.Log)
	realtimeSub, err := realtimeWSClient.Subscribe(ctx, "realtime", realtimeCh, "logs", q)
	require.NoError(t, err)
	defer realtimeSub.Unsubscribe()

	ethCh := make(chan *types.Log)
	ethSub, err := ethWSClient.Subscribe(ctx, "eth", ethCh, "logs", q)
	require.NoError(t, err)
	defer ethSub.Unsubscribe()

	// Benchmark subscibe realtime log
	for i := 0; i < Iterations; i++ {
		// Send tx
		erc20TransferTx(t, ctx, privateKey, ethclient, transferAmount, testAddress, erc20Address, startNonce+uint64(i))

		g, _ := errgroup.WithContext(ctx)
		var realtimeDuration, ethDuration time.Duration

		// realtime subscription
		g.Go(func() error {
			startTime := time.Now()

			select {
			case log := <-realtimeCh:
				if log.Topics[0] != erc20TransferTopic {
					return fmt.Errorf("realtime subscription fetched unknown logs")
				}
				realtimeDuration = time.Since(startTime)
				return nil
			case err := <-realtimeSub.Err():
				return err
			case <-time.After(DefaultTimeoutTxToBeMined):
				return fmt.Errorf("realtime subscription timeout")
			}
		})

		// eth subscription
		g.Go(func() error {
			startTime := time.Now()

			select {
			case log := <-ethCh:
				if log.Topics[0] != erc20TransferTopic {
					return fmt.Errorf("eth subscription fetched unknown logs")
				}
				ethDuration = time.Since(startTime)
				return nil
			case err := <-ethSub.Err():
				return err
			case <-time.After(DefaultTimeoutTxToBeMined):
				return fmt.Errorf("eth subscription timeout")
			}
		})

		// Wait for all goroutines to complete
		err = g.Wait()
		require.NoError(t, err)

		totalRealtimeDuration += realtimeDuration
		totalEthDuration += ethDuration

		fmt.Printf("Iteration %v:\n", i)
		fmt.Printf("Realtime log subscription duration: %s\n", totalRealtimeDuration)
		fmt.Printf("Eth log subscription duration: %s\n", totalEthDuration)
	}

	avgRealtimeDuration := time.Duration(int64(totalRealtimeDuration) / int64(Iterations))
	avgEthDuration := time.Duration(int64(totalEthDuration) / int64(Iterations))

	// Log out metrics
	fmt.Printf("Avg realtime log subscription duration: %s\n", avgRealtimeDuration)
	fmt.Printf("Avg eth log subscription duration: %s\n", avgEthDuration)
}
