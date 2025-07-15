package test

import (
	"context"
	"math/big"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	libcommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/holiman/uint256"
	"github.com/sieniven/xlayer-realtime/kafka"
	kafkaTypes "github.com/sieniven/xlayer-realtime/kafka/types"
	realtimeTypes "github.com/sieniven/xlayer-realtime/types"
	"gotest.tools/v3/assert"
)

var (
	sigBytes = "98ff921201554726367d2be8c804a7ff89ccf285ebc57dff8ae4c44b9c19ac4a8887321be575c8095f789dd4c743dfe42c1820f9231f98a962b210e3ac2452a301"

	rightvrsTx = types.NewTransaction(
		3,
		testToAddr,
		big.NewInt(10),
		2000,
		big.NewInt(1),
		libcommon.FromHex("5544"),
	)

	rightvrsTxReceipt = &types.Receipt{
		PostState:         libcommon.Hash{2}.Bytes(),
		CumulativeGasUsed: 3,
		Logs: []*types.Log{
			{Address: libcommon.BytesToAddress([]byte{0x22})},
			{Address: libcommon.BytesToAddress([]byte{0x02, 0x22})},
		},
		TxHash:          rightvrsTx.Hash(),
		ContractAddress: libcommon.BytesToAddress([]byte{0x02, 0x22, 0x22}),
		GasUsed:         2,
	}

	rightvrsTxInnerTxs = []*types.InnerTx{
		{
			Name:     "innerTx1",
			CallType: types.CALL_TYP,
		},
	}

	rightvrsTxChangeset = &realtimeTypes.Changeset{
		BalanceChanges: map[libcommon.Address]*uint256.Int{
			testToAddr: uint256.NewInt(10),
		},
	}

	difficulty, _ = new(big.Int).SetString("8398142613866510000000000000000000000000000000", 10)
	blockHeader   = &types.Header{
		ParentHash:  libcommon.HexToHash("0x8b00fcf1e541d371a3a1b79cc999a85cc3db5ee5637b5159646e1acd3613fd15"),
		UncleHash:   libcommon.HexToHash("1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347"),
		Coinbase:    libcommon.HexToAddress("0x571846e42308df2dad8ed792f44a8bfddf0acb4d"),
		Root:        libcommon.HexToHash("0x351780124dae86b84998c6d4fe9a88acfb41b4856b4f2c56767b51a4e2f94dd4"),
		TxHash:      libcommon.HexToHash("0x6a35133fbff7ea2cb5ee7635c9fb623f96d31d689d806a2bfe40a2b1d90ee99c"),
		ReceiptHash: libcommon.HexToHash("0x324f54860e214ea896ea7a05bda30f85541be3157de77a9059a04fdb1e86badd"),
		Difficulty:  difficulty,
		Number:      big.NewInt(24679923),
		GasLimit:    30_000_000,
		GasUsed:     3_074_345,
		Time:        1666343339,
		Extra:       common.FromHex("0x1234"),
		BaseFee:     big.NewInt(7_000_000_000),
	}
)

func TestKafka(t *testing.T) {
	signer := types.MakeSigner(GetTestChainConfig(DefaultL2ChainID), big.NewInt(1), 0)
	privateKey, err := crypto.HexToECDSA(strings.TrimPrefix(DefaultL2AdminPrivateKey, "0x"))
	assert.NilError(t, err)
	signedTx, err := types.SignTx(rightvrsTx, signer, privateKey)
	assert.NilError(t, err)

	cfg := kafka.KafkaConfig{
		BootstrapServers: []string{"0.0.0.0:9095"},
		BlockTopic:       "xlayer-test-block",
		TxTopic:          "xlayer-test-tx",
		ErrorTopic:       "xlayer-test-error",
		ClientID:         "xlayer-test-consumer",
	}
	producer, err := kafka.NewKafkaProducer(cfg)
	assert.NilError(t, err)

	for i := 0; i < 10; i++ {
		err = producer.SendKafkaTransaction(context.Background(), uint64(i), signedTx, rightvrsTxReceipt, rightvrsTxInnerTxs, rightvrsTxChangeset)
		assert.NilError(t, err)

		err = producer.SendKafkaBlockInfo(context.Background(), blockHeader, 10)
		assert.NilError(t, err)

		err = producer.SendKafkaErrorTrigger(context.Background(), uint64(i))
		assert.NilError(t, err)
	}

	err = producer.Close()
	assert.NilError(t, err)

	consumer, err := kafka.NewKafkaConsumer(cfg)
	assert.NilError(t, err)
	ctx, ctxWithCancel := context.WithCancel(context.Background())
	headersChan := make(chan kafkaTypes.BlockMessage, 10)
	txMsgsChan := make(chan kafkaTypes.TransactionMessage, 10)
	errorMsgsChan := make(chan kafkaTypes.ErrorTriggerMessage, 10)
	errorChan := make(chan error, 10)
	go consumer.ConsumeKafka(ctx, headersChan, txMsgsChan, errorMsgsChan, errorChan)

	// Verify tx messages
	for i := 0; i < 10; i++ {
		select {
		case err := <-errorChan:
			t.Fatalf("Received error from consumer: %v", err)
		case txMsg := <-txMsgsChan:
			AssertCommonTx(t, txMsg, rightvrsTx, uint64(i), types.LegacyTxType)
			AssertReceipt(t, txMsg, rightvrsTxReceipt)
			AssertInnerTxs(t, txMsg, rightvrsTxInnerTxs)
			AssertChangeseet(t, txMsg, rightvrsTxChangeset)
		}
	}

	// Verify header messages
	for i := 0; i < 10; i++ {
		select {
		case err := <-errorChan:
			t.Fatalf("Received error from consumer: %v", err)
		case rcvHeader := <-headersChan:
			header, _, err := rcvHeader.GetBlockInfo()
			assert.NilError(t, err)
			AssertHeader(t, blockHeader, header)
		}
	}

	// Verify error trigger messages
	for i := 0; i < 10; i++ {
		select {
		case err := <-errorChan:
			t.Fatalf("Received error from consumer: %v", err)
		case errorMsg := <-errorMsgsChan:
			assert.Equal(t, errorMsg.BlockNumber, uint64(i))
		}
	}

	ctxWithCancel()
	err = consumer.Close()
	assert.NilError(t, err)
}
