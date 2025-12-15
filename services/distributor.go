package services

import (
	"context"
	"crypto/ecdsa"
	"fmt"
	"log"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/params"
	"github.com/nitinmittal23/go-pos-airdrop/database"
)

type Distributor struct {
	db                      *database.FirestoreDB
	etherClient             *ethclient.Client
	privateKey              *ecdsa.PrivateKey
	walletAddress           common.Address
	airdropAmount           float64
	transactionCollectionId string
}

func NewDistributor(
	db *database.FirestoreDB,
	etherClient *ethclient.Client,
	privateKeyHex string,
	airdropAmount float64,
	transactionCollectionId string,
) (*Distributor, error) {
	if len(privateKeyHex) > 2 && privateKeyHex[:2] == "0x" {
		privateKeyHex = privateKeyHex[2:]
	}

	privateKey, err := crypto.HexToECDSA(privateKeyHex)
	if err != nil {
		return nil, fmt.Errorf("invalid private key: %w", err)
	}

	publicKey := privateKey.Public()
	publicKeyECDSA, ok := publicKey.(*ecdsa.PublicKey)
	if !ok {
		return nil, fmt.Errorf("error casting public key to ECDSA")
	}

	walletAddress := crypto.PubkeyToAddress(*publicKeyECDSA)
	fmt.Printf("Wallet initialized: %s\n", walletAddress.Hex())

	return &Distributor{
		db:                      db,
		etherClient:             etherClient,
		privateKey:              privateKey,
		walletAddress:           walletAddress,
		airdropAmount:           airdropAmount,
		transactionCollectionId: transactionCollectionId,
	}, nil
}

func weiToEth(wei *big.Int) float64 {
	fbalance := new(big.Float)
	fbalance.SetString(wei.String())
	ethValue := new(big.Float).Quo(fbalance, big.NewFloat(params.Ether))
	result, _ := ethValue.Float64()
	return result
}

func ethToWei(eth float64) *big.Int {
	ethBig := big.NewFloat(eth)
	weiBig := new(big.Float).Mul(ethBig, big.NewFloat(params.Ether))
	weiInt := new(big.Int)
	weiBig.Int(weiInt)
	return weiInt
}

func (a *Distributor) getTransactions(ctx context.Context) ([]map[string]interface{}, error) {
	data, err := a.db.GetDocuments(
		ctx, database.GetDocumentsParams{
			CollectionPath: a.transactionCollectionId,
			Filter: []database.FilterCondition{
				{
					Field:    "airdrop_completed",
					Operator: "==",
					Value:    false,
				},
			},
		},
	)

	if err != nil {
		log.Printf("[ERROR] AirdropDistributor.getTransactions: Failed to fetch transactions: %v", err)
		return nil, nil
	}

	return data.Documents, nil
}

func (a *Distributor) sendAirdrop(
	ctx context.Context,
	receiver common.Address,
	amount *big.Int,
) (string, error) {
	nonce, err := a.etherClient.PendingNonceAt(ctx, a.walletAddress)
	if err != nil {
		return "", fmt.Errorf("failed to get nonce: %w", err)
	}

	gasPrice, err := a.etherClient.SuggestGasPrice(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to get gas price: %w", err)
	}

	tx := types.NewTransaction(
		nonce,
		receiver,
		amount,
		21000, // gas limit for simple transfer
		gasPrice,
		nil,
	)

	chainID, err := a.etherClient.NetworkID(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to get chain ID: %w", err)
	}

	signedTx, err := types.SignTx(tx, types.NewEIP155Signer(chainID), a.privateKey)
	if err != nil {
		return "", fmt.Errorf("failed to sign transaction: %w", err)
	}

	err = a.etherClient.SendTransaction(ctx, signedTx)
	if err != nil {
		return "", fmt.Errorf("failed to send transaction: %w", err)
	}

	return signedTx.Hash().Hex(), nil
}

func (a *Distributor) waitForTransaction(
	ctx context.Context,
	txHash string,
	timeout time.Duration,
) (*types.Receipt, error) {
	hash := common.HexToHash(txHash)

	timeoutCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-timeoutCtx.Done():
			return nil, fmt.Errorf("transaction confirmation timed out")
		case <-ticker.C:
			receipt, err := a.etherClient.TransactionReceipt(ctx, hash)
			if err == nil {
				return receipt, nil
			}
		}
	}
}

func (a *Distributor) processTransaction(ctx context.Context, transaction map[string]interface{}) (string, error) {
	transactionID, ok := transaction["_id"].(string)
	if !ok {
		transactionID = fmt.Sprintf("%v", transaction["_id"])
	}

	receiver, ok := transaction["receiver"].(string)

	if !ok || receiver == "" {
		log.Printf("[ERROR] AirdropDistributor.processTransaction: Receiver address is empty")
		a.db.UpdateDocuments(
			context.Background(),
			[]string{a.transactionCollectionId},
			[]map[string]interface{}{
				{
					"airdrop_completed":   true,
					"airdrop_message":     "No receiver address provided",
					"airdrop_completedAt": time.Now().Format(time.RFC3339),
				},
			}, []string{transactionID})
		return "skipped", nil
	}

	receiverAddress := common.HexToAddress(receiver)
	balance, err := a.etherClient.BalanceAt(ctx, receiverAddress, nil)

	if err != nil {
		return "", fmt.Errorf("failed to get balance for %s: %w", receiver, err)
	}

	balanceInEth := weiToEth(balance)

	if balanceInEth < a.airdropAmount {
		fmt.Printf("[INFO] AirdropDistributor.processTransaction - Sending airdrop to %s, current balance: %f\n",
			receiver, balanceInEth)
		airdropAmountWei := ethToWei(a.airdropAmount)

		// Send transaction
		txHash, err := a.sendAirdrop(ctx, receiverAddress, airdropAmountWei)
		if err != nil {
			return "", fmt.Errorf("failed to send airdrop to %s: %w", receiver, err)
		}

		// Wait for transaction confirmation
		receipt, err := a.waitForTransaction(ctx, txHash, 60*time.Second)
		if err != nil {
			return "", fmt.Errorf("transaction confirmation failed: %w", err)
		}

		if receipt.Status == 0 {
			return "", fmt.Errorf("transaction failed on-chain")
		}

		fmt.Printf("[INFO] AirdropDistributor.processTransaction - Airdrop sent to %s, tx hash: %s\n",
			receiver, txHash)

		err = a.db.UpdateDocuments(
			ctx,
			a.transactionCollectionId,
			[]map[string]interface{}{
				{
					"airdrop_completed":       true,
					"airdrop_transactionHash": txHash,
					"airdrop_completedAt":     time.Now().Format(time.RFC3339),
				},
			},
			[]string{transactionID},
		)
		if err != nil {
			return "sent", fmt.Errorf("airdrop sent but failed to update document: %w", err)
		}

		return "sent", nil
	}

	fmt.Printf("[INFO] AirdropDistributor.processTransaction - Skipping airdrop for %s, balance %f >= %f\n",
		receiver, balanceInEth, a.airdropAmount)
	a.db.UpdateDocuments(
		context.Background(),
		[]string{a.transactionCollectionId},
		[]map[string]interface{}{
			{
				"airdrop_completed":   true,
				"airdrop_message":     fmt.Sprintf("Account already has more than %f balance", a.airdropAmount),
				"airdrop_completedAt": time.Now().Format(time.RFC3339),
			},
		}, []string{transaction["_id"].(string)},
	)
	return "skipped", nil
}

func (a *Distributor) Distribute(ctx context.Context) error {
	fmt.Println("[INFO] AirdropDistributor.Start - Starting airdrop distributor process")
	transactions, err := a.getTransactions(ctx)
	if err != nil {
		return fmt.Errorf("failed to get transactions: %w", err)
	}

	if len(transactions) == 0 {
		fmt.Println("[INFO] AirdropDistributor.Start - No transactions to process")
		return nil
	}

	successCount := 0
	skippedCount := 0

	for _, transaction := range transactions {
		result, err := a.processTransaction(ctx, transaction)
		if err != nil {
			log.Printf("[ERROR] AirdropDistributor.Distribute: Failed to process transaction: %v", err)
		}

		switch result {
		case "sent":
			successCount++
		case "skipped":
			skippedCount++
		}
	}

	fmt.Printf("[INFO] AirdropDistributor.Start - Completed - Sent: %d, Skipped: %d\n",
		successCount, skippedCount)

	return nil
}
