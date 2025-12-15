package services

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	"github.com/nitinmittal23/go-pos-airdrop/database"
)

type Consumer struct {
	db                        *database.FirestoreDB
	transactionBaseUrl        string
	startTimestamp            int64
	transactionCollectionId   string
	lastProcessedCollectionId string
	lastProcessedDocumentId   string
}

func NewConsumer(
	db *database.FirestoreDB,
	transactionBaseUrl string,
	startTimestamp int64,
	transactionCollectionId string,
	lastProcessedCollectionId string,
	lastProcessedDocumentId string,
) *Consumer {
	return &Consumer{
		db:                        db,
		transactionBaseUrl:        transactionBaseUrl,
		startTimestamp:            startTimestamp,
		transactionCollectionId:   transactionCollectionId,
		lastProcessedCollectionId: lastProcessedCollectionId,
		lastProcessedDocumentId:   lastProcessedDocumentId,
	}
}

func (a *Consumer) getLastSyncedTimestamp(ctx context.Context) (int64, error) {
	log.Printf("[DEBUG] AirdropConsumer.getLastSyncedTimestamp: Fetching last synced timestamp")

	data, err := a.db.GetDocument(
		ctx,
		a.lastProcessedCollectionId,
		a.lastProcessedDocumentId,
	)

	if err != nil {
		log.Printf("[ERROR] AirdropConsumer.getLastSyncedTimestamp: Failed to fetch last synced timestamp: %v", err)
		return 0, nil
	}

	if data == nil {
		log.Printf("[INFO] AirdropConsumer.getLastSyncedTimestamp: No last processed document found, returning 0")
		return 0, nil
	}

	timestamp := int64(0)
	if ts, ok := data["lastProcessedTimestamp"].(int64); ok {
		timestamp = ts
	} else if ts, ok := data["lastProcessedTimestamp"].(float64); ok {
		// Firestore sometimes returns numbers as float64
		timestamp = int64(ts)
	}

	log.Printf("[INFO] AirdropConsumer.getLastSyncedTimestamp: Last synced timestamp retrieved: %d", timestamp)

	return timestamp, nil
}

func reverseTransactions(transactions []map[string]interface{}) {
	for i, j := 0, len(transactions)-1; i < j; i, j = i+1, j-1 {
		transactions[i], transactions[j] = transactions[j], transactions[i]
	}
}

func (a *Consumer) getTransactionsSince(timestamp int64) ([]map[string]interface{}, error) {
	var transactions []map[string]interface{}
	hasNextPage := true
	limit := 1000
	page := 0
	totalFetched := 0

	fmt.Printf("[INFO] Starting to fetch transactions - afterTimestamp: %d, limit: %d\n", timestamp, limit)
	for hasNextPage {
		url := fmt.Sprintf("%s/transactions?userAddress=&sourceNetworkIds=0&destinationNetworkIds=-1&afterTimestamp=%d&page=%d&pageSize=%d",
			a.transactionBaseUrl, timestamp, page, limit)

		resp, err := http.Get(url)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch transactions: %w", err)
		}

		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()

		if err != nil {
			return nil, fmt.Errorf("failed to read response: %w", err)
		}

		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("API returned status code %d", resp.StatusCode)
		}

		var response map[string]interface{}
		if err := json.Unmarshal(body, &response); err != nil {
			fmt.Printf("[ERROR] Failed to parse JSON - page: %d, error: %v\n", page, err)
			return nil, fmt.Errorf("failed to parse response: %w", err)
		}

		if result, ok := response["result"].([]interface{}); ok && len(result) > 0 {
			for _, item := range result {
				if txMap, ok := item.(map[string]interface{}); ok {
					transactions = append(transactions, txMap)
					totalFetched++
				}
			}
		}

		if len(transactions) != limit {
			hasNextPage = false
		}

		page++
	}

	fmt.Printf("[INFO] Transactions fetched successfully - totalTransactions: %d, totalPages: %d\n",
		totalFetched, page)

	for i := range transactions {
		transactions[i]["airdrop_completed"] = false
	}

	reverseTransactions(transactions)

	return transactions, nil
}

func (a *Consumer) saveTransactionsInDatabase(ctx context.Context, transactions []map[string]interface{}) error {
	if len(transactions) == 0 {
		log.Printf("[INFO] AirdropConsumer.saveTransactionsInDatabase: No transactions to save")
		return nil
	}

	log.Printf("[INFO] AirdropConsumer.saveTransactionsInDatabase: Saving %d transactions to database", len(transactions))

	var docDatas []map[string]interface{}
	var docIds []string

	for _, tx := range transactions {
		docDatas = append(docDatas, tx)
		if id, ok := tx["_id"].(string); ok {
			docIds = append(docIds, id)
		} else {
			log.Printf("[WARN] AirdropConsumer.saveTransactionsInDatabase: Transaction missing _id field, skipping")
		}
	}

	err := a.db.AddDocuments(
		ctx,
		a.transactionCollectionId,
		docDatas,
		docIds,
	)

	if err != nil {
		log.Printf("[ERROR] AirdropConsumer.saveTransactionsInDatabase: Failed to save transactions: %v", err)
		return fmt.Errorf("failed to save transactions: %w", err)
	}

	log.Printf("[INFO] AirdropConsumer.saveTransactionsInDatabase: Transactions saved successfully")

	return nil
}

func (a *Consumer) Consume(ctx context.Context) error {
	log.Printf("[INFO] AirdropConsumer.Consume: Starting consumption process")

	lastSyncedTimestamp, err := a.getLastSyncedTimestamp(ctx)

	if err != nil {
		log.Printf("[ERROR] AirdropConsumer.Consume: Error getting last synced timestamp: %v", err)
		return err
	}

	if lastSyncedTimestamp == 0 {
		lastSyncedTimestamp = a.startTimestamp
	}

	transactions, err := a.getTransactionsSince(lastSyncedTimestamp)

	if err != nil {
		fmt.Printf("[ERROR] AirdropConsumer.Start - Airdrop consumer process failed: %v\n", err)
		return err
	}

	if len(transactions) == 0 {
		fmt.Println("[INFO] AirdropConsumer.Consume - No new transactions to process")
		return nil
	}

	if err := a.saveTransactionsInDatabase(ctx, transactions); err != nil {
		fmt.Printf("[ERROR] AirdropConsumer.Consume - Failed to save transactions: %v\n", err)
		return err
	}

	var newLastSyncedTimestamp int64
	if timestamp, ok := transactions[0]["timestamp"].(string); ok {
		// Parse timestamp string to time.Time
		t, err := time.Parse(time.RFC3339, timestamp)
		if err != nil {
			fmt.Printf("[ERROR] AirdropConsumer.Start - Failed to parse timestamp: %v\n", err)
			return err
		}
		newLastSyncedTimestamp = t.UnixMilli()
	} else if timestamp, ok := transactions[0]["timestamp"].(float64); ok {
		// If timestamp is already a number
		newLastSyncedTimestamp = int64(timestamp)
	} else {
		return fmt.Errorf("invalid timestamp format in transaction")
	}

	updateData := []map[string]interface{}{
		{
			"lastProcessedTimestamp": newLastSyncedTimestamp,
		},
	}

	err = a.db.UpdateDocuments(
		ctx,
		a.lastProcessedCollectionId,
		updateData,
		[]string{a.lastProcessedDocumentId},
	)

	if err != nil {
		fmt.Printf("[ERROR] AirdropConsumer.Start - Failed to update last processed timestamp: %v\n", err)
		return err
	}

	fmt.Printf("[INFO] AirdropConsumer.Start - function completed - transactionsLength: %d, newLastSyncedTimestamp: %d\n",
		len(transactions), newLastSyncedTimestamp)

	return nil
}
