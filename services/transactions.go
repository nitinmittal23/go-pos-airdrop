package services

import (
	"context"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/nitinmittal23/go-pos-airdrop/database"
)

type Transactions struct {
	db                      *database.FirestoreDB
	transactionCollectionId string
}

func NewTransactions(
	db *database.FirestoreDB,
	transactionCollectionId string,
) (*Transactions, error) {
	return &Transactions{
		db:                      db,
		transactionCollectionId: transactionCollectionId,
	}, nil
}

func (a *Transactions) GetTransactions(c *gin.Context) {
	address, _ := c.Get("address")
	limit, _ := c.Get("limit")
	offset, _ := c.Get("offset")

	filters := []database.FilterCondition{}

	if address != "" {
		filters = append(filters, database.FilterCondition{
			Field:    "receiver",
			Operator: "==",
			Value:    address,
		})
	}

	documents, err := a.db.GetDocuments(
		context.Background(),
		database.GetDocumentsParams{
			CollectionPath:            a.transactionCollectionId,
			Filter:                    filters,
			ReturnTotalDocumentsCount: true,
			Limit:                     limit.(int),
			Offset:                    offset.(int),
		},
	)

	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"status":  "error",
			"message": "Failed to fetch transactions",
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"status": "success",
		"result": documents.Documents,
		"pagination": gin.H{
			"total":  documents.TotalDocumentsCount,
			"limit":  limit,
			"offset": offset,
		},
	})
}
