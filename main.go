package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strconv"

	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/gin-gonic/gin"
	"github.com/joho/godotenv"
	"github.com/nitinmittal23/go-pos-airdrop/database"
	middleware "github.com/nitinmittal23/go-pos-airdrop/middlewares"
	"github.com/nitinmittal23/go-pos-airdrop/services"
	"github.com/robfig/cron/v3"
)

func main() {
	err := godotenv.Load()

	if err != nil {
		fmt.Println("Error loading .env file")
		return
	}

	ctx := context.Background()

	projectID := os.Getenv("GOOGLE_CLOUD_PROJECT_ID")
	databaseID := os.Getenv("FIRESTORE_DATABASE_ID")

	if projectID == "" || databaseID == "" {
		fmt.Println("GOOGLE_CLOUD_PROJECT_ID and FIRESTORE_DATABASE_ID must be set")
		return
	}
	databaseClient := database.New(projectID, databaseID)

	err = databaseClient.Connect(ctx)
	if err != nil {
		fmt.Println("Failed to connect to Firestore:", err)
		return
	}

	transactionBaseUrl := os.Getenv("TRANSACTION_BASE_URL")
	startTimestamp, _ := strconv.ParseInt(os.Getenv("START_TIMESTAMP"), 10, 64)

	consumer := services.NewConsumer(
		databaseClient,
		transactionBaseUrl,
		startTimestamp,
		"airdrop_transactions",
		"airdrop_metadata",
		"lastProcessedTimestamp",
	)

	rpcUrl := os.Getenv("RPC_URL")
	privateKey := os.Getenv("PRIVATE_KEY")

	blockChainClient, err := ethclient.Dial(rpcUrl)
	if err != nil {
		fmt.Println("Failed to connect to Ethereum client:", err)
		return
	}

	distributor, err := services.NewDistributor(
		databaseClient,
		blockChainClient,
		privateKey,
		0.01,
		"airdrop_transactions",
	)

	if err != nil {
		fmt.Println("Failed to create distributor:", err)
		return
	}

	crn := cron.New(
		cron.WithChain(
			cron.SkipIfStillRunning(cron.DefaultLogger), // Equivalent to protect: true
		),
	)

	err = consumer.Consume(ctx)
	if err != nil {
		fmt.Println("Error consuming transactions:", err)
		return
	}

	err = distributor.Distribute(ctx)
	if err != nil {
		fmt.Println("Error distributing airdrops:", err)
		return
	}

	crn.AddFunc("*/5 * * * *", func() {
		err := consumer.Consume(ctx)
		if err != nil {
			fmt.Println("Error consuming transactions:", err)
		}
	})

	crn.AddFunc("*/5 * * * *", func() {
		err := distributor.Distribute(ctx)
		if err != nil {
			fmt.Println("Error distributing airdrops:", err)
		}
	})

	crn.Start()
	defer crn.Stop()

	transactionService, _ := services.NewTransactions(
		databaseClient,
		"airdrop_transactions",
	)

	router := gin.Default()

	router.GET("/health-check", healthCheck)

	router.GET("/transactions", middleware.CheckGetTransactionQuery(), transactionService.GetTransactions)

	router.Run(":3000")
}

func healthCheck(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"message": "All services are operational",
	})
}
