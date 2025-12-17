package middleware

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
)

// CheckAddress middleware validates that address parameter is present
func CheckGetTransactionQuery() gin.HandlerFunc {
	return func(c *gin.Context) {
		address := c.Query("address")
		offset := c.Query("offset")
		limit := c.Query("limit")

		fmt.Println("offset", offset, limit, address)

		if address != "" {
			address = strings.ToLower(address)
		}

		offsetValue := 0
		if offset != "" {
			offsetValue, _ = strconv.Atoi(offset)
		}

		limitValue := 0
		if limit != "" {
			limitValue, _ = strconv.Atoi(limit)
		}

		// Store address in context for handler to use if needed
		c.Set("address", address)
		c.Set("offset", offsetValue)
		c.Set("limit", limitValue)
		c.Next()
	}
}
