package database

import (
	"context"
	"fmt"
	"log"

	"cloud.google.com/go/firestore"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// FirestoreDB is a wrapper around Firestore client with helper methods
type FirestoreDB struct {
	Client     *firestore.Client
	ProjectID  string
	DatabaseID string
}

type FilterCondition struct {
	Field    string
	Operator string // "==", "!=", "<", "<=", ">", ">=", "array-contains", "in", "array-contains-any", "not-in"
	Value    interface{}
}

type OrFilter struct {
	Or []FilterCondition
}

type OrderCondition struct {
	Field string
	Order firestore.Direction // firestore.Asc or firestore.Desc
}

type GetDocumentsResult struct {
	Documents           []map[string]interface{}
	TotalDocumentsCount *int
}

type GetDocumentsParams struct {
	CollectionPath            string
	Filter                    []FilterCondition
	Limit                     int
	Order                     []OrderCondition
	StartAfterCursor          *firestore.DocumentSnapshot
	SelectFields              []string
	OrFilters                 []OrFilter
	Offset                    int
	ReturnTotalDocumentsCount bool
}

var (
	ErrNotConnected     = fmt.Errorf("database not connected")
	ErrDocumentNotFound = fmt.Errorf("document not found")
)

// New creates a new FirestoreDB instance (without connecting)
func New(projectID string, databaseID string) *FirestoreDB {
	if databaseID == "" {
		databaseID = "(default)"
	}

	return &FirestoreDB{
		ProjectID:  projectID,
		DatabaseID: databaseID,
	}
}

func (db *FirestoreDB) Connect(ctx context.Context) error {
	client, err := firestore.NewClientWithDatabase(ctx, db.ProjectID, db.DatabaseID)
	if err != nil {
		log.Printf("Failed to create Firestore client: %v", err)
		return err
	}

	db.Client = client
	log.Printf("Connected to Firestore (Project: %s, Database: %s)", db.ProjectID, db.DatabaseID)
	return nil
}

func (db *FirestoreDB) Close() error {
	if db.Client != nil {
		return db.Client.Close()
	}
	return nil
}

func (db *FirestoreDB) GetDocument(ctx context.Context, collection string, docID string) (map[string]interface{}, error) {
	if db.Client == nil {
		return nil, ErrNotConnected
	}

	docRef := db.Client.Collection(collection).Doc(docID)
	docSnap, err := docRef.Get(ctx)

	if err != nil {
		if status.Code(err) == codes.NotFound {
			log.Printf("Document not found: %s/%s", collection, docID)
			return nil, err
		}
		log.Printf("Error getting document: %v", err)
		return nil, err
	}

	if !docSnap.Exists() {
		log.Printf("Document does not exist: %s/%s", collection, docID)
		return nil, nil
	}

	return docSnap.Data(), nil
}

func (db *FirestoreDB) AddDocuments(
	ctx context.Context,
	collectionPaths interface{}, // Can be string or []string
	docDatas []map[string]interface{},
	docIds []string,
) error {
	bulkWriter := db.Client.BulkWriter(ctx)

	for i, docData := range docDatas {
		var docRef *firestore.DocumentRef

		// Determine collection path
		var collectionPath string
		switch v := collectionPaths.(type) {
		case string:
			collectionPath = v
		case []string:
			collectionPath = v[i]
		default:
			bulkWriter.End()
			return fmt.Errorf("collectionPaths must be string or []string")
		}

		// Create document reference
		if docIds != nil && len(docIds) > i {
			// Use provided document ID
			docRef = db.Client.Collection(collectionPath).Doc(docIds[i])
		} else {
			// Use auto-generated document ID
			docRef = db.Client.Collection(collectionPath).NewDoc()
		}

		// Add to BulkWriter
		_, err := bulkWriter.Set(docRef, docData)
		if err != nil {
			bulkWriter.End()
			fmt.Printf("[ERROR] DatabaseClient.AddDocuments - failed to add document - error: %v, index: %d\n",
				err, i)
			return fmt.Errorf("error adding document at index %d: %w", i, err)
		}
	}

	bulkWriter.End()
	bulkWriter.Flush()

	fmt.Printf("[DEBUG] DatabaseClient.AddDocuments - function completed - collectionPaths: %v, docIds: %v\n",
		collectionPaths, docIds)

	return nil
}

func (db *FirestoreDB) UpdateDocuments(
	ctx context.Context,
	collectionPaths interface{}, // Can be string or []string
	docDatas []map[string]interface{},
	docIds []string,
) error {
	bulkWriter := db.Client.BulkWriter(ctx)
	defer bulkWriter.End()
	for i, docData := range docDatas {
		var docRef *firestore.DocumentRef

		// Determine collection path
		var collectionPath string
		switch v := collectionPaths.(type) {
		case string:
			collectionPath = v
		case []string:
			collectionPath = v[i]
		default:
			bulkWriter.End()
			return fmt.Errorf("collectionPaths must be string or []string")
		}

		// Add to BulkWriter
		docRef = db.Client.Collection(collectionPath).Doc(docIds[i])

		// Set with merge option (equivalent to batch.set(docRef, doc, { merge: true }))
		_, err := bulkWriter.Set(docRef, docData, firestore.MergeAll)
		if err != nil {
			fmt.Printf("[ERROR] DatabaseClient.updateDocuments - function failed - error: %v, collectionPaths: %v, docIds: %v, docDatas: %v\n",
				err, collectionPaths, docIds, docDatas)
			return fmt.Errorf("error in updateDocuments: %w", err)
		}
	}

	bulkWriter.Flush()

	fmt.Printf("[DEBUG] DatabaseClient.updateDocuments - function completed - collectionPaths: %v, docIds: %v\n",
		collectionPaths, docIds)

	return nil
}

func (db *FirestoreDB) GetDocuments(
	ctx context.Context, params GetDocumentsParams,
) (*GetDocumentsResult, error) {
	var totalDocumentsCount *int

	collectionRef := db.Client.Collection(params.CollectionPath)
	query := collectionRef.Query
	for _, condition := range params.Filter {
		query = query.Where(condition.Field, condition.Operator, condition.Value)
	}

	for _, orFilter := range params.OrFilters {
		if len(orFilter.Or) > 0 {
			// Create OR conditions
			filters := make([]firestore.EntityFilter, len(orFilter.Or))
			for i, condition := range orFilter.Or {
				filters[i] = firestore.PropertyFilter{
					Path:     condition.Field,
					Operator: condition.Operator,
					Value:    condition.Value,
				}
			}

			orConditionFilters := firestore.OrFilter{
				Filters: filters,
			}

			query = query.WhereEntity(orConditionFilters)
		}
	}

	for _, orderCond := range params.Order {
		query = query.OrderBy(orderCond.Field, orderCond.Order)
	}

	// Apply field selection
	if len(params.SelectFields) > 0 {
		query = query.Select(params.SelectFields...)
	}

	if params.ReturnTotalDocumentsCount {
		// Use manual counting with Select() for efficiency
		countQuery := query.Select() // Only fetch document IDs
		countIter := countQuery.Documents(ctx)
		count := 0
		for {
			_, err := countIter.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				countIter.Stop()
				fmt.Printf("[ERROR] DatabaseClient.getDocuments - failed to count documents: %v\n", err)
				return nil, fmt.Errorf("failed to count documents: %w", err)
			}
			count++
		}
		countIter.Stop()
		totalDocumentsCount = &count
	}

	// Apply pagination
	if params.StartAfterCursor != nil {
		query = query.StartAfter(params.StartAfterCursor)
	} else if params.Offset > 0 {
		query = query.Offset(params.Offset)
	}

	if params.Limit > 0 {
		query = query.Limit(params.Limit)
	}

	// Execute query
	iter := query.Documents(ctx)
	defer iter.Stop()

	var documents []map[string]interface{}
	for {
		doc, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			fmt.Printf("[ERROR] DatabaseClient.getDocuments - function failed - error: %v, collectionPath: %s\n",
				err, params.CollectionPath)
			return nil, fmt.Errorf("error in getDocuments: %w", err)
		}

		documents = append(documents, doc.Data())
	}

	fmt.Printf("[DEBUG] DatabaseClient.getDocuments - function completed - collectionPath: %s, documentsCount: %d\n",
		params.CollectionPath, len(documents))

	return &GetDocumentsResult{
		Documents:           documents,
		TotalDocumentsCount: totalDocumentsCount,
	}, nil
}
