// Package dynamo contains controls and objects for DynamoDB CRUD operations.
// Operations in this package are abstracted from all other application logic
// and are designed to be used with any DynamoDB table and any object schema.
// This file contains CRUD operations for working with DynamoDB.
package dynamo

import (
	"fmt"

	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"

	"github.com/aws/aws-sdk-go/aws"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

// InitSesh initializes a new session with default config/credentials.
func InitSesh() *dynamodb.DynamoDB {
	// Initialize a session that the SDK will use to load
	// credentials from the shared credentials file ~/.aws/credentials
	sesh := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))

	fmt.Println("session intialized")
	fmt.Println("region: ", aws.StringValue(sesh.Config.Region))

	// Create DynamoDB client
	svc := dynamodb.New(sesh)

	fmt.Println("DynamoDB client initialized")
	fmt.Println()

	return svc
}

// ListTables lists the tables in the database.
func ListTables(svc *dynamodb.DynamoDB) ([]string, int, error) {
	names := []string{}
	t := 0
	input := &dynamodb.ListTablesInput{}
	fmt.Println("Tables:")

	for {
		// Get the list of tables
		result, err := svc.ListTables(input)
		if err != nil {
			if aerr, ok := err.(awserr.Error); ok {
				switch aerr.Code() {
				case dynamodb.ErrCodeInternalServerError:
					fmt.Println(dynamodb.ErrCodeInternalServerError, aerr.Error())
				default:
					fmt.Println(aerr.Error())
				}
			} else {
				// Print the error, cast err to awserr.Error to get the Code
				// and Message from the error
				fmt.Println(err.Error())
			}
			return nil, 0, fmt.Errorf("ListTables failed: %v", err)
		}

		for _, n := range result.TableNames {
			fmt.Println(*n)
			names = append(names, *n)
			t++
		}

		// assign the last read tablename as the start for our next call to the ListTables function
		// the maximum number of table names returned in a call is 100 (default), which requires us to make
		// multiple calls to the ListTables function to retrieve all table names
		input.ExclusiveStartTableName = result.LastEvaluatedTableName

		if result.LastEvaluatedTableName == nil {
			break
		}
	}
	return names, t, nil
}

// CreateTable creates a new table with the parameters passed to the Table struct.
// NOTE: CreateTable creates Table in * On-Demand * billing mode.
func CreateTable(svc *dynamodb.DynamoDB, table *Table) error {
	input := &dynamodb.CreateTableInput{
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{ // Primary Key
				AttributeName: aws.String(table.PrimaryKeyName),
				AttributeType: aws.String(table.PrimaryKeyType),
			},
			{
				AttributeName: aws.String(table.SortKeyName),
				AttributeType: aws.String(table.SortKeyType),
			},
		},
		BillingMode: aws.String("PAY_PER_REQUEST"),
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: aws.String(table.PrimaryKeyName),
				KeyType:       aws.String("HASH"),
			},
			{
				AttributeName: aws.String(table.SortKeyName),
				KeyType:       aws.String("RANGE"),
			},
		},
		TableName: aws.String(table.TableName),
	}

	_, err := svc.CreateTable(input)
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			if awsErr.Code() == "ResourceInUseException" {
				return fmt.Errorf(awsErr.Code())
			}
			fmt.Println("Got error calling CreateTable:")
			// Get error details
			fmt.Println("CreateTable failed:", awsErr.Code(), awsErr.Message())
		} else {
			fmt.Println(err.Error())
			return fmt.Errorf("CreateTable failed: %v", err)
		}
	}

	fmt.Println("Created the table: ", table.TableName)
	return nil
}

// CreateItem puts a new item in the table.
func CreateItem(svc *dynamodb.DynamoDB, item interface{}, table *Table) error {
	av, err := dynamodbattribute.MarshalMap(item)
	if err != nil {
		fmt.Println("Got error marshalling new movie item: ")
		fmt.Println(err.Error())
		return fmt.Errorf("CreateItem failed: %v", err)
	}

	input := &dynamodb.PutItemInput{
		Item:      av,
		TableName: aws.String(table.TableName),
	}

	_, err = svc.PutItem(input)
	if err != nil {
		fmt.Println("Got error calling PutItem:")
		fmt.Println(err.Error())
		return fmt.Errorf("CreateItem failed: %v", err)
	}

	fmt.Printf("Successfully added item to table %s\n", table.TableName)
	return nil
}

// GetItem reads an item from the database.
// Returns Attribute Value map interface (map[stirng]interface{}) if object found.
// Returns interface of type item if object not found.
func GetItem(svc *dynamodb.DynamoDB, q *Query, t *Table, item interface{}) (interface{}, error) {
	key := keyMaker(q, t)
	result, err := svc.GetItem(&dynamodb.GetItemInput{
		TableName: aws.String(t.TableName),
		Key:       key,
	})
	if err != nil {
		fmt.Println(err.Error())
		return nil, fmt.Errorf("GetItem failed: %v", err)
	}

	err = dynamodbattribute.UnmarshalMap(result.Item, &item)
	if err != nil {
		fmt.Printf("Failed to unmarshal record, %v\n", err)
		return nil, fmt.Errorf("GetItem failed: Failed to unmarshal record, %v", err)
	}

	return item, nil
}

// UpdateItem updates the specified item's attribute defined in the
// Query object with the UpdateValue defined in the Query.
func UpdateItem(svc *dynamodb.DynamoDB, q *Query, t *Table) error {
	exprMap := make(map[string]*dynamodb.AttributeValue)
	exprMap[":u"] = createAV(q.UpdateValue)
	input := &dynamodb.UpdateItemInput{
		ExpressionAttributeValues: exprMap,
		TableName:                 aws.String(t.TableName),
		Key:                       keyMaker(q, t),
		ReturnValues:              aws.String("UPDATED_NEW"),
		UpdateExpression:          aws.String(fmt.Sprintf("set %s = :u", q.UpdateFieldName)),
	}

	_, err := svc.UpdateItem(input)
	if err != nil {
		fmt.Println(err.Error())
		return fmt.Errorf("UpdateItem failed: %v", err)
	}

	fmt.Printf("Updated %v: %v: %s = %v\n", q.PrimaryValue, q.SortValue, q.UpdateFieldName, q.UpdateValue)
	return nil
}

// DeleteTable deletes the selected table.
func DeleteTable(svc *dynamodb.DynamoDB, t *Table) error {
	input := &dynamodb.DeleteTableInput{
		TableName: aws.String(t.TableName),
	}
	_, err := svc.DeleteTable(input)
	if err != nil {
		fmt.Println(err.Error())
		return fmt.Errorf("DeleteTable failed: %v", err)
	}
	fmt.Println("Deleted Table: ", t.TableName)
	return nil
}

// DeleteItem deletes the specified item defined in the Query
func DeleteItem(svc *dynamodb.DynamoDB, q *Query, t *Table) error {
	input := &dynamodb.DeleteItemInput{
		Key:       keyMaker(q, t),
		TableName: aws.String(t.TableName),
	}

	_, err := svc.DeleteItem(input)
	if err != nil {
		fmt.Println("Got error calling DeleteItem")
		fmt.Println(err.Error())
		return fmt.Errorf("DeleteItem failed: %v", err)
	}

	fmt.Printf("Deleted %s: %s from table %s\n", q.PrimaryValue, q.SortValue, t.TableName)
	return nil
}

// BatchWriteCreate writes a list of items to the database.
func BatchWriteCreate(svc *dynamodb.DynamoDB, t *Table, fc *FailConfig, items []interface{}) error {
	if len(items) > 25 {
		return fmt.Errorf("too many items to process")
	}

	// create map of RequestItems
	reqItems := make(map[string][]*dynamodb.WriteRequest)
	wrs := []*dynamodb.WriteRequest{}

	// create PutRequests for each item
	for _, item := range items {
		if item == nil {
			fmt.Println("nil item")
			continue
		}

		// marshal each item
		av, err := dynamodbattribute.MarshalMap(item)
		if err != nil {
			fmt.Println("*** err item: ", item)
			return fmt.Errorf("BatchWriteCreate failed: %v", err)
		}
		// create put request, reformat as write request, and add to list
		pr := &dynamodb.PutRequest{Item: av}
		wr := &dynamodb.WriteRequest{PutRequest: pr}
		wrs = append(wrs, wr)
	}
	// populate reqItems map
	reqItems[t.TableName] = wrs

	// generate input from reqItems map
	input := &dynamodb.BatchWriteItemInput{
		RequestItems: reqItems,
	}

	// batch write and error handling with exponential backoff retries for HTTP 5xx errors
	var result *dynamodb.BatchWriteItemOutput
	var err error
	for {
		result, err = batchWriteUtil(svc, input)
		if err != nil {
			// if not HTTP 5xx error
			if err.(awserr.Error).Code() != dynamodb.ErrCodeInternalServerError {
				fmt.Printf("unprocessed items: \n%v\n", result.UnprocessedItems)
				// return fmt.Errorf("BatchWriteCreate failed: %v", err)
				return err
			}

			// Retry with exponential backoff algorithm
			if err.(awserr.Error).Code() == dynamodb.ErrCodeInternalServerError && result.UnprocessedItems != nil {
				fmt.Printf("unprocessed items: \n%v\n", result.UnprocessedItems)
				input = &dynamodb.BatchWriteItemInput{
					RequestItems: result.UnprocessedItems,
				}
				fmt.Println("retrying...")
				fc.ExponentialBackoff() // waits
				if fc.MaxRetriesReached == true {
					return fmt.Errorf("BatchWriteCreate failed: Max retries exceeded: %v", err)
				}
			}
		}

		if len(result.UnprocessedItems) == 0 {
			fc.Reset() // reset configuration after loop
			break
		}

	}

	return nil
}

// BatchWriteDelete deletes a list of items from the database.
func BatchWriteDelete(svc *dynamodb.DynamoDB, t *Table, fc *FailConfig, queries []*Query) error {
	if len(queries) > 25 {
		return fmt.Errorf("too many items to process")
	}

	// create map of RequestItems
	reqItems := make(map[string][]*dynamodb.WriteRequest)
	wrs := []*dynamodb.WriteRequest{}

	// create PutRequests for each item
	for _, q := range queries {
		if q == nil {
			continue
		}

		// create put request, reformat as write request, and add to list
		dr := &dynamodb.DeleteRequest{Key: keyMaker(q, t)}
		wr := &dynamodb.WriteRequest{DeleteRequest: dr}
		wrs = append(wrs, wr)
	}
	// populate reqItems map
	reqItems[t.TableName] = wrs

	// generate input from reqItems map
	input := &dynamodb.BatchWriteItemInput{
		RequestItems: reqItems,
	}

	// batch write and error handling with exponential backoff retries for HTTP 5xx errors
	var result *dynamodb.BatchWriteItemOutput
	var err error
	for {
		result, err = batchWriteUtil(svc, input)
		if err != nil {
			// if not HTTP 5xx error
			if err.(awserr.Error).Code() != dynamodb.ErrCodeInternalServerError {
				fmt.Printf("unprocessed items: \n%v\n", result.UnprocessedItems)
				return fmt.Errorf("BatchWriteDelete failed: %v", err)
			}

			// Retry with exponential backoff algorithm
			if err.(awserr.Error).Code() == dynamodb.ErrCodeInternalServerError && result.UnprocessedItems != nil {
				fmt.Printf("unprocessed items: \n%v\n", result.UnprocessedItems)
				input = &dynamodb.BatchWriteItemInput{
					RequestItems: result.UnprocessedItems,
				}
				fc.ExponentialBackoff() // waits
				if fc.MaxRetriesReached == true {
					return fmt.Errorf("BatchWriteDelete failed: Max retries exceeded: %v", err)
				}
			}
		}

		if len(result.UnprocessedItems) == 0 {
			fc.Reset() // reset configuration after loop
			break
		}

	}

	return nil
}

// BatchGet retrieves a list of items from the database
// refObjs must be non-nil pointers of the same type,
// 1 for each query/object returned.
//   - Returns err if len(queries) != len(refObjs).
func BatchGet(svc *dynamodb.DynamoDB, t *Table, fc *FailConfig, queries []*Query, refObjs []interface{}) ([]interface{}, error) {
	if len(queries) > 100 {
		return nil, fmt.Errorf("too many items to process")
	}

	if len(queries) != len(refObjs) {
		return nil, fmt.Errorf("number of queries does not match number of reference objects")
	}

	items := []interface{}{}

	// create map of RequestItems
	reqItems := make(map[string]*dynamodb.KeysAndAttributes)
	keys := []map[string]*dynamodb.AttributeValue{}

	// create Get requests for each query
	for _, q := range queries {
		if q == nil {
			continue
		}

		item := keyMaker(q, t)
		keys = append(keys, item)
	}
	// populate reqItems map
	ka := &dynamodb.KeysAndAttributes{Keys: keys}
	reqItems[t.TableName] = ka

	// generate input from reqItems map
	input := &dynamodb.BatchGetItemInput{
		RequestItems: reqItems,
	}

	// batch write and error handling with exponential backoff retries for HTTP 5xx errors
	var result *dynamodb.BatchGetItemOutput
	var err error
	for {
		result, err = batchGetUtil(svc, input)
		if err != nil {
			// if not HTTP 5xx error
			if err.(awserr.Error).Code() != dynamodb.ErrCodeInternalServerError {
				fmt.Printf("unprocessed items: \n%v\n", result.UnprocessedKeys)
				return nil, fmt.Errorf("BatchGet failed: %v", err)
			}
			if err.(awserr.Error).Code() == "ValidationException" {
				fmt.Printf("unprocessed items: \n%v\n", result.UnprocessedKeys)
				return nil, err
			}
			if err.(awserr.Error).Code() == "RequestError" {
				fmt.Printf("unprocessed items: \n%v\n", result.UnprocessedKeys)
				return nil, err
			}

			// Retry with exponential backoff algorithm
			if err.(awserr.Error).Code() == dynamodb.ErrCodeInternalServerError && result.UnprocessedKeys != nil {
				fmt.Printf("unprocessed items: \n%v\n", result.UnprocessedKeys)
				input = &dynamodb.BatchGetItemInput{
					RequestItems: result.UnprocessedKeys,
				}
				fc.ExponentialBackoff() // waits
				if fc.MaxRetriesReached == true {
					return nil, fmt.Errorf("BatchGet failed: Max retries exceeded: %v", err)
				}
			}
		}

		for i, r := range result.Responses[t.TableName] {
			ref := refObjs[i]
			err = dynamodbattribute.UnmarshalMap(r, &ref)
			if err != nil {
				fmt.Printf("Failed to unmarshal record, %v\n", err)
				return nil, fmt.Errorf("BatchGet failed: Failed to unmarshal record, %v", err)
			}
			items = append(items, ref)
		}

		if len(result.UnprocessedKeys) == 0 {
			fc.Reset() // reset configuration after loop
			break
		}

	}

	return items, nil
}

func batchWriteUtil(svc *dynamodb.DynamoDB, input *dynamodb.BatchWriteItemInput) (*dynamodb.BatchWriteItemOutput, error) {
	result, err := svc.BatchWriteItem(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case dynamodb.ErrCodeProvisionedThroughputExceededException:
				fmt.Println(dynamodb.ErrCodeProvisionedThroughputExceededException, aerr.Error())
			case dynamodb.ErrCodeResourceNotFoundException:
				fmt.Println(dynamodb.ErrCodeResourceNotFoundException, aerr.Error())
			case dynamodb.ErrCodeItemCollectionSizeLimitExceededException:
				fmt.Println(dynamodb.ErrCodeItemCollectionSizeLimitExceededException, aerr.Error())
			case dynamodb.ErrCodeRequestLimitExceeded:
				fmt.Println(dynamodb.ErrCodeRequestLimitExceeded, aerr.Error())
			case dynamodb.ErrCodeInternalServerError:
				fmt.Println(dynamodb.ErrCodeInternalServerError, aerr.Error())
			default:
				fmt.Println(aerr.Error())
			}
		} else {
			// Print the error, cast err to awserr.Error to get the Code and
			// Message from an error.
			fmt.Println(err.Error())
		}
	}
	return result, err
}

func batchGetUtil(svc *dynamodb.DynamoDB, input *dynamodb.BatchGetItemInput) (*dynamodb.BatchGetItemOutput, error) {
	result, err := svc.BatchGetItem(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case dynamodb.ErrCodeProvisionedThroughputExceededException:
				fmt.Println(dynamodb.ErrCodeProvisionedThroughputExceededException, aerr.Error())
			case dynamodb.ErrCodeResourceNotFoundException:
				fmt.Println(dynamodb.ErrCodeResourceNotFoundException, aerr.Error())
			case dynamodb.ErrCodeItemCollectionSizeLimitExceededException:
				fmt.Println(dynamodb.ErrCodeItemCollectionSizeLimitExceededException, aerr.Error())
			case dynamodb.ErrCodeRequestLimitExceeded:
				fmt.Println(dynamodb.ErrCodeRequestLimitExceeded, aerr.Error())
			case dynamodb.ErrCodeInternalServerError:
				fmt.Println(dynamodb.ErrCodeInternalServerError, aerr.Error())
			default:
				fmt.Println(aerr.Error())
			}
		} else {
			// Print the error, cast err to awserr.Error to get the Code and
			// Message from an error.
			fmt.Println(err.Error())
		}
	}
	return result, err
}
