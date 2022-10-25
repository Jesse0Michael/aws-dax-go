package dax

import (
	"context"
	"reflect"
	"testing"

	"github.com/aws/aws-dax-go/dax/internal/client"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func NewWithInternalClient(c client.DaxAPI) *Dax {
	return &Dax{client: c, config: DefaultConfig()}
}

func TestPaginationQueryPage(t *testing.T) {
	pages, numPages := []map[string]types.AttributeValue{}, 0

	resps := []*dynamodb.QueryOutput{
		{
			LastEvaluatedKey: map[string]types.AttributeValue{"key": &types.AttributeValueMemberS{Value: "key1"}},
			Count:            int32(1),
			Items: []map[string]types.AttributeValue{
				{
					"key": &types.AttributeValueMemberS{Value: "key1"},
				},
			},
		},
		{
			LastEvaluatedKey: map[string]types.AttributeValue{"key": &types.AttributeValueMemberS{Value: "key2"}},
			Count:            int32(1),
			Items: []map[string]types.AttributeValue{
				{
					"key": &types.AttributeValueMemberS{Value: "key2"},
				},
			},
		},
		{
			LastEvaluatedKey: nil,
			Count:            int32(1),
			Items: []map[string]types.AttributeValue{
				{
					"key": &types.AttributeValueMemberS{Value: "key3"},
				},
			},
		},
	}

	stub := client.NewClientStub(nil, resps, nil)
	db := NewWithInternalClient(stub)
	params := &dynamodb.QueryInput{
		Limit:     aws.Int32(1),
		TableName: aws.String("tablename"),
	}
	err := db.QueryPages(context.TODO(), params, func(p *dynamodb.QueryOutput) bool {
		numPages++
		pages = append(pages, p.Items...)
		return true
	})

	// There was no error
	if err != nil {
		t.Errorf("expect nil, %v", err)
	}

	// The correct items were returned
	if e, a :=
		[]map[string]types.AttributeValue{
			{"key": &types.AttributeValueMemberS{Value: "key1"}},
			{"key": &types.AttributeValueMemberS{Value: "key2"}},
			{"key": &types.AttributeValueMemberS{Value: "key3"}},
		}, pages; !reflect.DeepEqual(e, a) {
		t.Errorf("expect %v, got %v", e, a)
	}

	// Items were returned in the correct number of pages
	if e, a := 3, numPages; e != a {
		t.Errorf("expect %v, got %v", e, a)
	}

	// Each request had the correct start key
	if a := stub.GetQueryRequests()[0].ExclusiveStartKey; a != nil {
		t.Errorf("expect nil, %v", a)
	}
	for i, e := range []string{"key1", "key2"} {
		if a := stub.GetQueryRequests()[i+1].ExclusiveStartKey["key"].(*types.AttributeValueMemberS).Value; e != a {
			t.Errorf("expect %s, got %s at index %d", e, a, i+1)
		}
	}
}

func TestPaginationScanPage(t *testing.T) {
	pages, numPages := []map[string]types.AttributeValue{}, 0

	resps := []*dynamodb.ScanOutput{
		{
			LastEvaluatedKey: map[string]types.AttributeValue{"key": &types.AttributeValueMemberS{Value: "key1"}},
			Count:            int32(1),
			Items: []map[string]types.AttributeValue{
				{
					"key": &types.AttributeValueMemberS{Value: "key1"},
				},
			},
		},
		{
			LastEvaluatedKey: map[string]types.AttributeValue{"key": &types.AttributeValueMemberS{Value: "key2"}},
			Count:            int32(1),
			Items: []map[string]types.AttributeValue{
				{
					"key": &types.AttributeValueMemberS{Value: "key2"},
				},
			},
		},
		{
			LastEvaluatedKey: nil,
			Count:            int32(1),
			Items: []map[string]types.AttributeValue{
				{
					"key": &types.AttributeValueMemberS{Value: "key3"},
				},
			},
		},
	}

	stub := client.NewClientStub(nil, nil, resps)
	db := NewWithInternalClient(stub)
	params := &dynamodb.ScanInput{
		Limit:     aws.Int32(1),
		TableName: aws.String("tablename"),
	}
	err := db.ScanPages(context.TODO(), params, func(p *dynamodb.ScanOutput) bool {
		numPages++
		pages = append(pages, p.Items...)
		return true
	})

	// There was no error
	if err != nil {
		t.Errorf("expect nil, %v", err)
	}

	// The correct items were returned
	if e, a :=
		[]map[string]types.AttributeValue{
			{"key": &types.AttributeValueMemberS{Value: "key1"}},
			{"key": &types.AttributeValueMemberS{Value: "key2"}},
			{"key": &types.AttributeValueMemberS{Value: "key3"}},
		}, pages; !reflect.DeepEqual(e, a) {
		t.Errorf("expect %v, got %v", e, a)
	}

	// Items were returned in the correct number of pages
	if e, a := 3, numPages; e != a {
		t.Errorf("expect %v, got %v", e, a)
	}

	// Each request had the correct start key
	if a := stub.GetScanRequests()[0].ExclusiveStartKey; a != nil {
		t.Errorf("expect nil, %v", a)
	}
	for i, e := range []string{"key1", "key2"} {
		if a := stub.GetScanRequests()[i+1].ExclusiveStartKey["key"].(*types.AttributeValueMemberS).Value; e != a {
			t.Errorf("expect %s, got %s at index %d", e, a, i+1)
		}
	}
}
