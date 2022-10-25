/*
  Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.

  Licensed under the Apache License, Version 2.0 (the "License").
  You may not use this file except in compliance with the License.
  A copy of the License is located at

      http://www.apache.org/licenses/LICENSE-2.0

  or in the "license" file accompanying this file. This file is distributed
  on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
  express or implied. See the License for the specific language governing
  permissions and limitations under the License.
*/

package dax

import (
	"context"
	"errors"
	"io"

	"github.com/aws/aws-dax-go/dax/internal/client"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go/aws/request"
)

func (d *Dax) PutItem(ctx context.Context, input *dynamodb.PutItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
	o, cfn, err := d.config.requestOptions(false, ctx, opts...)
	if err != nil {
		return nil, err
	}
	if cfn != nil {
		defer cfn()
	}
	return d.client.PutItemWithOptions(input, &dynamodb.PutItemOutput{}, o)
}

func (d *Dax) DeleteItem(ctx context.Context, input *dynamodb.DeleteItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error) {
	o, cfn, err := d.config.requestOptions(false, ctx, opts...)
	if err != nil {
		return nil, err
	}
	if cfn != nil {
		defer cfn()
	}
	return d.client.DeleteItemWithOptions(input, &dynamodb.DeleteItemOutput{}, o)
}

func (d *Dax) UpdateItem(ctx context.Context, input *dynamodb.UpdateItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error) {
	o, cfn, err := d.config.requestOptions(false, ctx, opts...)
	if err != nil {
		return nil, err
	}
	if cfn != nil {
		defer cfn()
	}
	return d.client.UpdateItemWithOptions(input, &dynamodb.UpdateItemOutput{}, o)
}

func (d *Dax) GetItem(ctx context.Context, input *dynamodb.GetItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
	o, cfn, err := d.config.requestOptions(true, ctx, opts...)
	if err != nil {
		return nil, err
	}
	if cfn != nil {
		defer cfn()
	}
	return d.client.GetItemWithOptions(input, &dynamodb.GetItemOutput{}, o)
}

func (d *Dax) Scan(ctx context.Context, input *dynamodb.ScanInput, opts ...func(*dynamodb.Options)) (*dynamodb.ScanOutput, error) {
	o, cfn, err := d.config.requestOptions(true, ctx, opts...)
	if err != nil {
		return nil, err
	}
	if cfn != nil {
		defer cfn()
	}
	return d.client.ScanWithOptions(input, &dynamodb.ScanOutput{}, o)
}

func (d *Dax) Query(ctx context.Context, input *dynamodb.QueryInput, opts ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
	o, cfn, err := d.config.requestOptions(true, ctx, opts...)
	if err != nil {
		return nil, err
	}
	if cfn != nil {
		defer cfn()
	}
	return d.client.QueryWithOptions(input, &dynamodb.QueryOutput{}, o)
}

func (d *Dax) BatchWriteItem(ctx context.Context, input *dynamodb.BatchWriteItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.BatchWriteItemOutput, error) {
	o, cfn, err := d.config.requestOptions(false, ctx, opts...)
	if err != nil {
		return nil, err
	}
	if cfn != nil {
		defer cfn()
	}
	return d.client.BatchWriteItemWithOptions(input, &dynamodb.BatchWriteItemOutput{}, o)
}

func (d *Dax) BatchGetItem(ctx context.Context, input *dynamodb.BatchGetItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.BatchGetItemOutput, error) {
	o, cfn, err := d.config.requestOptions(true, ctx, opts...)
	if err != nil {
		return nil, err
	}
	if cfn != nil {
		defer cfn()
	}
	return d.client.BatchGetItemWithOptions(input, &dynamodb.BatchGetItemOutput{}, o)
}

func (d *Dax) TransactWriteItems(ctx context.Context, input *dynamodb.TransactWriteItemsInput, opts ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error) {
	o, cfn, err := d.config.requestOptions(false, ctx, opts...)
	if err != nil {
		return nil, err
	}
	if cfn != nil {
		defer cfn()
	}
	return d.client.TransactWriteItemsWithOptions(input, &dynamodb.TransactWriteItemsOutput{}, o)
}

func (d *Dax) TransactGetItems(ctx context.Context, input *dynamodb.TransactGetItemsInput, opts ...func(*dynamodb.Options)) (*dynamodb.TransactGetItemsOutput, error) {
	o, cfn, err := d.config.requestOptions(true, ctx, opts...)
	if err != nil {
		return nil, err
	}
	if cfn != nil {
		defer cfn()
	}
	return d.client.TransactGetItemsWithOptions(input, &dynamodb.TransactGetItemsOutput{}, o)
}

func (d *Dax) QueryPages(ctx context.Context, input *dynamodb.QueryInput, fn func(*dynamodb.QueryOutput) bool, opts ...func(*dynamodb.Options)) error {
	p := dynamodb.NewQueryPaginator(d, input)
	for p.HasMorePages() {
		output, err := p.NextPage(ctx, opts...)
		if err != nil {
			return err
		}
		if !fn(output) {
			break
		}
	}
	return nil
}

func (d *Dax) ScanPages(ctx context.Context, input *dynamodb.ScanInput, fn func(*dynamodb.ScanOutput) bool, opts ...func(*dynamodb.Options)) error {
	p := dynamodb.NewScanPaginator(d, input)
	for p.HasMorePages() {
		output, err := p.NextPage(ctx, opts...)
		if err != nil {
			return err
		}
		if !fn(output) {
			break
		}
	}
	return nil
}

func (d *Dax) CreateBackup(context.Context, *dynamodb.CreateBackupInput, ...func(*dynamodb.Options)) (*dynamodb.CreateBackupOutput, error) {
	return nil, d.unImpl()
}

func (d *Dax) CreateGlobalTable(context.Context, *dynamodb.CreateGlobalTableInput, ...func(*dynamodb.Options)) (*dynamodb.CreateGlobalTableOutput, error) {
	return nil, d.unImpl()
}

func (d *Dax) CreateTable(context.Context, *dynamodb.CreateTableInput, ...func(*dynamodb.Options)) (*dynamodb.CreateTableOutput, error) {
	return nil, d.unImpl()
}

func (d *Dax) DeleteBackup(context.Context, *dynamodb.DeleteBackupInput, ...func(*dynamodb.Options)) (*dynamodb.DeleteBackupOutput, error) {
	return nil, d.unImpl()
}

func (d *Dax) DeleteTable(context.Context, *dynamodb.DeleteTableInput, ...func(*dynamodb.Options)) (*dynamodb.DeleteTableOutput, error) {
	return nil, d.unImpl()
}

func (d *Dax) DescribeBackup(context.Context, *dynamodb.DescribeBackupInput, ...func(*dynamodb.Options)) (*dynamodb.DescribeBackupOutput, error) {
	return nil, d.unImpl()
}

func (d *Dax) DescribeContinuousBackups(context.Context, *dynamodb.DescribeContinuousBackupsInput, ...func(*dynamodb.Options)) (*dynamodb.DescribeContinuousBackupsOutput, error) {
	return nil, d.unImpl()
}

func (d *Dax) DescribeContributorInsights(context.Context, *dynamodb.DescribeContributorInsightsInput, ...func(*dynamodb.Options)) (*dynamodb.DescribeContributorInsightsOutput, error) {
	return nil, d.unImpl()
}

func (d *Dax) DescribeEndpoints(context.Context, *dynamodb.DescribeEndpointsInput, ...func(*dynamodb.Options)) (*dynamodb.DescribeEndpointsOutput, error) {
	return nil, d.unImpl()
}

func (d *Dax) DescribeGlobalTable(context.Context, *dynamodb.DescribeGlobalTableInput, ...func(*dynamodb.Options)) (*dynamodb.DescribeGlobalTableOutput, error) {
	return nil, d.unImpl()
}

func (d *Dax) DescribeGlobalTableSettings(context.Context, *dynamodb.DescribeGlobalTableSettingsInput, ...func(*dynamodb.Options)) (*dynamodb.DescribeGlobalTableSettingsOutput, error) {
	return nil, d.unImpl()
}

func (d *Dax) DescribeLimits(context.Context, *dynamodb.DescribeLimitsInput, ...func(*dynamodb.Options)) (*dynamodb.DescribeLimitsOutput, error) {
	return nil, d.unImpl()
}

func (d *Dax) DescribeTable(context.Context, *dynamodb.DescribeTableInput, ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error) {
	return nil, d.unImpl()
}

func (d *Dax) DescribeTableReplicaAutoScaling(context.Context, *dynamodb.DescribeTableReplicaAutoScalingInput, ...func(*dynamodb.Options)) (*dynamodb.DescribeTableReplicaAutoScalingOutput, error) {
	return nil, d.unImpl()
}

func (d *Dax) DescribeTimeToLive(context.Context, *dynamodb.DescribeTimeToLiveInput, ...func(*dynamodb.Options)) (*dynamodb.DescribeTimeToLiveOutput, error) {
	return nil, d.unImpl()
}

func (d *Dax) BatchExecuteStatement(context.Context, *dynamodb.BatchExecuteStatementInput, ...func(*dynamodb.Options)) (*dynamodb.BatchExecuteStatementOutput, error) {
	return nil, d.unImpl()
}

func (d *Dax) DescribeExport(context.Context, *dynamodb.DescribeExportInput, ...func(*dynamodb.Options)) (*dynamodb.DescribeExportOutput, error) {
	return nil, d.unImpl()
}

func (d *Dax) DescribeKinesisStreamingDestination(context.Context, *dynamodb.DescribeKinesisStreamingDestinationInput, ...func(*dynamodb.Options)) (*dynamodb.DescribeKinesisStreamingDestinationOutput, error) {
	return nil, d.unImpl()
}

func (d *Dax) DisableKinesisStreamingDestination(context.Context, *dynamodb.DisableKinesisStreamingDestinationInput, ...func(*dynamodb.Options)) (*dynamodb.DisableKinesisStreamingDestinationOutput, error) {
	return nil, d.unImpl()
}

func (d *Dax) EnableKinesisStreamingDestination(context.Context, *dynamodb.EnableKinesisStreamingDestinationInput, ...func(*dynamodb.Options)) (*dynamodb.EnableKinesisStreamingDestinationOutput, error) {
	return nil, d.unImpl()
}

func (d *Dax) ExecuteStatement(context.Context, *dynamodb.ExecuteStatementInput, ...func(*dynamodb.Options)) (*dynamodb.ExecuteStatementOutput, error) {
	return nil, d.unImpl()
}

func (d *Dax) ExecuteTransaction(context.Context, *dynamodb.ExecuteTransactionInput, ...func(*dynamodb.Options)) (*dynamodb.ExecuteTransactionOutput, error) {
	return nil, d.unImpl()
}

func (d *Dax) ExportTableToPointInTime(context.Context, *dynamodb.ExportTableToPointInTimeInput, ...func(*dynamodb.Options)) (*dynamodb.ExportTableToPointInTimeOutput, error) {
	return nil, d.unImpl()
}

func (d *Dax) ListBackups(context.Context, *dynamodb.ListBackupsInput, ...func(*dynamodb.Options)) (*dynamodb.ListBackupsOutput, error) {
	return nil, d.unImpl()
}

func (d *Dax) ListContributorInsights(context.Context, *dynamodb.ListContributorInsightsInput, ...func(*dynamodb.Options)) (*dynamodb.ListContributorInsightsOutput, error) {
	return nil, d.unImpl()
}

func (d *Dax) ListContributorInsightsPages(context.Context, *dynamodb.ListContributorInsightsInput, func(*dynamodb.ListContributorInsightsOutput, bool) bool, ...func(*dynamodb.Options)) error {
	return d.unImpl()
}

func (d *Dax) ListExports(context.Context, *dynamodb.ListExportsInput, ...func(*dynamodb.Options)) (*dynamodb.ListExportsOutput, error) {
	return nil, d.unImpl()
}

func (d *Dax) ListExportsPages(context.Context, *dynamodb.ListExportsInput, func(*dynamodb.ListExportsOutput, bool) bool, ...func(*dynamodb.Options)) error {
	return d.unImpl()
}

func (d *Dax) ListGlobalTables(context.Context, *dynamodb.ListGlobalTablesInput, ...func(*dynamodb.Options)) (*dynamodb.ListGlobalTablesOutput, error) {
	return nil, d.unImpl()
}

func (d *Dax) ListTables(context.Context, *dynamodb.ListTablesInput, ...func(*dynamodb.Options)) (*dynamodb.ListTablesOutput, error) {
	return nil, d.unImpl()
}

func (d *Dax) ListTablesPages(context.Context, *dynamodb.ListTablesInput, func(*dynamodb.ListTablesOutput, bool) bool, ...func(*dynamodb.Options)) error {
	return d.unImpl()
}

func (d *Dax) ListTagsOfResource(context.Context, *dynamodb.ListTagsOfResourceInput, ...func(*dynamodb.Options)) (*dynamodb.ListTagsOfResourceOutput, error) {
	return nil, d.unImpl()
}

func (d *Dax) RestoreTableFromBackup(context.Context, *dynamodb.RestoreTableFromBackupInput, ...func(*dynamodb.Options)) (*dynamodb.RestoreTableFromBackupOutput, error) {
	return nil, d.unImpl()
}

func (d *Dax) RestoreTableToPointInTime(context.Context, *dynamodb.RestoreTableToPointInTimeInput, ...func(*dynamodb.Options)) (*dynamodb.RestoreTableToPointInTimeOutput, error) {
	return nil, d.unImpl()
}

func (d *Dax) TagResource(context.Context, *dynamodb.TagResourceInput, ...func(*dynamodb.Options)) (*dynamodb.TagResourceOutput, error) {
	return nil, d.unImpl()
}

func (d *Dax) UntagResource(context.Context, *dynamodb.UntagResourceInput, ...func(*dynamodb.Options)) (*dynamodb.UntagResourceOutput, error) {
	return nil, d.unImpl()
}
func (d *Dax) UpdateContinuousBackups(context.Context, *dynamodb.UpdateContinuousBackupsInput, ...func(*dynamodb.Options)) (*dynamodb.UpdateContinuousBackupsOutput, error) {
	return nil, d.unImpl()
}

func (d *Dax) UpdateContributorInsights(context.Context, *dynamodb.UpdateContributorInsightsInput, ...func(*dynamodb.Options)) (*dynamodb.UpdateContributorInsightsOutput, error) {
	return nil, d.unImpl()
}

func (d *Dax) UpdateGlobalTable(context.Context, *dynamodb.UpdateGlobalTableInput, ...func(*dynamodb.Options)) (*dynamodb.UpdateGlobalTableOutput, error) {
	return nil, d.unImpl()
}

func (d *Dax) UpdateGlobalTableSettings(context.Context, *dynamodb.UpdateGlobalTableSettingsInput, ...func(*dynamodb.Options)) (*dynamodb.UpdateGlobalTableSettingsOutput, error) {
	return nil, d.unImpl()
}

func (d *Dax) UpdateTable(context.Context, *dynamodb.UpdateTableInput, ...func(*dynamodb.Options)) (*dynamodb.UpdateTableOutput, error) {
	return nil, d.unImpl()
}

func (d *Dax) UpdateTableReplicaAutoScaling(context.Context, *dynamodb.UpdateTableReplicaAutoScalingInput, ...func(*dynamodb.Options)) (*dynamodb.UpdateTableReplicaAutoScalingOutput, error) {
	return nil, d.unImpl()
}

func (d *Dax) UpdateTimeToLive(context.Context, *dynamodb.UpdateTimeToLiveInput, ...func(*dynamodb.Options)) (*dynamodb.UpdateTimeToLiveOutput, error) {
	return nil, d.unImpl()
}

func (d *Dax) WaitUntilTableExists(context.Context, *dynamodb.DescribeTableInput, ...request.WaiterOption) error {
	return d.unImpl()
}

func (d *Dax) unImpl() error {
	return errors.New(client.ErrCodeNotImplemented)
}

func (d *Dax) Close() error {
	if c, ok := d.client.(io.Closer); ok {
		return c.Close()
	}
	return nil
}
