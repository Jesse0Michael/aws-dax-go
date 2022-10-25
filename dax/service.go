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
	"crypto/tls"
	"net"
	"net/url"
	"os"
	"time"

	"github.com/aws/aws-dax-go/dax/internal/client"
	"github.com/aws/aws-dax-go/dax/internal/proxy"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/smithy-go/logging"
)

// Dax makes requests to the Amazon DAX API, which conforms to the DynamoDB API.
//
// Dax methods are safe to use concurrently
type Dax struct {
	client client.DaxAPI
	config Config
}

const ServiceName = "dax"

type Config struct {
	client.Config

	// Default request options
	RequestTimeout time.Duration
	WriteRetries   int
	ReadRetries    int

	// LogLevel aws.LogLevelType
	Logger logging.Logger
}

// DefaultConfig returns the default DAX configuration.
//
// Config.Region and Config.HostPorts still need to be configured properly
// to start up a DAX client.
func DefaultConfig() Config {
	return Config{
		Config:         client.DefaultConfig(),
		RequestTimeout: 1 * time.Minute,
		WriteRetries:   2,
		ReadRetries:    2,
		// LogLevel:       aws.LogOff,
		Logger: logging.NewStandardLogger(os.Stdout),
	}
}

// New creates a new instance of the DAX client with a DAX configuration.
func New(cfg Config) (*Dax, error) {
	cfg.Config.SetLogger(cfg.Logger)
	c, err := client.New(cfg.Config)
	if err != nil {
		if cfg.Logger != nil {
			cfg.Logger.Logf(logging.Warn, "ERROR: Exception in initialisation of DAX Client : %s", err)
		}
		return nil, err
	}
	return &Dax{client: c, config: cfg}, nil
}

// SecureDialContext creates a secure DialContext for connecting to encrypted cluster
func SecureDialContext(endpoint string, skipHostnameVerification bool) (func(ctx context.Context, network string, address string) (net.Conn, error), error) {
	dialer := &proxy.Dialer{}
	var cfg tls.Config
	if skipHostnameVerification {
		cfg = tls.Config{InsecureSkipVerify: true}
	} else {
		u, err := url.ParseRequestURI(endpoint)
		if err != nil {
			return nil, err
		}
		cfg = tls.Config{ServerName: u.Hostname()}
	}
	dialer.Config = &cfg
	return dialer.DialContext, nil
}

// NewWithConfig creates a new instance of the DAX client with an AWS config.
//
// Only configurations relevent to DAX will be used, others will be ignored.
//
// Example:
//
//		cfg, err := config.LoadDefaultConfig(context.Background())
//	 if err != nil {
//		  // handle error
//	 }
//
//		// Create a DAX client from an aws config.
//		svc, err := dax.NewWithConfig(cfg)
func NewWithConfig(config aws.Config) (*Dax, error) {
	dc := DefaultConfig()
	dc.mergeFrom(config)
	return New(dc)
}

func (c *Config) mergeFrom(ac aws.Config) {
	if ac.Retryer != nil {
		c.WriteRetries = ac.RetryMaxAttempts
		c.ReadRetries = ac.RetryMaxAttempts
	}
	if ac.Logger != nil {
		c.Logger = ac.Logger
	}
	if ac.EndpointResolver != nil {
		c.EndpointResolver = ac.EndpointResolver
	}
	if ac.EndpointResolverWithOptions != nil {
		c.EndpointResolverWithOptions = ac.EndpointResolverWithOptions
	}
	if ac.Credentials != nil {
		c.Credentials = ac.Credentials
	}
	c.Region = ac.Region
}

func (c *Config) requestOptions(read bool, ctx context.Context, opts ...func(*dynamodb.Options)) (client.RequestOptions, context.CancelFunc, error) {
	r := c.WriteRetries
	if read {
		r = c.ReadRetries
	}
	var cfn context.CancelFunc
	if ctx == nil && c.RequestTimeout > 0 {
		ctx, cfn = context.WithTimeout(context.Background(), c.RequestTimeout)
	}
	opt := client.RequestOptions{
		// LogLevel:   c.LogLevel,
		Logger:     c.Logger,
		MaxRetries: r,
	}
	if err := opt.MergeFromRequestOptions(ctx, opts...); err != nil {
		if c.Logger != nil { // && c.LogLevel.AtLeast(aws.LogDebug) {
			c.Logger.Logf(logging.Debug, "DEBUG: Error in merging from Request Options : %s", err)
		}
		return client.RequestOptions{}, nil, err
	}
	return opt, cfn, nil
}
