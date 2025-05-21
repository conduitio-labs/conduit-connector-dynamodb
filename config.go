// Copyright © 2025 Meroxa, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package dynamodb

// Config contains shared config parameters, common to the source and
// destination. If you don't need shared parameters you can entirely remove this
// file.
type Config struct {
	// Table is the DynamoDB table name to pull data from, or push data into.
	Table string `json:"table" validate:"required"`
	// AWS region.
	AWSRegion string `json:"aws.region" validate:"required"`
	// AWS access key id. Optional - if not provided, the connector will use the default credential chain
	// (environment variables, shared credentials file, or IAM role). For production environments,
	// it's recommended to use the default credential chain with IAM roles rather than static credentials.
	AWSAccessKeyID string `json:"aws.accessKeyId"`
	// AWS secret access key. Optional - if not provided, the connector will use the default credential chain
	// (environment variables, shared credentials file, or IAM role). For production environments,
	// it's recommended to use the default credential chain with IAM roles rather than static credentials.
	AWSSecretAccessKey string `json:"aws.secretAccessKey"`
	// AWS temporary session token. Optional - if not provided, the connector will use the default credential chain.
	// Note that to keep the connector running long-term, you should use the default credential chain
	// rather than temporary session tokens which will expire. For production environments,
	// it's recommended to use IAM roles (IRSA, EC2 instance profile, or ECS task role).
	AWSSessionToken string `json:"aws.sessionToken"`
	// The URL for AWS (useful when testing the connector with localstack).
	AWSURL string `json:"aws.url"`
}
