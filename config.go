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
	// Table is the DynamoDB table name to pull data from.
	Table string `json:"table" validate:"required"`
	// AWS region.
	AWSRegion string `json:"aws.region" validate:"required"`
	// AWS access key id.
	AWSAccessKeyID string `json:"aws.accessKeyId" validate:"required"`
	// AWS secret access key.
	AWSSecretAccessKey string `json:"aws.secretAccessKey" validate:"required"`
	// AWSURL The URL for AWS (useful when testing the connector with localstack).
	AWSURL string `json:"aws.url"`
	// AWS temporary session token. Note that to keep the connector running long-term, you should use an IAM user with no temporary session token.
	// If the session token is used, then the connector will fail once it expires.
	AWSSessionToken string `json:"aws.sessionToken"`
}
