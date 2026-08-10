/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */

package com.snowplowanalytics.snowplow.rdbloader.transformer.batch.aws

import software.amazon.awssdk.auth.credentials.{AwsCredentials, AwsCredentialsProvider, DefaultCredentialsProvider}

/**
 * Bridges hadoop-aws's reflection-based credential loader with SDK v2's DefaultCredentialsProvider.
 *
 * hadoop-aws instantiates S3A credential providers via reflection, requiring a public no-arg
 * constructor. SDK v2 credential providers use the builder/factory pattern and have no public
 * constructors, so hadoop-aws silently skips them and falls back to the EC2 instance profile. This
 * wrapper supplies the no-arg constructor hadoop-aws needs and delegates to
 * DefaultCredentialsProvider, which covers the full AWS credential chain:
 *   - Java system properties
 *   - Environment variables (AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY)
 *   - Web identity token file (IRSA on EKS: AWS_WEB_IDENTITY_TOKEN_FILE + AWS_ROLE_ARN)
 *   - Shared credentials file (~/.aws/credentials)
 *   - ECS task role / EC2 instance metadata
 *
 * This class is set as the default S3A credential provider in docker-entrypoint.sh, so IRSA works
 * automatically on EKS without any per-deployment credential configuration.
 */
class S3ADefaultCredentialsProvider extends AwsCredentialsProvider {
  private val delegate = DefaultCredentialsProvider.create()
  override def resolveCredentials(): AwsCredentials = delegate.resolveCredentials()
}
