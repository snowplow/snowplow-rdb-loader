/*
 * Copyright (c) 2012-2023 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at
 * http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and
 * limitations there under.
 */
package com.snowplowanalytics.snowplow.rdbloader.azure

import java.util.Date

import com.azure.core.credential.TokenRequestContext
import com.azure.identity.DefaultAzureCredentialBuilder

import org.apache.hadoop.fs.azurebfs.extensions.CustomTokenProviderAdaptee
import org.apache.hadoop.conf.Configuration

/**
 * Creates Azure tokens for using with Hadoop file system. It isn't directly used in the project.
 * Instead, class name is given as Hadoop configuration in the Main of Transformer Kafka. Then, it
 * is used by Hadoop Azure File System to generate tokens.
 */
class AzureTokenProvider extends CustomTokenProviderAdaptee {

  private var expiryTime: Date = _
  private var accountName: String = _

  override def initialize(configuration: Configuration, accountName: String): Unit =
    this.accountName = accountName

  override def getAccessToken: String = {
    val creds = new DefaultAzureCredentialBuilder().build()
    val request = new TokenRequestContext().addScopes(s"https://$accountName/.default")
    val token = creds.getToken(request).block()
    this.expiryTime = new Date(token.getExpiresAt.toInstant.toEpochMilli)
    token.getToken
  }

  override def getExpiryTime: Date = expiryTime
}
