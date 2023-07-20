/*
 * Copyright (c) 2012-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.rdbloader.test

import com.snowplowanalytics.snowplow.rdbloader.cloud.authservice.LoadAuthService

object PureLoadAuthService {
  def interpreter: LoadAuthService[Pure] =
    new LoadAuthService[Pure] {
      def forLoadingEvents: Pure[LoadAuthService.LoadAuthMethod] =
        Pure.pure(LoadAuthService.LoadAuthMethod.NoCreds)
      def forFolderMonitoring: Pure[LoadAuthService.LoadAuthMethod] =
        Pure.pure(LoadAuthService.LoadAuthMethod.NoCreds)
    }
}
