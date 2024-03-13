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
package com.snowplowanalytics.snowplow.rdbloader.azure

import java.net.URI

import cats.data.Validated.Valid
import cats.effect.IO
import cats.effect.unsafe.implicits.global

import blobstore.azure.AzureBlob
import blobstore.url.{Authority, Path, Url}

import com.snowplowanalytics.snowplow.rdbloader.azure.AzureBlobStorage.PathParts
import com.snowplowanalytics.snowplow.rdbloader.common.cloud.BlobStorage
import com.snowplowanalytics.snowplow.rdbloader.common.cloud.BlobStorage.Key

import org.specs2.mutable.Specification

class AzureBlobStorageSpec extends Specification {
  import AzureBlobStorageSpec._

  "PathParts" should {
    "parse root path" in {
      PathParts.parse(testContainerPath) must beEqualTo(
        PathParts(
          containerName = "test-container",
          storageAccountName = "accountName",
          scheme = "https",
          endpointSuffix = "core.windows.net",
          relative = ""
        )
      )
    }

    "parse non-root path" in {
      PathParts.parse(s"$testContainerPath/path1/path2/") must beEqualTo(
        PathParts(
          containerName = "test-container",
          storageAccountName = "accountName",
          scheme = "https",
          endpointSuffix = "core.windows.net",
          relative = "path1/path2/"
        )
      )
    }

    "extract relative path" in {
      PathParts.parse(testContainerPath).extractRelative(s"$testContainerPath/path1/path2") must beEqualTo(
        "path1/path2"
      )
    }

    "extract root" in {
      PathParts.parse(s"$testContainerPath/path1/path2").root must beEqualTo(
        "https://accountName.blob.core.windows.net/"
      )
    }

    "convert to parquet path correctly" in {
      PathParts.parse(s"$testContainerPath/path1/path2").toParquetPath must beEqualTo(
        "abfss://test-container@accountName.dfs.core.windows.net/path1/path2/"
      )
    }
  }

  "createStorageUrlFrom" should {
    "return expected URL" in {
      AzureBlobStorage
        .createDefault[IO](URI.create(s"$testContainerPath/path1/path2"))
        .use { blobStorage =>
          IO.delay {
            blobStorage.createStorageUrlFrom(s"$testContainerPath/path1/path2/path3/path4")
          }
        }
        .unsafeRunSync() must beEqualTo(
        Valid(Url("https", Authority.unsafe("test-container"), Path.plain("path1/path2/path3/path4")))
      )

      AzureBlobStorage
        .createDefault[IO](URI.create(s"$testContainerPath"))
        .use { blobStorage =>
          IO.delay {
            blobStorage.createStorageUrlFrom(s"$testContainerPath/path1/path2/path3/path4")
          }
        }
        .unsafeRunSync() must beEqualTo(
        Valid(Url("https", Authority.unsafe("test-container"), Path.plain("path1/path2/path3/path4")))
      )
    }
  }

  "createBlobObject" should {
    "create blob object from given url correctly" in {
      AzureBlobStorage
        .createDefault[IO](URI.create(s"$testContainerPath/path1/path2"))
        .use { blobStorage =>
          IO.delay {
            blobStorage.createBlobObject(
              Url(
                "https",
                Authority.unsafe("test-container"),
                Path.of(
                  "path1/path2/path3/path4",
                  AzureBlob("test-container", "test-blob", None, Map.empty)
                )
              )
            )
          }
        }
        .unsafeRunSync() must beEqualTo(
        BlobStorage.BlobObject(Key.coerce(s"$testContainerPath/path1/path2/path3/path4"), 0L)
      )

      AzureBlobStorage
        .createDefault[IO](URI.create(s"$testContainerPath"))
        .use { blobStorage =>
          IO.delay {
            blobStorage.createBlobObject(
              Url(
                "https",
                Authority.unsafe("test-container"),
                Path.of(
                  "path1/path2/path3/path4",
                  AzureBlob("test-container", "test-blob", None, Map.empty)
                )
              )
            )
          }
        }
        .unsafeRunSync() must beEqualTo(
        BlobStorage.BlobObject(Key.coerce(s"$testContainerPath/path1/path2/path3/path4"), 0L)
      )
    }
  }

}

object AzureBlobStorageSpec {

  val testContainerPath = "https://accountName.blob.core.windows.net/test-container"
}
