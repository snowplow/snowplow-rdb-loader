package com.snowplowanalytics.snowplow.rdbloader.transformer.stream.kafka

import cats.effect._
import com.snowplowanalytics.snowplow.rdbloader.azure.AzureBlobStorage
import com.snowplowanalytics.snowplow.rdbloader.common.cloud.BlobStorage
import com.snowplowanalytics.snowplow.rdbloader.common.cloud.BlobStorage.Folder

import java.net.URI

object TestAzureBlobStorage extends IOApp {

  override def run(args: List[String]): IO[ExitCode] =
    AzureBlobStorage
      .createDefault[IO](URI.create("https://PLACEHOLDER.blob.core.windows.net/PLACEHOLDER"))
      .use(showBlobs)
      .as(ExitCode.Success)

  private def showBlobs(storage: BlobStorage[IO]) =
    storage
      .list(Folder.coerce("https://PLACEHOLDER.blob.core.windows.net/PLACEHOLDER/"), true)
      .evalMap(blob => showBlob(storage, blob))
      .compile
      .drain

  private def showBlob(storage: BlobStorage[IO], blob: BlobStorage.BlobObject) =
    storage
      .get(blob.key)
      .flatMap(content => IO(println(asString(blob, content))))

  private def asString(ob: BlobStorage.BlobObject, content: Either[Throwable, String]) =
    s"""
         |KEY -> ${ob.key}
         |CONTENT -> ${content.right.get}
         |-------------------
         |""".stripMargin
}
