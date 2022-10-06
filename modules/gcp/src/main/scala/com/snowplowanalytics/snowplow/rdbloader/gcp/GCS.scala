package com.snowplowanalytics.snowplow.rdbloader.gcp

import cats.effect._
import cats.implicits._
import blobstore.gcs._
import com.google.cloud.storage.{BlobInfo, StorageOptions}
import com.snowplowanalytics.snowplow.rdbloader.common.cloud.BlobStorage.{Folder, Key}
import com.snowplowanalytics.snowplow.rdbloader.common.cloud.BlobStorage
import fs2.Pipe

object GCS {

  def getClient[F[_] : ConcurrentEffect : ContextShift](blocker: Blocker): Resource[F, GcsStore[F]] =
    Resource.pure[F, GcsStore[F]](GcsStore(StorageOptions.getDefaultInstance.getService, blocker))

  def blobStorage[F[_] : ConcurrentEffect](client: GcsStore[F]): Resource[F, BlobStorage[F]] =
    Resource.pure[F, BlobStorage[F]](
      new BlobStorage[F] {

        override def list(folder: Folder, recursive: Boolean): fs2.Stream[F, BlobStorage.BlobObject] = {
          val (bucket, path) = BlobStorage.splitPath(folder)
          client.list(GcsPath(BlobInfo.newBuilder(bucket, path).build()), recursive)
            .map { gcsPath =>
              val root = gcsPath.root.getOrElse("")
              val pathFromRoot = gcsPath.pathFromRoot.toList.mkString("/")
              val filename = gcsPath.fileName.getOrElse("")
              val key = BlobStorage.Key.coerce(s"gs://$root/$pathFromRoot/$filename")
              BlobStorage.BlobObject(key, gcsPath.size.getOrElse(0L))
            }
        }

        override def put(key: Key, overwrite: Boolean): Pipe[F, Byte, Unit] = {
          val (bucket, path) = BlobStorage.splitKey(key)
          client.put(GcsPath(BlobInfo.newBuilder(bucket, path).build()), overwrite)
        }

        override def get(key: Key): F[Either[Throwable, String]] = {
          val (bucket, path) = BlobStorage.splitKey(key)
          client
            .get(GcsPath(BlobInfo.newBuilder(bucket, path).build()), 1024)
            .compile
            .to(Array)
            .map(array => new String(array))
            .attempt
        }

        override def keyExists(key: Key): F[Boolean] = {
          val (bucket, path) = BlobStorage.splitKey(key)
          client.list(GcsPath(BlobInfo.newBuilder(bucket, path).build())).compile.toList.map(_.nonEmpty)
        }
      }
    )
}
