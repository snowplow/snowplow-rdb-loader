/*
 * Copyright (c) 2012-2022 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.rdbloader.common.cloud

import cats.MonadThrow
import cats.syntax.either._

import fs2.{Pipe, Stream}

import io.circe.{Decoder, Encoder, Json}

import shapeless.tag
import shapeless.tag._

trait BlobStorage[F[_]] {

  /**
   * List blob storage folder
   */
  def list(bucket: BlobStorage.Folder, recursive: Boolean): Stream[F, BlobStorage.BlobObject]

  def put(path: BlobStorage.Key, overwrite: Boolean): Pipe[F, Byte, Unit]

  def get(path: BlobStorage.Key): F[Either[Throwable, String]]

  /** Check if blob storage key exist */
  def keyExists(key: BlobStorage.Key): F[Boolean]
}

/**
 * Common types and functions for Snowplow blob storage clients
 */
object BlobStorage {
  def apply[F[_]](implicit ev: BlobStorage[F]): BlobStorage[F] = ev

  /**
   * Refined type for blob storage bucket, allowing only valid blob storage paths (with `xx://`
   * prefix and trailing slash)
   */
  type Folder = String @@ BlobStorageFolderTag

  implicit class FolderOps(f: Folder) {
    def withKey(s: String): BlobStorage.Key =
      BlobStorage.Key.join(f, s)

    def append(s: String): BlobStorage.Folder =
      Folder.append(f, s)

    def folderName: String =
      f.split("/").last

    def bucketName: String =
      f.split("://").last.split("/").head

    /**
     * Find diff of two blob storage paths. Return None if parent path is longer than sub path or
     * parent path doesn't match with sub path.
     */
    def diff(parent: Folder): Option[String] = {
      def go(parentParts: List[String], subParts: List[String]): Option[String] =
        (parentParts, subParts) match {
          case (_, Nil) =>
            None
          case (Nil, s) =>
            Some(s.mkString("/"))
          case (pHead :: _, sHead :: _) if pHead != sHead =>
            None
          case (pHead :: pTail, sHead :: sTail) if pHead == sHead =>
            go(pTail, sTail)
        }

      go(parent.split("/").toList, f.split("/").toList)
    }
  }

  object Folder extends tag.Tagger[BlobStorageFolderTag] {

    def parse(s: String): Either[String, Folder] = s match {
      case _ if !correctlyPrefixed(s) => s"Bucket name $s doesn't start with s3:// s3a:// s3n:// or gs:// prefix".asLeft
      case _ if s.length > 1024 => "Key length cannot be more than 1024 symbols".asLeft
      case _ => coerce(s).asRight
    }

    /** Turn proper `xx://bucket/path/` string into `Folder` */
    def coerce(s: String): Folder =
      apply(appendTrailingSlash(fixPrefix(s)).asInstanceOf[Folder])

    def append(bucket: Folder, s: String): Folder = {
      val normalized = if (s.endsWith("/")) s else s + "/"
      coerce(bucket + normalized)
    }

    def getParent(key: Folder): Folder = {
      val string = key.split("/").dropRight(1).mkString("/")
      coerce(string)
    }

    private def appendTrailingSlash(s: String): String =
      if (s.endsWith("/")) s
      else s + "/"

  }

  /**
   * Extract `xx://path/run=YYYY-MM-dd-HH-mm-ss/atomic-events` part from Set of prefixes that can be
   * used in config.yml In the end it won't affect how blob storage is accessed
   */
  val supportedPrefixes = Set("s3", "s3n", "s3a", "gs")

  private def correctlyPrefixed(s: String): Boolean =
    supportedPrefixes.foldLeft(false) { (result, prefix) =>
      result || s.startsWith(s"$prefix://")
    }

  case class BlobObject(key: Key, size: Long)

  /**
   * Refined type for blob storage key, allowing only valid blob storage paths (with `xx://` prefix
   * and without trailing shash)
   */
  type Key = String @@ BlobStorageKeyTag

  object Key extends tag.Tagger[BlobStorageKeyTag] {

    def join(folder: Folder, name: String): Key =
      coerce(folder + name)

    def getParent(key: Key): Folder = {
      val string = key.split("/").dropRight(1).mkString("/")
      Folder.coerce(string)
    }

    def coerce(s: String): Key =
      fixPrefix(s).asInstanceOf[Key]

    def parse(s: String): Either[String, Key] = s match {
      case _ if !correctlyPrefixed(s) => s"Bucket name $s doesn't start with s3:// s3a:// s3n:// or gs:// prefix".asLeft
      case _ if s.length > 1024 => "Key length cannot be more than 1024 symbols".asLeft
      case _ if s.endsWith("/") => "Blob storage key cannot have trailing slash".asLeft
      case _ => coerce(s).asRight
    }
  }

  implicit class KeyOps(k: Key) {
    def getParent: BlobStorage.Folder =
      BlobStorage.Key.getParent(k)
  }

  // Tags for refined types
  sealed trait BlobStorageFolderTag

  sealed trait BlobStorageKeyTag

  sealed trait AtomicEventsKeyTag

  implicit val blobStorageFolderDecoder: Decoder[Folder] =
    Decoder.decodeString.emap(Folder.parse)
  implicit val blobStorageFolderEncoder: Encoder[Folder] =
    Encoder.instance(Json.fromString)

  /**
   * Split blob storage path into bucket name and folder path
   *
   * @param path
   *   blob storage full path with `xx://` and with trailing slash
   * @return
   *   pair of bucket name and remaining path ("some-bucket", "some/prefix/")
   */
  private[rdbloader] def splitPath(path: Folder): (String, String) =
    path.split("://").drop(1).mkString("").split("/").toList match {
      case head :: Nil => (head, "/")
      case head :: tail => (head, tail.mkString("/") + "/")
      case Nil => throw new IllegalArgumentException(s"Invalid blob storage path was passed") // Impossible
    }

  /**
   * Split blob storage key into bucket name and filePath
   *
   * @param key
   *   blob storage full path with `xx://` prefix and without trailing slash
   * @return
   *   pair of bucket name and remaining path ("some-bucket", "some/prefix/")
   */
  def splitKey(key: Key): (String, String) =
    key.split("://").drop(1).mkString("").split("/").toList match {
      case head :: tail => (head, tail.mkString("/").stripSuffix("/"))
      case _ => throw new IllegalArgumentException(s"Invalid blob storage key [$key] was passed") // Impossible
    }

  /** Used only to list blob storage directories, not to read and write data. */
  private def fixPrefix(s: String): String =
    if (s.startsWith("s3n")) "s3" + s.stripPrefix("s3n")
    else if (s.startsWith("s3a")) "s3" + s.stripPrefix("s3a")
    else if (s.startsWith("gcs")) "gs" + s.stripPrefix("gcs")
    else s

  def noop[F[_]: MonadThrow]: BlobStorage[F] = new BlobStorage[F] {

    override def list(bucket: Folder, recursive: Boolean): Stream[F, BlobObject] =
      Stream.empty

    override def put(path: Key, overwrite: Boolean): Pipe[F, Byte, Unit] =
      _.map(_ => ())

    override def get(path: Key): F[Either[Throwable, String]] =
      MonadThrow[F].raiseError(new IllegalArgumentException("noop blobstorage interpreter"))

    override def keyExists(key: Key): F[Boolean] =
      MonadThrow[F].raiseError(new IllegalArgumentException("noop blobstorage interpreter"))
  }
}
