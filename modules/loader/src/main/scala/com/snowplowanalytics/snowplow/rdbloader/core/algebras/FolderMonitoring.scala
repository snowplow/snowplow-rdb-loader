package com.snowplowanalytics.snowplow.rdbloader.core.algebras

import java.time.{Instant, ZoneId, ZoneOffset}
import java.time.format.{DateTimeFormatter, DateTimeParseException}
import java.util.concurrent.TimeUnit

import scala.concurrent.duration.FiniteDuration

import cats.{Applicative, Functor, Monad}
import cats.effect.{Sync, Timer}
import cats.effect.concurrent.Ref
import cats.implicits._
import doobie.util.Get
import fs2.Stream
import fs2.text.utf8Encode

import com.snowplowanalytics.snowplow.rdbloader.common.S3
import com.snowplowanalytics.snowplow.rdbloader.core.LoaderAction
import com.snowplowanalytics.snowplow.rdbloader.core.config.Config
import com.snowplowanalytics.snowplow.rdbloader.core.db.Statement.{
  CreateAlertingTempTableStatement,
  DropAlertingTempTableStatement,
  FoldersCopyStatement,
  FoldersMinusManifestStatement
}
import com.snowplowanalytics.snowplow.rdbloader.core.algebras.Monitoring.AlertPayload
import com.snowplowanalytics.snowplow.rdbloader.generated.BuildInfo

/**
  * A module for automatic discovery of corrupted (half-shredded) and abandoned (unloaded) folders
  *
  * The logic is following:
  * * Periodically list all folders in shredded archive (down to `since`)
  * * Sink this list into `staging` S3 Folder
  * * Load this list into a Redshift temporary table
  * * Execute MINUS query, finding out what folders are in the list, but *not* in the manifest
  * * Check every that folder for presence of `shredding_complete.json` file
  * Everything with the file is "abandoned", everything without the file is "corrupted"
  */
object FolderMonitoring {

  implicit private val LoggerName =
    Logging.LoggerName(getClass.getSimpleName.stripSuffix("$"))

  implicit val s3FolderGet: Get[S3.Folder] =
    Get[String].temap(S3.Folder.parse)

  private val TimePattern: String =
    "yyyy-MM-dd-HH-mm-ss"

  def createAlertPayload(folder: S3.Folder, message: String): AlertPayload =
    AlertPayload(BuildInfo.version, folder, Monitoring.AlertPayload.Severity.Warning, message, Map.empty)

  val LogTimeFormatter: DateTimeFormatter =
    DateTimeFormatter.ofPattern(TimePattern).withZone(ZoneId.from(ZoneOffset.UTC))

  val ShreddingComplete = "shredding_complete.json"

  /**
    * Check if S3 key name represents a date more recent than a `since`
    *
    * @param since optional duration representing, how fresh a folder needs to be
    *              in order to be taken into account; If None - all folders are taken
    *              into account
    * @param now   current timestamp
    * @param key   S3 key, representing a folder, must be in `run=2021-10-08-16-30-05` format
    *              (no trailing slash); keys of a wrong format won't be filtered out
    * @return false if folder is old enough, true otherwise
    */
  def isRecent(since: Option[FiniteDuration], now: Instant)(folder: S3.Folder): Boolean =
    since match {
      case Some(duration) =>
        Either.catchOnly[DateTimeParseException](
          LogTimeFormatter.parse(folder.stripSuffix("/").takeRight(TimePattern.size))
        ) match {
          case Right(accessor) =>
            val oldest = now.minusMillis(duration.toMillis)
            accessor.query(Instant.from).isAfter(oldest)
          case Left(_) =>
            true
        }
      case None => true
    }

  /**
    * Sink all processed paths of folders in `input` (shredded archive) into `output` (temp staging)
    * Processed folders is everything in shredded archive
    *
    * @param since  optional duration to ignore old folders
    * @param input  shredded archive
    * @param output temp staging path to store the list
    * @return whether the list was non-empty (true) or empty (false)
    */
  def sinkFolders[F[_]: Sync: Timer: Logging: AWS](
    since: Option[FiniteDuration],
    input: S3.Folder,
    output: S3.Folder
  ): F[Boolean] =
    Ref.of[F, Int](0).flatMap { ref =>
      Stream
        .eval(Timer[F].clock.instantNow)
        .flatMap { now =>
          AWS[F]
            .listS3(input, recursive = false)
            .mapFilter(blob =>
              if (blob.key.endsWith("/") && blob.key != input) S3.Folder.parse(blob.key).toOption
              else None
            ) // listS3 returns the root dir as well
            .filter(isRecent(since, now))
            .evalTap(_ => ref.update(size => size + 1))
            .intersperse("\n")
            .through(utf8Encode[F])
            .through(AWS[F].sinkS3(output.withKey("keys"), true))
            .onFinalize(ref.get.flatMap(size => Logging[F].info(s"Saved $size folders from $input in $output")))
        }
        .compile
        .drain *> ref.get.map(size => size != 0)
    }

  /**
    * Check if folders have shredding_complete.json file inside,
    * i.e. checking if they were fully processed by shredder
    * This function uses a blocking S3 call and expects `folders` to be
    * a small list (usually 0-length) because external system (Redshift `MINUS` statements)
    * already filtered these folders as problematic, i.e. missing in `manifest` table
    *
    * @param folders list of folders missing in `manifest` table
    * @return same list of folders with attached `true` if the folder has `shredding_complete.json`
    *         thus processed, but unloaded and `false` if shredder hasn't been fully processed
    */
  def checkShreddingComplete[F[_]: Applicative: AWS](folders: List[S3.Folder]): F[List[(S3.Folder, Boolean)]] =
    folders.traverse(folder => AWS[F].keyExists(folder.withKey(ShreddingComplete)).tupleLeft(folder))

  final case class TempTableOps(
    create: CreateAlertingTempTableStatement,
    drop: DropAlertingTempTableStatement,
    copy: FoldersCopyStatement,
    minus: FoldersMinusManifestStatement
  )

  /**
    * List all folders in `loadFrom`, load the list into temporary Redshift table and check
    * if they exist in `manifest` table. Ones that don't exist are checked for existence
    * of `shredding_complete.json` and turned into corresponding `AlertPayload`
    * @return potentially empty list of alerts
    */
  def check[F[_]: Monad: AWS: JDBC](tableOps: TempTableOps): LoaderAction[F, List[AlertPayload]] =
    for {
      _                 <- JDBC[F].executeUpdate(tableOps.drop)
      _                 <- JDBC[F].executeUpdate(tableOps.create)
      _                 <- JDBC[F].executeUpdate(tableOps.copy)
      onlyS3Batches     <- JDBC[F].executeQueryList[S3.Folder](tableOps.minus)
      foldersWithChecks <- LoaderAction.liftF(checkShreddingComplete[F](onlyS3Batches))
    } yield foldersWithChecks.map {
      case (folder, exists) =>
        if (exists) createAlertPayload(folder, "Unloaded batch")
        else createAlertPayload(folder, "Incomplete shredding")
    }

  /** Get stream of S3 folders emitted with configured interval */
  def getOutputKeys[F[_]: Timer: Functor](folders: Config.Folders): Stream[F, S3.Folder] = {
    val getKey = Timer[F]
      .clock
      .realTime(TimeUnit.MILLISECONDS)
      .map(Instant.ofEpochMilli)
      .map(LogTimeFormatter.format)
      .map(time => folders.staging.append("shredded").append(time))

    Stream.eval(getKey) ++ Stream.fixedRate[F](folders.period).evalMap(_ => getKey)
  }

  /**
    * Alerting entrypoint. Launches a stream or periodic checks.
    * The stream ignores a first failure just printing an error, hoping it's transient,
    * but second failure in row makes the whole stream to crash
    *
    * All configuration necessary for monitoring of corrupted and unloaded folders
    * is target-specific, so the parsing is done in the respective module.
    *
    * Resulting stream has to be running in background.
    *
    * @param folders configuration for folders monitoring
    */
  def run[F[_]: Sync: Timer: AWS: JDBC: Logging: Monitoring](
    folders: Config.Folders,
    tableOps: TempTableOps
  ): Stream[F, Unit] =
    Stream.eval(Ref.of(false)).flatMap { failed =>
      getOutputKeys[F](folders).evalMap { outputFolder =>
        val sinkAndCheck =
          sinkFolders[F](folders.since, folders.shredderOutput, outputFolder).ifM(
            check[F](tableOps).rethrowT.flatMap { alerts =>
              alerts.traverse_ { payload =>
                Monitoring[F].alert(payload) *> Logging[F].warning(s"${payload.message} ${payload.base}")
              }
            },
            Logging[F].info(s"No folders were found in ${folders.shredderOutput}. Skipping manifest check")
          ) *> failed.set(false)
        Logging[F].info("Monitoring shredded folders") *>
          sinkAndCheck.handleErrorWith { error =>
            failed.getAndSet(true).flatMap { failedBefore =>
              if (failedBefore)
                Logging[F].error(error)("Folder monitoring has failed with unhandled exception for the second time") *>
                  Sync[F].raiseError[Unit](error)
              else Logging[F].error(error)("Folder monitoring has failed with unhandled exception, ignoring for now")
            }
          }
      }
    }
}
