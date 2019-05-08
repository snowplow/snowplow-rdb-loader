package com.snowplowanalytics.snowplow.fs2shredder

import fs2.{Stream, io, text}
import cats.implicits._
import cats.effect._

import scala.collection.JavaConverters._
import java.nio.file.{Files, Path, Paths, StandardOpenOption}
import java.util.concurrent.Executors

import cats.Monad
import com.snowplowanalytics.iglu.client.Resolver
import com.snowplowanalytics.iglu.client.resolver.registries.RegistryLookup

import scala.concurrent.ExecutionContext
import com.snowplowanalytics.snowplow.analytics.scalasdk.Event
import com.snowplowanalytics.snowplow.rdbloader.common.{Common, EventUtils, Shredded}

object Shredder extends IOApp {

  private val blockingExecutionContext =
    Resource.make(IO(ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(2))))(ec => IO(ec.shutdown()))

  def list(path: Path): Stream[IO, Path] =
    Stream
      .fromIterator[IO, Path](Files.list(path).iterator().asScala)
      .filter(p => !p.getFileName.toString.startsWith("."))

  def readEvents(blockingEC: ExecutionContext)(path: Path) = {
    io.file.readAll[IO](path, blockingEC, 4096)
      .through(text.utf8Decode)
      .through(text.lines)
      .filter(s => !s.trim.isEmpty)
      .map { line => Event.parse(line).valueOr(error => throw new RuntimeException(error.toList.mkString(", "))) }
  }

  def shred[F[_]: Monad: RegistryLookup: Clock](base: Path, resolver: Resolver[F])(event: Event): F[List[(Path, String)]] = {
    val atomic = EventUtils.alterEnrichedEvent(event, Map())
    val shredded = Common.getShreddedEntities(event)
      .traverse { hierarchy => Shredded.fromHierarchy[F](false, resolver)(hierarchy).map(_.tabular) }
      .map(_.unite)
      .map { s => s.map { case (vendor, name, format, version, data) => (getShredPath(base, vendor, name, format, version), data) } }
    shredded.map { data => (Paths.get(base.toAbsolutePath.toString, "atomic-events", "part-0"), atomic) :: data }
  }

  def getShredPath(base: Path, vendor: String, name: String, format: String, version: String) =
    Paths.get(base.toAbsolutePath.toString, "shredded-tsv", s"vendor=$vendor", s"name=$name", s"format=$format", s"version=$version", "part-0")


  def create(file: Path): IO[Unit] =
    IO {
      if (Files.exists(file)) () else {
        Files.createDirectories(file.getParent)
        Files.createFile(file)
        println(s"Created $file")
      }
    }

  def run(args: List[String]): IO[ExitCode] = {
    ShredderCli.command.parse(args) match {
      case Right(cli) =>
        val resources = for {
          ec <- blockingExecutionContext
          iglu <- Resource.liftF(ShredderCli.loadResolver[IO](cli.igluConfig))
        } yield (ec, iglu)

        val process = Stream.resource(resources)
          .flatMap { case (ec, iglu) =>
            val events = list(cli.inFolder).flatMap(readEvents(ec))
            events.evalMap(shred[IO](cli.outFolder, iglu))
              .flatMap(rows => Stream.emits(rows))
              .evalTap { case (path, _) => create(path) }
              .flatMap { case (path, row) =>
                val sink = io.file.writeAll[IO](path, ec, Seq(StandardOpenOption.WRITE, StandardOpenOption.APPEND))
                val input = Stream.emit(row ++ "\n").through(text.utf8Encode)
                input.through(sink)
              }
          }
        process.compile.drain.as(ExitCode.Success)

      case Left(help) => IO.delay(println(help)) *> IO.pure(ExitCode.Error)
    }
  }
}
