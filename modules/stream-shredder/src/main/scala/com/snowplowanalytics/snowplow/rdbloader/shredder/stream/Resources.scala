package com.snowplowanalytics.snowplow.rdbloader.shredder.stream

import java.util.UUID

import scala.concurrent.duration._

import cats.data.EitherT
import cats.implicits._

import cats.effect.concurrent.Ref
import cats.effect.{Blocker, Clock, Resource, Timer, Concurrent, Sync}

import fs2.concurrent.SignallingRef

import io.circe.Json

import org.typelevel.log4cats.slf4j.Slf4jLogger

import com.snowplowanalytics.iglu.client.Client
import com.snowplowanalytics.iglu.client.resolver.{InitSchemaCache, InitListCache}

import com.snowplowanalytics.snowplow.rdbloader.common.transformation.EventUtils
import com.snowplowanalytics.snowplow.rdbloader.common.config.ShredderConfig.QueueConfig

import com.snowplowanalytics.aws.AWSQueue

case class Resources[F[_]](iglu: Client[F, Json],
                           atomicLengths: Map[String, Int],
                           awsQueue: AWSQueue[F],
                           instanceId: String,
                           blocker: Blocker,
                           halt: SignallingRef[F, Boolean],
                           windows: State.Windows[F],
                           global: Ref[F, Long])

object Resources {

  private implicit def logger[F[_]: Sync] = Slf4jLogger.getLogger[F]

  def mk[F[_]: Concurrent: Clock: InitSchemaCache: InitListCache: Timer](igluConfig: Json, queueConfig: QueueConfig): Resource[F, Resources[F]] = {
    val init = for {
      igluClient <- Client.parseDefault[F](igluConfig)
        .leftMap(e => new RuntimeException(s"Error while parsing Iglu config: ${e.getMessage()}"))
      atomicLengths <- EitherT(EventUtils.getAtomicLengths(igluClient.resolver))
    } yield (igluClient, atomicLengths)
    val client = init.value.flatMap {
      case Right(init) => Sync[F].pure(init)
      case Left(error) => Sync[F].raiseError[(Client[F, Json], Map[String, Int])](error)
    }

    for {
      (client, lengths) <- Resource.eval(client)
      blocker <- Blocker[F]
      state <- Resource.make(State.init[F]) { global =>
        global.get.flatMap { stack =>
          if (stack.isEmpty)
            logger.warn(s"Final window state is empty")
          else
            logger.info(s"Final window state:\n${stack.mkString("\n")}")
        }
      }
      awsQueue <- queueConfig match {
        case QueueConfig.SQS(queueName, region) => AWSQueue.build(AWSQueue.QueueType.SQS, queueName, region.name)
        case QueueConfig.SNS(topicArn, region) => AWSQueue.build(AWSQueue.QueueType.SNS, topicArn, region.name)
      }
      sinks <- Resource.eval(Ref.of(0L))
      instanceId <- Resource
        .eval(Sync[F].delay(UUID.randomUUID()))
        .evalTap(id => logger[F].info(s"Instantiated $id shredder instance"))
      halt <- Resource.make(SignallingRef(false)) { s =>
        logger[F].warn("Halting the source, sleeping for 5 seconds...") *>
          s.set(true) *>
          Timer[F].sleep(5.seconds) *>
          logger[F].warn(s"Shutting down $instanceId instance")
      }
    } yield Resources(client, lengths, awsQueue, instanceId.toString, blocker, halt, state, sinks)
  }
}

