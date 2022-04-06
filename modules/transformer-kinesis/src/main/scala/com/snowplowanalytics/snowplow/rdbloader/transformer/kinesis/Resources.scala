package com.snowplowanalytics.snowplow.rdbloader.transformer.kinesis

import cats.effect.concurrent.Ref
import cats.effect.{Blocker, Clock, Concurrent, Resource, Sync, Timer}
import cats.implicits._
import com.snowplowanalytics.aws.AWSQueue
import com.snowplowanalytics.iglu.client.Client
import com.snowplowanalytics.iglu.client.resolver.{InitListCache, InitSchemaCache, Resolver}
import com.snowplowanalytics.snowplow.rdbloader.common.transformation.EventUtils
import fs2.concurrent.SignallingRef
import io.circe.Json
import org.typelevel.log4cats.slf4j.Slf4jLogger
import com.snowplowanalytics.snowplow.rdbloader.common.config.TransformerConfig.QueueConfig

import java.util.UUID
import scala.concurrent.duration.DurationInt

case class Resources[F[_]](iglu: Client[F, Json],
                           atomicLengths: Map[String, Int],
                           awsQueue: AWSQueue[F],
                           instanceId: String,
                           blocker: Blocker,
                           halt: SignallingRef[F, Boolean],
                           windows: State.Windows[F],
                           global: Ref[F, Long])

object Resources {

  implicit private def logger[F[_]: Sync] = Slf4jLogger.getLogger[F]

  def mk[F[_]: Concurrent: Clock: InitSchemaCache: InitListCache: Timer](igluConfig: Json,
                                                                         queueConfig: QueueConfig): Resource[F, Resources[F]] = {
    mkQueueFromConfig(queueConfig).flatMap(mk(igluConfig, _))
  }

  def mk[F[_]: Concurrent: Clock: InitSchemaCache: InitListCache: Timer](igluConfig: Json,
                                                                         awsQueue: AWSQueue[F]): Resource[F, Resources[F]] = {
    for {
      client        <- mkClient(igluConfig)
      atomicLengths <- mkAtomicFieldLengthLimit(client.resolver)
      blocker       <- Blocker[F]
      initialState  <- mkInitialState
      sinks         <- Resource.eval(Ref.of(0L))
      instanceId    <- mkTransformerInstanceId
      halt          <- mkHaltingSignal(instanceId)
    } yield Resources(client, atomicLengths, awsQueue, instanceId.toString, blocker, halt, initialState, sinks)
  }

  private def mkClient[F[_]: Sync: InitSchemaCache: InitListCache](igluConfig: Json): Resource[F, Client[F, Json]] = Resource.eval {
    Client
      .parseDefault[F](igluConfig)
      .leftMap(e => new RuntimeException(s"Error while parsing Iglu config: ${e.getMessage()}"))
      .value
      .flatMap {
        case Right(init) => Sync[F].pure(init)
        case Left(error) => Sync[F].raiseError[Client[F, Json]](error)
      }
  }

  private def mkAtomicFieldLengthLimit[F[_]: Sync: Clock](igluResolver: Resolver[F]): Resource[F, Map[String, Int]] = Resource.eval {
    EventUtils.getAtomicLengths(igluResolver).flatMap {
      case Right(valid) => Sync[F].pure(valid)
      case Left(error)  => Sync[F].raiseError[Map[String, Int]](error)
    }
  }

  private def mkInitialState[F[_]: Sync] = {
    Resource.make(State.init[F]) { state =>
      state.get.flatMap { stack =>
        if (stack.isEmpty)
          logger.warn(s"Final window state is empty")
        else
          logger.info(s"Final window state:\n${stack.mkString("\n")}")
      }
    }
  }

  private def mkTransformerInstanceId[F[_]: Sync] = {
    Resource
      .eval(Sync[F].delay(UUID.randomUUID()))
      .evalTap(id => logger.info(s"Instantiated $id shredder instance"))
  }

  private def mkHaltingSignal[F[_]: Concurrent: Timer](instanceId: UUID) = {
    Resource.make(SignallingRef(false)) { s =>
      logger.warn("Halting the source, sleeping for 5 seconds...") *>
        s.set(true) *>
        Timer[F].sleep(5.seconds) *>
        logger.warn(s"Shutting down $instanceId instance")
    }
  }

  private def mkQueueFromConfig[F[_]: Concurrent](queueConfig: QueueConfig): Resource[F, AWSQueue[F]] = {
    queueConfig match {
      case QueueConfig.SQS(queueName, region) => AWSQueue.build(AWSQueue.QueueType.SQS, queueName, region.name)
      case QueueConfig.SNS(topicArn, region)  => AWSQueue.build(AWSQueue.QueueType.SNS, topicArn, region.name)
    }
  }

}
