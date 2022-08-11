package com.snowplowanalytics.snowplow.rdbloader.common.cloud.gcp

import scala.concurrent.duration._
import cats.effect._
import cats.implicits._
import blobstore.gcs._
import com.google.api.gax.core.NoCredentialsProvider
import com.google.api.gax.grpc.GrpcTransportChannel
import com.google.api.gax.rpc.{FixedTransportChannelProvider, TransportChannelProvider}
import com.google.pubsub.v1.PubsubMessage
import com.google.cloud.storage.{BlobInfo, StorageOptions}
import com.permutive.pubsub.consumer.decoder.MessageDecoder
import com.permutive.pubsub.consumer.grpc.{PubsubGoogleConsumer, PubsubGoogleConsumerConfig}
import com.permutive.pubsub.consumer.Model.{Subscription, ProjectId => ConsumerProjectId}
import com.permutive.pubsub.producer.Model.{Topic, ProjectId => ProducerProjectId}
import com.permutive.pubsub.producer.encoder.MessageEncoder
import com.permutive.pubsub.producer.grpc.{GooglePubsubProducer, PubsubProducerConfig}
import com.snowplowanalytics.snowplow.rdbloader.common.cloud.BlobStorage.{Folder, Key}
import com.snowplowanalytics.snowplow.rdbloader.common.cloud.{BlobStorage, Queue}
import fs2.Pipe
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder
import org.typelevel.log4cats.Logger

object GCP {

  implicit val byteArrayEncoder: MessageEncoder[Array[Byte]] =
    new MessageEncoder[Array[Byte]] {
      def encode(a: Array[Byte]): Either[Throwable, Array[Byte]] =
        a.asRight
    }

  implicit val byteArrayMessageDecoder: MessageDecoder[Array[Byte]] =
    new MessageDecoder[Array[Byte]] {
      def decode(message: Array[Byte]): Either[Throwable, Array[Byte]] =
        message.asRight
    }

  def queueProducer[F[_]: Concurrent: Logger](projectId: String, topic: String): Resource[F, Queue.Producer[F]] = {
    val config = PubsubProducerConfig[F](
      batchSize = 1000,
      requestByteThreshold = Some(8000000),
      delayThreshold = 200.milliseconds,
      onFailedTerminate = err => Logger[F].error(err)("PubSub sink termination error")
    )

    GooglePubsubProducer
      .of[F, Array[Byte]](ProducerProjectId(projectId), Topic(topic), config)
      .map(producer => {
        new Queue.Producer[F] {
          override def send(groupId: Option[String], message: String): F[Unit] =
            producer.produce(message.getBytes).void
        }
      })
  }

  def queueConsumer[F[_]: ConcurrentEffect: Logger: ContextShift: Timer](blocker: Blocker,
                                                                         projectId: String,
                                                                         subscription: String,
                                                                         customPubsubEndpoint: Option[String]): Resource[F, Queue.Consumer[F]] =
    for {
      channelProvider <- customPubsubEndpoint.map(createPubsubChannelProvider[F]).sequence
      consumer = new Queue.Consumer[F] {
        override def read(stop: fs2.Stream[F, Boolean]): fs2.Stream[F, String] = {
          val onFailedTerminate: Throwable => F[Unit] =
            e => Logger[F].error(s"Cannot terminate ${e.getMessage}")
          val pubSubConfig =
            PubsubGoogleConsumerConfig(
              onFailedTerminate = onFailedTerminate,
              parallelPullCount = 1,
              maxQueueSize = 10,
              maxAckExtensionPeriod = 2.hours,
              customizeSubscriber = channelProvider.map { c =>
                b => b.setChannelProvider(c).setCredentialsProvider(NoCredentialsProvider.create())
              }
            )
          val errorHandler: (PubsubMessage, Throwable, F[Unit], F[Unit]) => F[Unit] = // Should be useless
            (message, error, _, _) =>
              Logger[F].error(s"Cannot decode message ${message.getMessageId} into array of bytes. ${error.getMessage}")

          // Ack deadline extension isn't needed in here because it is handled by the pubsub library
          PubsubGoogleConsumer
            .subscribe[F, Array[Byte]](blocker, ConsumerProjectId(projectId), Subscription(subscription), errorHandler, pubSubConfig)
            .flatMap { r =>
              Queue.Consumer.postProcess(new String(r.value, "UTF-8"), r.ack)
            }
        }
      }
    } yield consumer

  def getGcsClient[F[_]: ConcurrentEffect: ContextShift](blocker: Blocker): GcsStore[F] = {
    GcsStore(StorageOptions.getDefaultInstance.getService, blocker)
  }

  def blobStorage[F[_]: ConcurrentEffect](client: GcsStore[F]): BlobStorage[F] = new BlobStorage[F] {

    override def listBlob(folder: Folder, recursive: Boolean): fs2.Stream[F, BlobStorage.BlobObject] = {
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

    override def sinkBlob(key: Key, overwrite: Boolean): Pipe[F, Byte, Unit] = {
      val (bucket, path) = BlobStorage.splitKey(key)
      client.put(GcsPath(BlobInfo.newBuilder(bucket, path).build()), overwrite)
    }

    override def readKey(key: Key): F[Either[Throwable, String]] = {
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

  private def createPubsubChannelProvider[F[_]: ConcurrentEffect](host: String): Resource[F, TransportChannelProvider] = {
    Resource.make (
      ConcurrentEffect[F].delay {
        val channel = NettyChannelBuilder.forTarget(host).usePlaintext().build()
        FixedTransportChannelProvider.create(GrpcTransportChannel.create(channel))
      }
    ) (p => ConcurrentEffect[F].delay(p.getTransportChannel.close()))
  }
}
