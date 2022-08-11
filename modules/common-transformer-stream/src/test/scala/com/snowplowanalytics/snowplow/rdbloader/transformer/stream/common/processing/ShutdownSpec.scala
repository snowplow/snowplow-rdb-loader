package com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common.processing

import cats.effect.IO
import cats.effect.concurrent.{Deferred, Ref}
import fs2.Stream

import scala.concurrent.duration.DurationInt

import com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common.processing.BaseProcessingSpec.{TransformerConfig, ProcessingOutput}
import com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common.processing.WiderowJsonProcessingSpec.{appConfig, igluConfig}
import com.snowplowanalytics.snowplow.rdbloader.transformer.stream.common.sources.ParsedC

class ShutdownSpec extends BaseProcessingSpec {

  "Streaming transformer" should {
    "send completion message and checkpoint upon shutdown" in {
      temporaryDirectory.use { outputDirectory =>
        val inputStream = InputEventsProvider.eventStream(
          inputEventsPath = "/processing-spec/1/input/events"
        )

        val config = TransformerConfig(appConfig(outputDirectory), igluConfig)

        for {
          output                    <- runWithShutdown(inputStream, config)
          expectedCompletionMessage <- readMessageFromResource("/processing-spec/1/output/good/widerow/completion.json", outputDirectory)
        } yield {
          removeAppId(output.completionMessages.toList) must beEqualTo(List(expectedCompletionMessage))

          output.checkpointed must beEqualTo(1)
        }

      }.unsafeRunSync()
    }
  }

  def runWithShutdown(input: Stream[IO, ParsedC[Unit]],
                      config: TransformerConfig): IO[ProcessingOutput] = {
    val args = prepareAppArgs(config)
    for {
      wait           <- Deferred[IO, Unit]
      checkpointRef  <- Ref.of[IO, Int](0)
      completionsRef <- Ref.of[IO, Vector[String]](Vector.empty)
      stream          = nonTerminatingStream(input, wait)
      fiber          <- TestApplication.run(args, completionsRef, checkpointRef, stream).start
      _              <- wait.get.timeout(60.seconds)
      _              <- fiber.cancel.timeout(60.seconds) // This terminates the application
      checkpointed   <- checkpointRef.get
      completions    <- completionsRef.get
    } yield ProcessingOutput(completions, checkpointed) 
  }

  // Unlike the input stream, this stream does not terminate naturally.
  // So we can only terminate the app by calling `cancel` on the fiber
  def nonTerminatingStream[A](input: Stream[IO, A], wait: Deferred[IO, Unit]): Stream[IO, A] =
    input ++ Stream.eval(wait.complete(())).drain ++ Stream.never[IO]

}
