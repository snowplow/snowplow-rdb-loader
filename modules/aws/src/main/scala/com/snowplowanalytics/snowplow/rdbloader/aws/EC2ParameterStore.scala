package com.snowplowanalytics.snowplow.rdbloader.aws

import cats.effect._
import cats.implicits._
import com.amazonaws.services.simplesystemsmanagement.AWSSimpleSystemsManagementClientBuilder
import com.amazonaws.services.simplesystemsmanagement.model.{AWSSimpleSystemsManagementException, GetParameterRequest}
import com.snowplowanalytics.snowplow.rdbloader.common.cloud.SecretStore

object EC2ParameterStore {

  /**
   * Get value from AWS EC2 Parameter Store
   *
   * @param name systems manager parameter's name with SSH key
   * @return decrypted string with key
   */
  def secretStore[F[_]: Sync]: Resource[F, SecretStore[F]] =
    Resource.pure[F, SecretStore[F]](
      new SecretStore[F] {
        override def getValue(key: String): F[String] = {
          val result = for {
            client <- Sync[F].delay(AWSSimpleSystemsManagementClientBuilder.defaultClient())
            req: GetParameterRequest = new GetParameterRequest().withName(key).withWithDecryption(true)
            par <- Sync[F].delay(client.getParameter(req))
          } yield par.getParameter.getValue

          result.recoverWith {
            case e: AWSSimpleSystemsManagementException =>
              Sync[F].raiseError(new RuntimeException(s"Cannot get $key EC2 property: ${e.getMessage}"))
          }
      }
    }
  )
}
