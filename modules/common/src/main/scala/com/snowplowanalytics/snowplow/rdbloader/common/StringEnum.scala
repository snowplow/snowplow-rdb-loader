/*
 * Copyright (c) 2012-2021 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */
package com.snowplowanalytics.snowplow.rdbloader.common

import scala.reflect.runtime.universe._
import scala.reflect.runtime.{universe => ru}

import io.circe._

/**
  * Common trait for all ADTs that can be read from string
  * Must be extended by sealed hierarchy including only singletons
  * Used by `decodeStringEnum` to get runtime representation of whole ADT
  */
trait StringEnum {
  /**
    * **IN** string representation.
    * It should be used only to help read `StringEnum` from a string
    * and never other way round, such as render value into SQL statement
    */
  def asString: String
}

object StringEnum {
  /**
    * Derive decoder for ADT with `StringEnum`
    *
    * @tparam A sealed hierarchy
    * @return circe decoder for ADT `A`
    */
  def decodeStringEnum[A <: StringEnum: TypeTag]: Decoder[A] =
    Decoder.instance(parseEnum[A])

  /**
    * Parse element of `StringEnum` sealed hierarchy from circe AST
    *
    * @param hCursor parser's cursor
    * @tparam A sealed hierarchy
    * @return either successful circe AST or decoding failure
    */
  private def parseEnum[A <: StringEnum: TypeTag](hCursor: HCursor): Decoder.Result[A] = {
    for {
      string <- hCursor.as[String]
      method  = fromString[A](string)
      result <- method.asDecodeResult(hCursor)
    } yield result
  }

  /**
    * Parse element of `StringEnum` sealed hierarchy from String
    *
    * @param string line containing `asString` representation of `StringEnum`
    * @tparam A sealed hierarchy
    * @return either successful circe AST or decoding failure
    */
  def fromString[A <: StringEnum: TypeTag](string: String): Either[String, A] = {
    val map = sealedDescendants[A].map { o => (o.asString, o) }.toMap
    map.get(string) match {
      case Some(a) => Right(a)
      case None => Left(s"Unknown ${typeOf[A].typeSymbol.name.toString} [$string]")
    }
  }

  /**
    * Get all objects extending some sealed hierarchy
    * @tparam Root some sealed trait with object descendants
    * @return whole set of objects
    */
  def sealedDescendants[Root: TypeTag]: Set[Root] = {
    val symbol = typeOf[Root].typeSymbol
    val internal = symbol.asInstanceOf[scala.reflect.internal.Symbols#Symbol]
    val descendants = if (internal.isSealed)
      Some(internal.sealedDescendants.map(_.asInstanceOf[Symbol]) - symbol)
    else None
    descendants.getOrElse(Set.empty).map(x => getCaseObject(x).asInstanceOf[Root])
  }

  private val m = ru.runtimeMirror(getClass.getClassLoader)

  /**
    * Reflection method to get runtime object by compiler's `Symbol`
    * @param desc compiler runtime `Symbol`
    * @return "real" scala case object
    */
  private def getCaseObject(desc: Symbol): Any = {
    val mod = m.staticModule(desc.asClass.fullName)
    m.reflectModule(mod).instance
  }
}
