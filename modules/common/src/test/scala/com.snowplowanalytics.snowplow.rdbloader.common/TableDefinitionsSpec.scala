package com.snowplowanalytics.snowplow.rdbloader.common

import scala.io.Source

import org.specs2.mutable.Specification

class TableDefinitionsSpec extends Specification {
  "TableDefinitions" should {
    "have correct atomic columns" in {
      val referenceStream = getClass.getResourceAsStream("/sql/atomic-def.sql")
      val expectedLines = Source.fromInputStream(referenceStream).getLines().toList
      val expected = TableDefinitionsSpec.normalizeSql(expectedLines)

      val resultLines = TableDefinitions.createAtomicEventsTable("atomic").toDdl.split("\n").toList
      val result = TableDefinitionsSpec.normalizeSql(resultLines)

      result must beEqualTo(expected)
    }
  }
}

object TableDefinitionsSpec {
  /** Remove comments and formatting */
  def normalizeSql(lines: List[String]) = lines
    .map(_.dropWhile(_.isSpaceChar)
      .dropWhile(_ == '\t')
      .toLowerCase
      .replace("\"", "")
    )
    .map(line => if (line.contains("--")) "" else line)
    .filterNot(_.isEmpty)
    .map(_.replaceAll("""\s+""", " "))
    .map(_.replaceAll(""",\s""", ","))
}
