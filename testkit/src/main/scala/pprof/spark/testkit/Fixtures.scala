package pprof.spark.testkit

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import java.security.MessageDigest

import scala.jdk.CollectionConverters._

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}

/** The checked-in reference fixtures (§9.3, ADR-0005): synthetic inputs, outputs of the pinned
  * pprof_py and of R, and `manifest.json` with a SHA-256 checksum for every file. Doubles are
  * stored as hexadecimal floating-point strings, which parse back bit for bit.
  */
object Fixtures {

  /** System property naming the fixtures directory; build.sbt sets it for forked tests. */
  val Property: String = "pprof.fixtures"

  lazy val root: Path = sys.props.get(Property).map(Paths.get(_)).getOrElse(findRoot())

  private def findRoot(): Path = Iterator
    .iterate(Paths.get("").toAbsolutePath)(_.getParent)
    .takeWhile(_ != null)
    .map(_.resolve("fixtures"))
    .find(dir => Files.exists(dir.resolve("manifest.json")))
    .getOrElse(
      throw new IllegalStateException(s"no fixtures/manifest.json found; set -D$Property")
    )

  private val mapper = new ObjectMapper()

  /** A JSON file below the fixtures directory. */
  def json(relative: String): JsonNode = mapper.readTree(root.resolve(relative).toFile)

  lazy val manifest: JsonNode = json("manifest.json")

  /** Relative path to expected SHA-256, for every file the manifest lists. */
  def checksums: Map[String, String] =
    manifest.get("files").properties().asScala.map(e => e.getKey -> e.getValue.asText()).toMap

  /** Relative paths of every file under the fixtures directory except the manifest, sorted. */
  def filesOnDisk: Seq[String] = {
    val stream = Files.walk(root)
    try
      stream
        .iterator()
        .asScala
        .filter(Files.isRegularFile(_))
        .map(path => root.relativize(path).toString.replace('\\', '/'))
        .filter(_ != "manifest.json")
        .toVector
        .sorted
    finally stream.close()
  }

  def sha256(relative: String): String =
    MessageDigest
      .getInstance("SHA-256")
      .digest(Files.readAllBytes(root.resolve(relative)))
      .map(b => f"${b & 0xff}%02x")
      .mkString

  /** Names of the Cox cases, as listed in the manifest. */
  def coxCases: Seq[String] = {
    val node = manifest.get("cases").get("cox")
    (0 until node.size).map(i => node.get(i).asText())
  }

  /** An array of hexadecimal floating-point strings (Python's `float.hex`, R's `%a`). */
  def doubles(node: JsonNode): Array[Double] = {
    require(node != null && node.isArray, "expected an array of hexadecimal doubles")
    Array.tabulate(node.size)(i => java.lang.Double.parseDouble(node.get(i).asText()))
  }

  /** A table of numbers with a header row. */
  final case class Table(columns: Vector[String], rows: Vector[Array[Double]]) {
    def column(name: String): Array[Double] = {
      val index = columns.indexOf(name)
      require(index >= 0, s"no column $name")
      rows.map(_(index)).toArray
    }
  }

  /** A CSV file of numbers below the fixtures directory. */
  def csv(relative: String): Table = {
    val lines = Files
      .readAllLines(root.resolve(relative), StandardCharsets.UTF_8)
      .asScala
      .toVector
      .filter(_.nonEmpty)
    Table(
      lines.head.split(',').toVector,
      lines.tail.map(_.split(',').map(java.lang.Double.parseDouble))
    )
  }
}
