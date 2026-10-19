package pprof.spark.bench.synthetic

import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}

import pprof.spark.engine.BuildInfo

/** What a generated data set is (scale-test plan §4): the generator and its version, the design, the true
  * coefficients, the row count, the number of files, and checksums that prove a regenerated table identical.
  */
final case class Manifest(
    design: Design,
    rows: Long,
    files: Int,
    checksum: Long,
    providersChecksum: Long
) {

  def json: String = {
    val mapper = new ObjectMapper()
    val root = mapper.createObjectNode()
    root.put("generator", "pprof_spark.bench.synthetic").put("version", Synthetic.Version)
    root.put("pprof_spark", BuildInfo.version).put("git_sha", BuildInfo.gitSha)
    root.put("model", design.model.name)
    Manifest.fill(root.putObject("parameters"), design.model.parameters)
    root.put("n", design.n).put("m", design.m).put("p", design.p).put("seed", design.seed)
    Manifest.fill(root.putObject("sizes"), design.sizes.describe)
    val features = root.putArray("features")
    design.features.foreach(f => features.add(f))
    val beta = root.putArray("beta")
    design.beta.foreach(b => beta.add(b))
    root
      .put("rows", rows)
      .put("files", files)
      .put("checksum", checksum)
      .put("providers_checksum", providersChecksum)
    mapper.writeValueAsString(root)
  }
}

object Manifest {

  def parse(json: String): JsonNode = new ObjectMapper().readTree(json)

  private def fill(node: ObjectNode, values: Seq[(String, Any)]): Unit = values.foreach {
    case (key, v: Double) => node.put(key, v)
    case (key, v: Int)    => node.put(key, v)
    case (key, v: Long)   => node.put(key, v)
    case (key, v)         => node.put(key, String.valueOf(v))
  }
}
