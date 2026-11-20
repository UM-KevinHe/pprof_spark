package pprof.spark.engine

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path}

import scala.jdk.CollectionConverters._

/** Edits the metadata record of a saved fit, so that tests can load what an older format version wrote. */
object FitFiles {

  /** Rewrites the one metadata part under `path` with `edit`. */
  def editMetadata(path: String)(edit: String => String): Unit = {
    val parts = Files
      .list(Path.of(path, "metadata"))
      .iterator
      .asScala
      .filter(_.getFileName.toString.startsWith("part-"))
      .toSeq
    require(parts.size == 1, s"one metadata part expected under $path, found ${parts.size}")
    val text = new String(Files.readAllBytes(parts.head), UTF_8)
    Files.write(parts.head, edit(text).getBytes(UTF_8))
    // Hadoop's local file system keeps a checksum beside each file; drop it with the edit.
    Files.deleteIfExists(parts.head.resolveSibling(s".${parts.head.getFileName}.crc"))
  }

  /** The record as format version `version` wrote it: that version, and none of the `missing` fields. */
  def asVersion(version: Int, current: Int, missing: String*)(text: String): String = {
    val marker = s"\"formatVersion\":$current,"
    require(text.contains(marker), text.take(80))
    missing
      .foldLeft(text)((t, name) => t.replaceAll("\"" + name + "\":[^,}]*,", ""))
      .replace(marker, s"\"formatVersion\":$version,")
  }
}
