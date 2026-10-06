package pprof.spark.engine.backend

class BlocksSuite extends munit.FunSuite {

  private def bits(x: Double): Long = java.lang.Double.doubleToRawLongBits(x)

  private def row(group: Int, id: Long, x: Double*): PlacedRow = PlacedRow(0, group, id, x.toArray)

  test("blocks hold rows grouped and in canonical order") {
    val rows = Seq(row(2, 5, 1, 2), row(1, 9, 3, 4), row(2, 1, 5, 6), row(1, 3, 7, 8))
    val block = BlockBuilder.build(0, rows.iterator, 2)
    assertEquals(block.groupIndex.toSeq, Seq(1, 2))
    assertEquals(block.groupStart.toSeq, Seq(0, 2, 4))
    assertEquals(block.rowId.toSeq, Seq(3L, 9L, 1L, 5L))
    assertEquals(block.values.toSeq, Seq(7.0, 8.0, 3.0, 4.0, 5.0, 6.0, 1.0, 2.0))
  }

  test("without identifiers rows are ordered by value, and negative zero becomes positive zero") {
    val rows = Seq(row(0, 0, 0.5, -0.0), row(0, 0, -1.0, 3.0), row(0, 0, 0.5, 0.0))
    val expected = Seq(-1.0, 3.0, 0.5, 0.0, 0.5, 0.0).map(bits)
    assertEquals(BlockBuilder.build(0, rows.iterator, 2).values.toSeq.map(bits), expected)
    assertEquals(BlockBuilder.build(0, rows.reverseIterator, 2).values.toSeq.map(bits), expected)
  }

  test("the arrival order of rows never changes the block") {
    val rows = (0 until 40).map(i => row(i % 3, (i * 7919L) % 101, i * 0.25, -i * 1.5))
    val reference = BlockBuilder.build(4, rows.iterator, 2)
    val shuffled = BlockBuilder.build(4, new scala.util.Random(3L).shuffle(rows).iterator, 2)
    assertEquals(shuffled.values.toSeq.map(bits), reference.values.toSeq.map(bits))
    assertEquals(shuffled.rowId.toSeq, reference.rowId.toSeq)
    assertEquals(shuffled.groupStart.toSeq, reference.groupStart.toSeq)
  }

  test("rows of the wrong width are rejected") {
    intercept[IllegalArgumentException](BlockBuilder.build(0, Iterator(row(0, 0, 1.0)), 2))
  }

  test("ordered reduction follows block identifiers, not arrival order") {
    val partials =
      (0 until 30).map(b => b -> Array(Math.scalb(1.0 + b, (b * 17) % 60 - 30), 1.0 / (b + 1)))
    val reference = OrderedReduction.reduce(partials, 2).toSeq.map(bits)
    val shuffled = new scala.util.Random(5L).shuffle(partials)
    assertEquals(OrderedReduction.reduce(shuffled, 2).toSeq.map(bits), reference)
    intercept[IllegalArgumentException](OrderedReduction.reduce(partials :+ partials.head, 2))
  }
}
