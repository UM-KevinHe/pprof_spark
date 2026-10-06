package pprof.spark.engine.layout

import pprof.spark.engine.layout.GroupKey.{Integral, Text}

class LayoutPlanSuite extends munit.FunSuite {

  private val sizes: Seq[(GroupKey, Long)] = Seq(
    Integral(5) -> 30L,
    Integral(-2) -> 7L,
    Integral(9) -> 120L,
    Integral(1) -> 45L,
    Integral(3) -> 45L,
    Integral(7) -> 2L
  )

  test("whole groups in key order, dense block identifiers, no empty block") {
    val plan = LayoutPlan.create(sizes, targetRowsPerBlock = 50)
    assertEquals(plan.placements.map(_.groupIndex), (0 until 6).toVector)
    assertEquals(plan.placements.map(_.key), sizes.map(_._1).sorted.toVector)
    assertEquals(plan.placements.map(_.blockId).distinct.sorted, (0 until plan.blockCount).toVector)
    assert(plan.blockRows.forall(_ > 0))
    assertEquals(plan.rowCount, 249L)
  }

  test("oversized groups get their own blocks, first") {
    val plan = LayoutPlan.create(sizes, targetRowsPerBlock = 50)
    assertEquals(plan.oversizedGroups, 1)
    assertEquals(plan.placements.find(_.key == Integral(9)).map(_.blockId), Some(0))
    assertEquals(plan.blockCount, 4) // 1 dedicated + ceil(129 / 50) shared
  }

  test("largest group first, each to the least-loaded block, ties to the lowest block") {
    val plan = LayoutPlan.create(sizes, targetRowsPerBlock = 50)
    val blockOf = plan.placements.map(g => g.key -> g.blockId).toMap
    assertEquals(blockOf(Integral(1)), 1)
    assertEquals(blockOf(Integral(3)), 2)
    assertEquals(blockOf(Integral(5)), 3)
    assertEquals(blockOf(Integral(-2)), 3)
    assertEquals(blockOf(Integral(7)), 3)
    assertEquals(plan.blockRows, Vector(120L, 45L, 45L, 39L))
  }

  test("the plan depends on group sizes only, not on their order") {
    val shuffled = new scala.util.Random(11L).shuffle(sizes)
    assertEquals(LayoutPlan.create(shuffled, 50), LayoutPlan.create(sizes, 50))
  }

  test("string keys follow unsigned UTF-8 byte order, not Java's UTF-16 order") {
    val emoji = "\ud83d\ude00"
    val keys = Seq("\uffff", emoji, "a", "Z", "\u00e9").map(Text(_): GroupKey)
    assertEquals(keys.sorted, Seq("Z", "a", "\u00e9", "\uffff", emoji).map(Text(_): GroupKey))
    assert(emoji.compareTo("\uffff") < 0, "Java orders these two the other way")
  }

  test("invalid group sizes are rejected") {
    intercept[IllegalArgumentException](LayoutPlan.create(Seq.empty, 10))
    intercept[IllegalArgumentException](LayoutPlan.create(Seq(Integral(1) -> 0L), 10))
    intercept[IllegalArgumentException](
      LayoutPlan.create(Seq(Integral(1) -> 2L, Integral(1) -> 3L), 10)
    )
    intercept[IllegalArgumentException](LayoutPlan.create(Seq(Integral(1) -> 2L), 0))
  }
}
