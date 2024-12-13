package me.lpmg.ste.data

import org.apache.spark.util.collection.BitSet
import me.lpmg.ste.types.Types.bitSetToString
import me.lpmg.ste.types.Types.stringToBitSet

class TypesTest extends munit.FunSuite {

  test("bitSetToString") {
    val bitSet = new BitSet(8)
    bitSet.set(0)
    bitSet.set(2)
    assertEquals(bitSetToString(bitSet), "101")

    val emptyBitSet = new BitSet(8)
    assertEquals(bitSetToString(emptyBitSet), "")
  }

  test("stringToBitSet") {
    val bitSet = stringToBitSet("101", 8)
    assertEquals(bitSet.get(0), true)
    assertEquals(bitSet.get(2), true)
    assertEquals(bitSet.get(3), false)
    assertEquals(bitSet.get(4), false)
    assertEquals(bitSet.get(5), false)
    assertEquals(bitSet.get(6), false)
    assertEquals(bitSet.get(7), false)
  }

}
