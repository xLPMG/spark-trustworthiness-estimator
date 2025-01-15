package me.lpmg.ste.types

import org.apache.spark.util.collection.BitSet

/** Collector class of different custom types used in the project.
  */
object Types {

  /** Map of template names to their position in the template bitset Source:
    * https://en.wikipedia.org/wiki/Wikipedia:Template_index/Cleanup
    */
  final val TemplateBitPositions: Map[String, Byte] = Map(
    "Circular" -> 0,
    "Better sources needed" -> 1,
    "Dubious" -> 2,
    "No reliable sources" -> 3,
    "Self-published" -> 4,
    "Third-party" -> 5,
    "Unreliable sources" -> 6,
    "User-generated" -> 7
  )

  def escapeTemplates(templateBitPositions: Map[String, Byte]): Map[String, Byte] = {
    templateBitPositions.map {
      case (template, position) =>
        val escapedTemplate = template.toLowerCase.replace(" ", "-")
        escapedTemplate -> position
    }
  }

  /**
    * Converts a BitSet to a binary string
    *
    * @param bitSet
    * @return
    */
  def bitSetToString(bitSet: BitSet): String = {
    val sb = new StringBuilder
    val lastSetBitIndex =
      (bitSet.capacity - 1 to 0 by -1).find(bitSet.get(_)).getOrElse(-1)
    for (i <- 0 to lastSetBitIndex) {
      if (bitSet.get(i)) {
        sb.append("1")
      } else {
        sb.append("0")
      }
    }
    sb.toString()
  }

  /**
    * Converts a binary string to a BitSet
    *
    * @param str
    * @param capacity
    * @return
    */
  def stringToBitSet(str: String, capacity: Int): BitSet = {
    val bitSet = new BitSet(capacity)
    val goUntil = Math.min(str.length, capacity)
    for (i <- 0 until goUntil) {
      if (str(i) == '1') {
        bitSet.set(i)
      }
    }
    bitSet
  }
}
