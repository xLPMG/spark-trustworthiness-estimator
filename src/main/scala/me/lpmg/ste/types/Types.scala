package me.lpmg.ste.types

import org.apache.spark.util.collection.BitSet

/** Collector class of different custom types used in the project.
  */
object Types {

  /** Map of template names to their position in the template bitset Source:
    * https://en.wikipedia.org/wiki/Wikipedia:Template_index/Cleanup
    */
  final val TemplateBitPositions: Map[String, Byte] = Map(
    //////////////////////////////////////////////////////////////////////
    // SOURCES
    //////////////////////////////////////////////////////////////////////
    // Indicates that an article relies on sources that may not be reliable or reputable.
    "Unreliable sources" -> 0,

    // Warns that the article or section cites self-published sources (blogs, personal websites, etc.) which may not meet Wikipedia’s reliability standards.
    "Self-published source" -> 1,

    // Flags situations where a source is citing Wikipedia or another circular reference, leading to reliability issues.
    "Circular source" -> 2,

    // Notes that the article relies heavily on primary sources, which may be biased or incomplete without secondary analysis.
    "Primary sources" -> 3,

    // Used in articles to indicate inline citations that link to insufficiently reliable sources
    "Better source needed" -> 4,

    // Article contains improper references to user-generated content
    "user-generated" -> 5,

    // Indicates that the article has a large number of references in need of verification
    "Verify sources" -> 6,
    //////////////////////////////////////////////////////////////////////
    // wong
    //////////////////////////////////////////////////////////////////////
    "Unreferenced" -> 7,
    "One source" -> 8,
    "Original research" -> 9,
    "More citations needed" -> 10,
    "Disputed" -> 11,
    "POV" -> 12,
    "Third-party" -> 13,
    "Contradict" -> 14,
    "Hoax" -> 15
  )

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
