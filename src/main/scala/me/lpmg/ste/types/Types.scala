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

  /** Converts a BitSet to a byte array for storage
    *
    * @param bitSet
    *   The BitSet to convert
    * @return
    *   Array of bytes representing the BitSet
    */
  def bitSetToByteArray(bitSet: BitSet): Array[Byte] = {
    val numBytes = (bitSet.capacity + 7) / 8 // Round up to nearest byte
    val bytes = new Array[Byte](numBytes)

    for (i <- 0 until bitSet.capacity) {
      if (bitSet.get(i)) {
        val byteIndex = i / 8
        val bitIndex = i % 8
        bytes(byteIndex) = (bytes(byteIndex) | (1 << bitIndex)).toByte
      }
    }

    bytes
  }

  /** Converts a byte array back to a BitSet
    *
    * @param bytes
    *   The byte array to convert
    * @param capacity
    *   The capacity of the original BitSet
    * @return
    *   BitSet reconstructed from the byte array
    */
  def byteArrayToBitSet(bytes: Array[Byte], capacity: Int): BitSet = {
    val bitSet = new BitSet(capacity)
    for (i <- 0 until capacity) {
      val byteIndex = i / 8
      val bitIndex = i % 8
      if (
        byteIndex < bytes.length && (bytes(byteIndex) & (1 << bitIndex)) != 0
      ) {
        bitSet.set(i)
      }
    }
    bitSet
  }
}
