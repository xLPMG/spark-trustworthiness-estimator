package me.lpmg.ste.types

import org.apache.spark.util.collection.BitSet

/** Collector class of different custom types used in the project.
  */
object Types {
  /** Map of template names to their position in the template bitset */
  final val TemplateBitPositions: Map[String, Byte] = Map(
    "Contradict" -> 0,
    "Disputed" -> 1,
    "Hoax" -> 2,
    "Unreferenced" -> 3
  )

  /** Converts a BitSet to a byte array for storage
    *
    * @param bitSet
    *   The BitSet to convert
    * @return
    *   Array of bytes representing the BitSet
    */
  def BitSetToByteArray(bitSet: BitSet): Array[Byte] = {
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
  def ByteArrayToBitSet(bytes: Array[Byte], capacity: Int): BitSet = {
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
