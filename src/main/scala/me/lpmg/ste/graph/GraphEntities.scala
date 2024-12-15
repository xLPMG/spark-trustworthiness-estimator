package me.lpmg.ste.graph

import org.apache.spark.util.collection.BitSet

sealed trait VertexType extends Serializable {
  val id: Long
  val trustScore: Float
}

/** Represents a revision vertex in the graph.
  *
  * @param trustScore
  *   Trust score of the revision
  * @param contributorId
  *   ID of the contributor
  * @param templatePresence
  *   Bitset representing the presence of templates
  * @param templateAdded
  *   Bitset representing the added templates
  * @param templateRemoved
  *   Bitset representing the removed templates
  * @param isGroundTruth
  *   Whether the revision is ground truth
  */
case class RevisionVertex(
    val id: Long,
    val trustScore: Float,
    val contributorId: Int,
    val templatePresence: BitSet,
    val templateAdded: BitSet,
    val templateRemoved: BitSet,
    val isGroundTruth: Boolean = false
) extends VertexType {

  override def toString(): String = {
    s"Revision(trustScore=${trustScore}, contributorId=${contributorId}, templatePresence=${bitSetToBinaryString(templatePresence)}, templateAdded=${bitSetToBinaryString(templateAdded)}, templateRemoved=${bitSetToBinaryString(templateRemoved)}, isGroundTruth=${isGroundTruth})"
  }

  private def bitSetToBinaryString(bitSet: BitSet): String = {
    val binaryString = (0 until bitSet.capacity).map { bit =>
      if (bitSet.get(bit)) '1' else '0'
    }.mkString
    binaryString.reverse.dropWhile(_ == '0').reverse
  }
}

/**
  * Represents a source vertex in the graph.
  *
  * @param id
  * @param domain
  * @param trustScore
  */
case class SourceVertex(
  id: Long,
  domain: String,
  val trustScore: Float
) extends VertexType

final object EdgeType {
  final val isParentOf: Byte = 0.toByte
  final val isChildOf: Byte = 1.toByte
  final val hasSource: Byte = 3.toByte
  final val isReferencedBy: Byte = 4.toByte
}