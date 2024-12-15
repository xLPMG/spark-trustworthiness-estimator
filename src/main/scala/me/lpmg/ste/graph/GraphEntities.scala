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
  * @param templateAdded
  * @param templateRemoved
  * @param isGroundTruth
  *   Whether the revision is ground truth
  */
case class RevisionVertex(
    val id: Long,
    val trustScore: Float,
    val contributorId: Int,
    val templatePresence: Boolean,
    val templateAdded: Boolean,
    val templateRemoved: Boolean,
    val isGroundTruth: Boolean = false
) extends VertexType {

  override def toString(): String = {
    s"Revision(trustScore=${trustScore}, contributorId=${contributorId}, templatePresence=${templatePresence}, templateAdded=${templateAdded}, templateRemoved=${templateRemoved}, isGroundTruth=${isGroundTruth})"
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