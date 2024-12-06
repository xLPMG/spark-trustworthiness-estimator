package me.lpmg.ste.types

import org.apache.spark.util.collection.BitSet

/** Represents a revision vertex in the graph. Only contains the necessary
  * information for the trust algorithm.
  *
  * @param isGroundTruth
  * @param trustScore
  * @param isRedirect
  */
class RevisionVertex(
    val trustScore: Float,
    val contributorId: Int,
    val templatePresence: BitSet,
    val templateAdded: BitSet,
    val templateRemoved: BitSet,
    val isGroundTruth: Boolean = false
) extends Serializable {
  def copy(
      trustScore: Float = this.trustScore,
      contributorId: Int = this.contributorId,
      templatePresence: BitSet = this.templatePresence,
      templateAdded: BitSet = this.templateAdded,
      templateRemoved: BitSet = this.templateRemoved,
      isGroundTruth: Boolean = this.isGroundTruth
  ): RevisionVertex = {
    new RevisionVertex(
      trustScore,
      contributorId,
      templatePresence,
      templateAdded,
      templateRemoved,
      isGroundTruth
    )
  }

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
