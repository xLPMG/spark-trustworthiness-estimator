package me.lpmg.ste.types

/** Represents a revision vertex in the graph. Only contains the necessary
  * information for the trust algorithm.
  *
  * @param isGroundTruth
  * @param trustScore
  * @param isRedirect
  */
class RevisionVertex(
    val isGroundTruth: Boolean,
    val trustScore: Float,
    val isRedirect: Boolean,
) extends Serializable {
  def copy(
      isGroundTruth: Boolean = this.isGroundTruth,
      trustScore: Float = this.trustScore,
      isRedirect: Boolean = this.isRedirect,
  ): RevisionVertex = {
    new RevisionVertex(isGroundTruth, trustScore, isRedirect)
  }

  override def toString(): String = {
    s"RevisionVertex(isGroundTruth=$isGroundTruth, trustScore=$trustScore, isRedirect=$isRedirect)"
  }
}
