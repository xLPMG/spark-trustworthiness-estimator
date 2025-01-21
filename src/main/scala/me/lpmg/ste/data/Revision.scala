package me.lpmg.ste.data

/** Data class for revision data
  *
  * @param revisionId The revision ID
  * @param pairId The revision ID of the pair
  * @param pageId The page ID
  * @param templateAdded Whether a template was added
  * @param templateRemoved Whether a template was removed
  * @param templateAddedGT Whether a template was added (ground truth)
  * @param templateRemovedGT Whether a template was removed (ground truth)
  * @param sources The sources
  */
case class Revision(
    val revisionId: Long,
    val pairId: Long,
    val pageId: Int,
    val templateAdded: Boolean = false,
    val templateRemoved: Boolean = false,
    val templateAddedGT: Boolean = false,
    val templateRemovedGT: Boolean = false,
    val sources: Seq[String] = Seq.empty
) extends Serializable {

  def copy(
      revisionId: Long = this.revisionId,
      pairId: Long = this.pairId,
      pageId: Int = this.pageId,
      templateAdded: Boolean = this.templateAdded,
      templateRemoved: Boolean = this.templateRemoved,
      templateAddedGT: Boolean = this.templateAddedGT,
      templateRemovedGT: Boolean = this.templateRemovedGT,
      sources: Seq[String] = this.sources
  ): Revision = {
    new Revision(
      revisionId,
      pairId,
      pageId,
      templateAdded,
      templateRemoved,
      templateAddedGT,
      templateRemovedGT,
      sources
    )
  }
}
