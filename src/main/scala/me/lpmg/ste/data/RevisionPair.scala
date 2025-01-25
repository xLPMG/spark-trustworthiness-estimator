package me.lpmg.ste.data

/** @param revisionIdTemplateAdded
  *   The ID of the revision where the template was added.
  * @param revisionIdTemplateRemoved
  *   The ID of the revision where the template was removed.
  * @param pageId
  *   The ID of the page.
  * @param sourcesTemplateAdded
  *   A sequence of sources present when the template was added.
  * @param sourcesTemplateRemoved
  *   A sequence of sources present when the template was removed.
  */
case class RevisionPair(
    val revisionIdTemplateAdded: Long,
    val revisionIdTemplateRemoved: Long,
    val pageId: Int,
    val sourcesTemplateAdded: Seq[String] = Seq.empty,
    val sourcesTemplateRemoved: Seq[String] = Seq.empty
) extends Serializable {

  /** Creates a copy of this RevisionPair with optional new values for its
    * fields.
    *
    * @param revisionIdTemplateAdded
    *   The ID of the revision where the template was added.
    * @param revisionIdTemplateRemoved
    *   The ID of the revision where the template was removed.
    * @param pageId
    *   The ID of the page.
    * @param sourcesTemplateAdded
    *   A sequence of sources present when the template was added.
    * @param sourcesTemplateRemoved
    *   A sequence of sources present when the template was removed.
    * @return
    *   A new RevisionPair instance with the specified values.
    */
  def copy(
      revisionIdTemplateAdded: Long = this.revisionIdTemplateAdded,
      revisionIdTemplateRemoved: Long = this.revisionIdTemplateRemoved,
      pageId: Int = this.pageId,
      sourcesTemplateAdded: Seq[String] = this.sourcesTemplateAdded,
      sourcesTemplateRemoved: Seq[String] = this.sourcesTemplateRemoved
  ): RevisionPair = {
    new RevisionPair(
      revisionIdTemplateAdded,
      revisionIdTemplateRemoved,
      pageId,
      sourcesTemplateAdded,
      sourcesTemplateRemoved
    )
  }
}
