package me.lpmg.ste.types

final case class TemplateProbabilityVector(
    probabilityTemplateAdded: Float,
    probabilityTemplateRemoved: Float,
    probabilityTemplateUnchanged: Float
) {

  def isNotZero(): Boolean = {
    !isZero()
  }

  def isZero(): Boolean = {
    val minimumProbability = 0.0001f
    probabilityTemplateAdded + probabilityTemplateRemoved + probabilityTemplateUnchanged < minimumProbability
  }
}
