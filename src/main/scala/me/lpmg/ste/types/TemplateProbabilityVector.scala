package me.lpmg.ste.types

final case class TemplateProbabilityVector(
    probabilityTemplateAdded: Float,
    probabilityTemplateRemoved: Float
) extends Serializable {
  def isZero(): Boolean = {
    val minimumProbability = 0.0001f
    probabilityTemplateAdded + probabilityTemplateRemoved < minimumProbability
  }

  def isUndecided(): Boolean = {
    val tolerance = 0.0001f
    Math.abs(probabilityTemplateAdded - probabilityTemplateRemoved) < tolerance
  }

  def extractValues(): (Float, Float) = {
    (probabilityTemplateAdded, probabilityTemplateRemoved)
  }

  def extractValuesString: String = {
    val rounded1 = BigDecimal(probabilityTemplateAdded).setScale(
      4,
      BigDecimal.RoundingMode.HALF_UP
    )
    val rounded2 =
      (BigDecimal(1.0) - rounded1).setScale(4, BigDecimal.RoundingMode.HALF_UP)
    s"(${rounded1.toString()};${rounded2.toString()})"
  }
}
