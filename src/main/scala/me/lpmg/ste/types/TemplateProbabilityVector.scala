package me.lpmg.ste.types

final case class TemplateProbabilityVector(
    probabilityTemplateAdded: Float,
    probabilityTemplateRemoved: Float
) extends Serializable {

  def isUndecided(): Boolean = {
    val tolerance = 0.0001f
    Math.abs(probabilityTemplateAdded - probabilityTemplateRemoved) < tolerance
  }

  def extractValues(): (Float, Float) = {
    (probabilityTemplateAdded, probabilityTemplateRemoved)
  }

  def extractValuesString: (String, String) = {
    import scala.util.{Try, Success, Failure}

    Try {
      val rounded1 = BigDecimal.valueOf(probabilityTemplateAdded.toDouble).setScale(
        4,
        BigDecimal.RoundingMode.HALF_UP
      )
      val rounded2 =
        (BigDecimal.valueOf(1.0) - rounded1)
          .setScale(4, BigDecimal.RoundingMode.HALF_UP)
      (s"${rounded1.toString()}", s"${rounded2.toString()}")
    } match {
      case Success(result) => result
      case Failure(error) =>
        println(
          s"Error converting probability: ${probabilityTemplateAdded};${probabilityTemplateRemoved} with ${error.getMessage}"
        )
        ("NaN", "NaN")
    }
  }

  override def toString(): String = {
    extractValuesString._1 + ";" + extractValuesString._2
  }
}

object TemplateProbabilityVector {
  def apply(probabilityTemplateAdded: Float, probabilityTemplateRemoved: Float): TemplateProbabilityVector = {
    new TemplateProbabilityVector(probabilityTemplateAdded, probabilityTemplateRemoved)
  }

  def apply(probabilityTemplateAdded: Float): TemplateProbabilityVector = {
    new TemplateProbabilityVector(probabilityTemplateAdded, 1.0f - probabilityTemplateAdded)
  }
}
