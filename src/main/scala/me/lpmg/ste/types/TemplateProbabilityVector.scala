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
      val rounded1 = BigDecimal(probabilityTemplateAdded).setScale(
        4,
        BigDecimal.RoundingMode.HALF_UP
      )
      val rounded2 =
        (BigDecimal(1.0) - rounded1)
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
