package me.lpmg.ste.data

/**
 * A case class representing a vector of probabilities for template addition and removal.
 *
 * @param probabilityTemplateAdded Probability that a template is added.
 * @param probabilityTemplateRemoved Probability that a template is removed.
 */
final case class TemplateProbabilityVector(
    probabilityTemplateAdded: Float,
    probabilityTemplateRemoved: Float
) extends Serializable {

  /**
   * Checks if the probabilities are undecided within a tolerance.
   *
   * @return True if the probabilities are within the tolerance, false otherwise.
   */
  def isUndecided(): Boolean = {
    val tolerance = 0.0001f
    Math.abs(probabilityTemplateAdded - probabilityTemplateRemoved) < tolerance
  }

  /**
   * Extracts the probability values as a tuple.
   *
   * @return A tuple containing the probabilities.
   */
  def extractValues(): (Float, Float) = {
    (probabilityTemplateAdded, probabilityTemplateRemoved)
  }

  /**
   * Extracts the probability values as a tuple of strings, rounded to 4 decimal places.
   *
   * @return A tuple containing the rounded probabilities as strings.
   */
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

  /**
   * Converts the probability vector to a string representation.
   *
   * @return A string representation of the probability vector.
   */
  override def toString(): String = {
    extractValuesString._1 + ";" + extractValuesString._2
  }
}

/**
 * Companion object for TemplateProbabilityVector.
 */
object TemplateProbabilityVector {
  /**
   * Creates a TemplateProbabilityVector with the given probabilities.
   *
   * @param probabilityTemplateAdded Probability that a template is added.
   * @param probabilityTemplateRemoved Probability that a template is removed.
   * @return A new TemplateProbabilityVector instance.
   */
  def apply(probabilityTemplateAdded: Float, probabilityTemplateRemoved: Float): TemplateProbabilityVector = {
    new TemplateProbabilityVector(probabilityTemplateAdded, probabilityTemplateRemoved)
  }

  /**
   * Creates a TemplateProbabilityVector with the given probability for template addition.
   * The probability for template removal is set to 1.0 minus the given probability.
   *
   * @param probabilityTemplateAdded Probability that a template is added.
   * @return A new TemplateProbabilityVector instance.
   */
  def apply(probabilityTemplateAdded: Float): TemplateProbabilityVector = {
    new TemplateProbabilityVector(probabilityTemplateAdded, 1.0f - probabilityTemplateAdded)
  }
}
