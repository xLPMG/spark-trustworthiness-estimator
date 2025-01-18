package me.lpmg.ste.algorithms

import me.lpmg.ste.types.TemplateProbabilityVector

object ProbabilityHandler {

  def multiplicativeCombination(
      probs: Seq[TemplateProbabilityVector]
  ): TemplateProbabilityVector = {
    // Compute the product of probabilities for "Added" and "Removed"
    val prodAdd = probs.map(_.probabilityTemplateAdded).product
    val prodRemove = probs.map(_.probabilityTemplateRemoved).product

    // Normalize the results
    val sumProds = prodAdd + prodRemove
    TemplateProbabilityVector(
      prodAdd / sumProds,
      prodRemove / sumProds
    )
  }

  def logarithmicCombination(
      probs: Seq[TemplateProbabilityVector]
  ): TemplateProbabilityVector = {

    // Handle edge case: Empty input
    if (probs.isEmpty) {
      throw new IllegalArgumentException("Input sequence is empty")
    }

    // Compute the max log probabilities for numerical stability
    val maxLogAdd = probs.map(p => math.log(p.probabilityTemplateAdded)).max
    val maxLogRemove =
      probs.map(p => math.log(p.probabilityTemplateRemoved)).max

    // Compute stable log-space sum
    val logAdd = maxLogAdd + math.log(
      probs
        .map(p => math.exp(math.log(p.probabilityTemplateAdded) - maxLogAdd))
        .sum
    )
    val logRemove = maxLogRemove + math.log(
      probs
        .map(p =>
          math.exp(math.log(p.probabilityTemplateRemoved) - maxLogRemove)
        )
        .sum
    )

    // Convert back to normal space
    val expAdd = math.exp(logAdd).toFloat
    val expRemove = math.exp(logRemove).toFloat
    val sumExp = expAdd + expRemove

    // Handle edge case: sumExp is zero
    if (sumExp == 0.0f) {
      // Default to a uniform distribution to avoid NaN
      return TemplateProbabilityVector(0.5f, 0.5f)
    }

    // Normalize and return
    TemplateProbabilityVector(
      expAdd / sumExp,
      expRemove / sumExp
    )
  }

// Softmax function for a sequence of floats
  def softmax(values: Seq[Float], temperature: Float): Seq[Float] = {
    val scaledValues = values.map(_ / temperature)
    val expValues = scaledValues.map(v => Math.exp(v.toDouble).toFloat)
    val sumExp = expValues.sum
    if (sumExp == 0) {
      Seq.fill(values.length)(0.0f)
    } else {
      expValues.map(_ / sumExp)
    }
  }

  def softmaxAggregation(
      vectors: Seq[TemplateProbabilityVector],
      temperature: Float = 1.0f // Default temperature for softmax
  ): TemplateProbabilityVector = {
    // Extract probabilities for each event
    val addProbs = vectors.map(_.probabilityTemplateAdded)
    val removeProbs = vectors.map(_.probabilityTemplateRemoved)

    // Apply softmax to each dimension
    val softmaxAdd = softmax(addProbs, temperature)
    val softmaxRemove = softmax(removeProbs, temperature)

    // Sum the probabilities for the combined vector
    val combinedAdd = softmaxAdd.sum
    val combinedRemove = softmaxRemove.sum

    // Normalize to ensure the probabilities sum to 1
    val total = combinedAdd + combinedRemove
    if (total == 0) {
      TemplateProbabilityVector(0.0f, 0.0f)
    } else {
      TemplateProbabilityVector(
        probabilityTemplateAdded = combinedAdd / total,
        probabilityTemplateRemoved = combinedRemove / total
      )
    }
  }
}
