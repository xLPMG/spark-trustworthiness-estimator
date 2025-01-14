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
    // Compute the log-space sum for "Added" and "Removed"
    val logAdd = probs.map(p => math.log(p.probabilityTemplateAdded)).sum
    val logRemove = probs.map(p => math.log(p.probabilityTemplateRemoved)).sum

    // Convert back to normal space and normalize
    val expAdd = math.exp(logAdd).toFloat
    val expRemove = math.exp(logRemove).toFloat
    val sumExp = expAdd + expRemove

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
