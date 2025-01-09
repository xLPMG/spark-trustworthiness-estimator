package me.lpmg.ste.algorithms

import me.lpmg.ste.types.TemplateProbabilityVector

object ProbabilityHandler {

  def naiveBayesProduct(
      probs: Seq[TemplateProbabilityVector]
  ): TemplateProbabilityVector = {
    val prodAdd = probs.map(_.probabilityTemplateAdded).product
    val prodRemove = probs.map(_.probabilityTemplateRemoved).product
    val prodUnchanged = probs.map(_.probabilityTemplateUnchanged).product
    val sumProds = prodAdd + prodRemove + prodUnchanged
    TemplateProbabilityVector(
      prodAdd / sumProds,
      prodRemove / sumProds,
      prodUnchanged / sumProds
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
    val unchangedProbs = vectors.map(_.probabilityTemplateUnchanged)

    // Apply softmax to each dimension
    val softmaxAdd = softmax(addProbs, temperature)
    val softmaxRemove = softmax(removeProbs, temperature)
    val softmaxUnchanged = softmax(unchangedProbs, temperature)

    // Sum the probabilities for the combined vector
    val combinedAdd = softmaxAdd.sum
    val combinedRemove = softmaxRemove.sum
    val combinedUnchanged = softmaxUnchanged.sum

    // Normalize to ensure the probabilities sum to 1
    val total = combinedAdd + combinedRemove + combinedUnchanged
    if (total == 0) {
      TemplateProbabilityVector(0.0f, 0.0f, 0.0f)
    } else {
      TemplateProbabilityVector(
        probabilityTemplateAdded = combinedAdd / total,
        probabilityTemplateRemoved = combinedRemove / total,
        probabilityTemplateUnchanged = combinedUnchanged / total
      )
    }
  }
}
