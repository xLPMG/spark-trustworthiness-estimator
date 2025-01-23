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

    if (probs.forall(_.probabilityTemplateAdded >= 0.999999f)) {
      return TemplateProbabilityVector(1.0f, 0.0f)
    } else if (probs.forall(_.probabilityTemplateRemoved >= 0.999999f)) {
      return TemplateProbabilityVector(0.0f, 1.0f)
    }

    // Compute the max log probabilities for numerical stability
    val maxLogAdd = probs.map(p => math.log(p.probabilityTemplateAdded)).reduceOption(_ max _).getOrElse(Double.NegativeInfinity)
    val maxLogRemove =
      probs.map(p => math.log(p.probabilityTemplateRemoved)).reduceOption(_ max _).getOrElse(Double.NegativeInfinity)

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
    if (sumExp <= 0.00001f) {
      // Default to a uniform distribution to avoid NaN
      return TemplateProbabilityVector(0.5f, 0.5f)
    }

    // Normalize and return
    TemplateProbabilityVector(
      expAdd / sumExp,
      expRemove / sumExp
    )
  }

  def weightedCombination(
      probsWithOccurences: Seq[(TemplateProbabilityVector, Int)]
  ): TemplateProbabilityVector = {

    // Handle edge case: Empty input
    if (probsWithOccurences.isEmpty) {
      throw new IllegalArgumentException("Input sequence is empty")
    }

    val probs = probsWithOccurences.map(_._1)
    val weights = probsWithOccurences.map(_._2).map(occurencesToWeight)

    if (probs.forall(_.probabilityTemplateAdded >= 0.999999f)) {
      return TemplateProbabilityVector(1.0f, 0.0f)
    } else if (probs.forall(_.probabilityTemplateRemoved >= 0.999999f)) {
      return TemplateProbabilityVector(0.0f, 1.0f)
    }

    val totalWeight = weights.sum
    val scaledWeights = weights.map(_ / totalWeight)

    val probabilityTemplateAdded = probs.zip(scaledWeights).map(p => p._1.probabilityTemplateAdded * p._2).sum
    val probabilityTemplateRemoved = 1.0f - probabilityTemplateAdded

    TemplateProbabilityVector(probabilityTemplateAdded, probabilityTemplateRemoved)
  }

  def occurencesToWeight(occurences: Int): Float = {
    val max = 20
    val steepness = 0.1f

    val clampedOccurences = math.min(occurences, max)
    
    max - (max - clampedOccurences) * math.pow(Math.E.toFloat, -steepness * clampedOccurences.toFloat).toFloat
  }
}
