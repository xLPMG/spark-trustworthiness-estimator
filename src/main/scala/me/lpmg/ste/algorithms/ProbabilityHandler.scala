package me.lpmg.ste.algorithms

import me.lpmg.ste.types.TemplateProbabilityVector

object ProbabilityHandler {

  val NonsenseVector = TemplateProbabilityVector(-0.5f, 1.5f)

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
    val maxLogAdd = probs
      .map(p => math.log(p.probabilityTemplateAdded))
      .reduceOption(_ max _)
      .getOrElse(Double.NegativeInfinity)
    val maxLogRemove =
      probs
        .map(p => math.log(p.probabilityTemplateRemoved))
        .reduceOption(_ max _)
        .getOrElse(Double.NegativeInfinity)

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

    // Check for negative or NaN values in probs
    if (
      probs.exists(p =>
        p.probabilityTemplateAdded < 0 || p.probabilityTemplateAdded.isNaN || p.probabilityTemplateRemoved < 0 || p.probabilityTemplateRemoved.isNaN
      )
    ) {
      throw new IllegalArgumentException(
        "Probabilities contain negative or NaN values"
      )
    }

    // Check for negative or NaN values in weights
    if (weights.exists(w => w < 0 || w.isNaN)) {
      throw new IllegalArgumentException(
        "Weights contain negative or NaN values"
      )
    }

    if (probs.forall(_.probabilityTemplateAdded >= 0.999999f)) {
      return TemplateProbabilityVector(1.0f, 0.0f)
    } else if (probs.forall(_.probabilityTemplateRemoved >= 0.999999f)) {
      return TemplateProbabilityVector(0.0f, 1.0f)
    }

    val totalWeight = weights.sum
    if (totalWeight < 0.001) {
      return TemplateProbabilityVector(0.5f, 0.5f)
    }
    val scaledWeights = weights.map(_ / totalWeight)

    val probabilityTemplateAdded = probs
      .zip(scaledWeights)
      .map(p => p._1.probabilityTemplateAdded * p._2)
      .sum
    val probabilityTemplateRemoved = 1.0f - probabilityTemplateAdded

    TemplateProbabilityVector(
      probabilityTemplateAdded,
      probabilityTemplateRemoved
    )
  }

    def weightedCombinationNoUnsureResults(
      probsWithOccurences: Seq[(TemplateProbabilityVector, Int)]
  ): TemplateProbabilityVector = {

    // Handle edge case: Empty input
    if (probsWithOccurences.isEmpty) {
      throw new IllegalArgumentException("Input sequence is empty")
    }

    val numProbsWithoutEvaluation= probsWithOccurences.filter(_._2 < 1).size
    // there should be max 10% of probs without evaluation
    if (numProbsWithoutEvaluation > 0) {
      return NonsenseVector
    }

    // at least 3 "expert opinions"
    val probsWithOccurencesFiltered = probsWithOccurences.filter(_._2 >= 1)
    if (probsWithOccurencesFiltered.size < 3) {
      return NonsenseVector
    }

    val probs = probsWithOccurencesFiltered.map(_._1)
    val weights = probsWithOccurencesFiltered.map(_._2).map(occurencesToWeight)

    // Check for negative or NaN values in probs
    if (
      probs.exists(p =>
        p.probabilityTemplateAdded < 0 || p.probabilityTemplateAdded.isNaN || p.probabilityTemplateRemoved < 0 || p.probabilityTemplateRemoved.isNaN
      )
    ) {
      throw new IllegalArgumentException(
        "Probabilities contain negative or NaN values"
      )
    }

    // Check for negative or NaN values in weights
    if (weights.exists(w => w < 0 || w.isNaN)) {
      throw new IllegalArgumentException(
        "Weights contain negative or NaN values"
      )
    }

    if (probs.forall(_.probabilityTemplateAdded >= 0.999999f)) {
      return TemplateProbabilityVector(1.0f, 0.0f)
    } else if (probs.forall(_.probabilityTemplateRemoved >= 0.999999f)) {
      return TemplateProbabilityVector(0.0f, 1.0f)
    }

    val totalWeight = weights.sum
    if (totalWeight < 0.001) {
      return NonsenseVector
    }
    val scaledWeights = weights.map(_ / totalWeight)

    val probabilityTemplateAdded = probs
      .zip(scaledWeights)
      .map(p => p._1.probabilityTemplateAdded * p._2)
      .sum
    val probabilityTemplateRemoved = 1.0f - probabilityTemplateAdded

    TemplateProbabilityVector(
      probabilityTemplateAdded,
      probabilityTemplateRemoved
    )
  }

  def occurencesToWeight(occurences: Int): Float = {
    val max = 100
    val steepness = 0.05f

    val clampedOccurences = math.min(occurences, max)

    max - max * math
      .pow(Math.E.toFloat, -steepness * clampedOccurences.toFloat)
      .toFloat
  }
}
