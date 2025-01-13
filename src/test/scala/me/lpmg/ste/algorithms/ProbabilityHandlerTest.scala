package me.lpmg.ste.algorithms

import me.lpmg.ste.types.TemplateProbabilityVector

class ProbabilityHandlerTest extends munit.FunSuite {
  test("testNaiveBayesProduct") {

    val testProbs_1 = Seq(
      TemplateProbabilityVector(0.5f, 0.5f),
      TemplateProbabilityVector(0.5f, 0.5f),
      TemplateProbabilityVector(0.5f, 0.5f),
    )
    assertEquals(ProbabilityHandler.naiveBayesProduct(testProbs_1), TemplateProbabilityVector(0.5f, 0.5f))

    val testProbs_2 = Seq(
      TemplateProbabilityVector(0.5f, 0.5f),
      TemplateProbabilityVector(0.5f, 0.5f),
      TemplateProbabilityVector(0.4f, 0.6f),
    )
    assertEquals(ProbabilityHandler.naiveBayesProduct(testProbs_2), TemplateProbabilityVector(0.4f, 0.6f))

    val testProbs_3 = Seq(
      TemplateProbabilityVector(0.4f, 0.6f),
      TemplateProbabilityVector(0.6f, 0.4f)
    )
    assertEquals(ProbabilityHandler.naiveBayesProduct(testProbs_3), TemplateProbabilityVector(0.5f, 0.5f))

  }
  
}
