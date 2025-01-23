package me.lpmg.ste.algorithms

import me.lpmg.ste.types.TemplateProbabilityVector

class ProbabilityHandlerTest extends munit.FunSuite {
  test("testMultiplicativeCombination") {

    val testProbs_1 = Seq(
      TemplateProbabilityVector(0.5f, 0.5f),
      TemplateProbabilityVector(0.5f, 0.5f),
      TemplateProbabilityVector(0.5f, 0.5f)
    )
    assertEqualsFloat(
      ProbabilityHandler
        .multiplicativeCombination(testProbs_1)
        .probabilityTemplateAdded,
      0.5f,
      0.0001f
    )
    assertEqualsFloat(
      ProbabilityHandler
        .multiplicativeCombination(testProbs_1)
        .probabilityTemplateRemoved,
      0.5f,
      0.0001f
    )

    val testProbs_2 = Seq(
      TemplateProbabilityVector(0.5f, 0.5f),
      TemplateProbabilityVector(0.5f, 0.5f),
      TemplateProbabilityVector(0.4f, 0.6f)
    )
    assertEqualsFloat(
      ProbabilityHandler
        .multiplicativeCombination(testProbs_2)
        .probabilityTemplateAdded,
      0.4f,
      0.0001f
    )
    assertEqualsFloat(
      ProbabilityHandler
        .multiplicativeCombination(testProbs_2)
        .probabilityTemplateRemoved,
      0.6f,
      0.0001f
    )

    val testProbs_3 = Seq(
      TemplateProbabilityVector(0.4f, 0.6f),
      TemplateProbabilityVector(0.6f, 0.4f)
    )
    assertEqualsFloat(
      ProbabilityHandler
        .multiplicativeCombination(testProbs_3)
        .probabilityTemplateAdded,
      0.5f,
      0.0001f
    )
    assertEqualsFloat(
      ProbabilityHandler
        .multiplicativeCombination(testProbs_3)
        .probabilityTemplateRemoved,
      0.5f,
      0.0001f
    )

    val testProbs_4 = Seq(
      TemplateProbabilityVector(0.0f, 1.0f),
      TemplateProbabilityVector(0.0f, 1.0f)
    )
    assertEqualsFloat(
      ProbabilityHandler
        .multiplicativeCombination(testProbs_4)
        .probabilityTemplateAdded,
      0.0f,
      0.0001f
    )
    assertEqualsFloat(
      ProbabilityHandler
        .multiplicativeCombination(testProbs_4)
        .probabilityTemplateRemoved,
      1.0f,
      0.0001f
    )
  }

  test("testLogarithmicCombination") {

    val testProbs_1 = Seq(
      TemplateProbabilityVector(0.5f, 0.5f),
      TemplateProbabilityVector(0.5f, 0.5f),
      TemplateProbabilityVector(0.5f, 0.5f)
    )
    assertEqualsFloat(
      ProbabilityHandler
        .logarithmicCombination(testProbs_1)
        .probabilityTemplateAdded,
      0.5f,
      0.0001f
    )
    assertEqualsFloat(
      ProbabilityHandler
        .logarithmicCombination(testProbs_1)
        .probabilityTemplateRemoved,
      0.5f,
      0.0001f
    )

    val testProbs_2 = Seq(
      TemplateProbabilityVector(0.5f, 0.5f),
      TemplateProbabilityVector(0.5f, 0.5f),
      TemplateProbabilityVector(0.4f, 0.6f)
    )
    assertEqualsFloat(
      ProbabilityHandler
        .logarithmicCombination(testProbs_2)
        .probabilityTemplateAdded,
      0.46666667f,
      0.0001f
    )
    assertEqualsFloat(
      ProbabilityHandler
        .logarithmicCombination(testProbs_2)
        .probabilityTemplateRemoved,
      0.53333336f,
      0.0001f
    )

    val testProbs_3 = Seq(
      TemplateProbabilityVector(0.4f, 0.6f),
      TemplateProbabilityVector(0.6f, 0.4f)
    )
    assertEqualsFloat(
      ProbabilityHandler
        .logarithmicCombination(testProbs_3)
        .probabilityTemplateAdded,
      0.5f,
      0.0001f
    )
    assertEqualsFloat(
      ProbabilityHandler
        .logarithmicCombination(testProbs_3)
        .probabilityTemplateRemoved,
      0.5f,
      0.0001f
    )

    val testProbs_4 = Seq(
      TemplateProbabilityVector(0.0f, 1.0f),
      TemplateProbabilityVector(0.0f, 1.0f)
    )
    assertEqualsFloat(
      ProbabilityHandler
        .logarithmicCombination(testProbs_4)
        .probabilityTemplateAdded,
      0.0f,
      0.0001f
    )
    assertEqualsFloat(
      ProbabilityHandler
        .logarithmicCombination(testProbs_4)
        .probabilityTemplateRemoved,
      1.0f,
      0.0001f
    )
  }

  // this test is not about exact values. it is a specification on what results i expect the function to return
  test("testWeightedCombination") {

    val tolerance = 0.05f

    val testProbs_1 = Seq(
      (TemplateProbabilityVector(0.2f, 0.8f), 1),
      (TemplateProbabilityVector(0.8f, 0.2f), 20)
    )
    val result_1 = ProbabilityHandler.weightedCombination(testProbs_1)
    assert(result_1.probabilityTemplateAdded < 0.8f)
    assert(result_1.probabilityTemplateAdded > 0.7f)

    assert(result_1.probabilityTemplateRemoved > 0.2f)
    assert(result_1.probabilityTemplateRemoved < 0.3f)

    val testProbs_2 = Seq(
      (TemplateProbabilityVector(0.2f, 0.8f), 5),
      (TemplateProbabilityVector(0.8f, 0.2f), 5)
    )
    val result_2 = ProbabilityHandler.weightedCombination(testProbs_2)
    assertEqualsFloat(result_2.probabilityTemplateAdded, 0.5f, tolerance)
    assertEqualsFloat(result_2.probabilityTemplateRemoved, 0.5f, tolerance)

    val testProbs_3 = Seq(
      (TemplateProbabilityVector(0.000001f, 0.999999f), 1),
      (TemplateProbabilityVector(0.000001f, 0.999999f), 1)
    )
    val result_3 = ProbabilityHandler.weightedCombination(testProbs_3)
    assertEqualsFloat(result_3.probabilityTemplateAdded, 0.0f, tolerance)
    assertEqualsFloat(result_3.probabilityTemplateRemoved, 1.0f, tolerance)

    val testProbs_4 = Seq(
      (TemplateProbabilityVector(0.5f, 0.5f), 1),
      (TemplateProbabilityVector(0.5f, 0.5f), 1),
      (TemplateProbabilityVector(0.5f, 0.5f), 1),
      (TemplateProbabilityVector(0.5f, 0.5f), 1),
      (TemplateProbabilityVector(0.5f, 0.5f), 1),
      (TemplateProbabilityVector(0.5f, 0.5f), 1),
      (TemplateProbabilityVector(0.5f, 0.5f), 1),
      (TemplateProbabilityVector(0.5f, 0.5f), 1),
      (TemplateProbabilityVector(0.5f, 0.5f), 1),
      (TemplateProbabilityVector(0.1f, 0.9f), 1)
    )
    val result_4 = ProbabilityHandler.weightedCombination(testProbs_4)
    assertEqualsFloat(result_4.probabilityTemplateAdded, 0.5f, tolerance)
    assertEqualsFloat(result_4.probabilityTemplateRemoved, 0.5f, tolerance)
  }

}
