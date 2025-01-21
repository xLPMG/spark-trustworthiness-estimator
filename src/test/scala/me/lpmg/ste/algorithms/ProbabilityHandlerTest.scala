package me.lpmg.ste.algorithms

import me.lpmg.ste.types.TemplateProbabilityVector

class ProbabilityHandlerTest extends munit.FunSuite {
  test("testMultiplicativeCombination") {

    val testProbs_1 = Seq(
      TemplateProbabilityVector(0.5f, 0.5f),
      TemplateProbabilityVector(0.5f, 0.5f),
      TemplateProbabilityVector(0.5f, 0.5f),
    )
    assertEqualsFloat(ProbabilityHandler.multiplicativeCombination(testProbs_1).probabilityTemplateAdded, 0.5f, 0.0001f)
    assertEqualsFloat(ProbabilityHandler.multiplicativeCombination(testProbs_1).probabilityTemplateRemoved, 0.5f, 0.0001f)

    val testProbs_2 = Seq(
      TemplateProbabilityVector(0.5f, 0.5f),
      TemplateProbabilityVector(0.5f, 0.5f),
      TemplateProbabilityVector(0.4f, 0.6f),
    )
    assertEqualsFloat(ProbabilityHandler.multiplicativeCombination(testProbs_2).probabilityTemplateAdded, 0.4f, 0.0001f)
    assertEqualsFloat(ProbabilityHandler.multiplicativeCombination(testProbs_2).probabilityTemplateRemoved, 0.6f, 0.0001f)

    val testProbs_3 = Seq(
      TemplateProbabilityVector(0.4f, 0.6f),
      TemplateProbabilityVector(0.6f, 0.4f)
    )
    assertEqualsFloat(ProbabilityHandler.multiplicativeCombination(testProbs_3).probabilityTemplateAdded, 0.5f, 0.0001f)
    assertEqualsFloat(ProbabilityHandler.multiplicativeCombination(testProbs_3).probabilityTemplateRemoved, 0.5f, 0.0001f)

    val testProbs_4 = Seq(
      TemplateProbabilityVector(0.0f, 1.0f),
      TemplateProbabilityVector(0.0f, 1.0f)
    )
    assertEqualsFloat(ProbabilityHandler.multiplicativeCombination(testProbs_4).probabilityTemplateAdded, 0.0f, 0.0001f)
    assertEqualsFloat(ProbabilityHandler.multiplicativeCombination(testProbs_4).probabilityTemplateRemoved, 1.0f, 0.0001f)
  }

  test("testLogarithmicCombination") {

    val testProbs_1 = Seq(
      TemplateProbabilityVector(0.5f, 0.5f),
      TemplateProbabilityVector(0.5f, 0.5f),
      TemplateProbabilityVector(0.5f, 0.5f),
    )
    assertEqualsFloat(ProbabilityHandler.logarithmicCombination(testProbs_1).probabilityTemplateAdded, 0.5f, 0.0001f)
    assertEqualsFloat(ProbabilityHandler.logarithmicCombination(testProbs_1).probabilityTemplateRemoved, 0.5f, 0.0001f)

    val testProbs_2 = Seq(
      TemplateProbabilityVector(0.5f, 0.5f),
      TemplateProbabilityVector(0.5f, 0.5f),
      TemplateProbabilityVector(0.4f, 0.6f),
    )
    assertEqualsFloat(ProbabilityHandler.logarithmicCombination(testProbs_2).probabilityTemplateAdded, 0.46666667f, 0.0001f)
    assertEqualsFloat(ProbabilityHandler.logarithmicCombination(testProbs_2).probabilityTemplateRemoved, 0.53333336f, 0.0001f)

    val testProbs_3 = Seq(
      TemplateProbabilityVector(0.4f, 0.6f),
      TemplateProbabilityVector(0.6f, 0.4f)
    )
    assertEqualsFloat(ProbabilityHandler.logarithmicCombination(testProbs_3).probabilityTemplateAdded, 0.5f, 0.0001f)
    assertEqualsFloat(ProbabilityHandler.logarithmicCombination(testProbs_3).probabilityTemplateRemoved, 0.5f, 0.0001f)

    val testProbs_4 = Seq(
      TemplateProbabilityVector(0.0f, 1.0f),
      TemplateProbabilityVector(0.0f, 1.0f)
    )
    assertEqualsFloat(ProbabilityHandler.logarithmicCombination(testProbs_4).probabilityTemplateAdded, 0.0f, 0.0001f)
    assertEqualsFloat(ProbabilityHandler.logarithmicCombination(testProbs_4).probabilityTemplateRemoved, 1.0f, 0.0001f)
  }
  
}
