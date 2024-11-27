package me.lpmg.ste.types

/** Collector class of different custom types used in the project.
  */
object Types {
  final type DictType = Map[String, (Int, String)]

  final val TemplateBitPositions: Map[String, Byte] = Map(
    "Contradict" -> 0,
    "Disputed" -> 1,
    "Hoax" -> 2,
    "Unreferenced" -> 3
  )
}
