package me.lpmg.ste.types

/** Collector class of different custom types used in the project.
  */
object Types {
  final type DictType = Map[String, (Int, String)]

  final val TemplateBitPositions: Map[String, Byte] = Map(
    "contradict" -> 0,
    "disputed" -> 1,
    "hoax" -> 2,
    "more-citations-needed" -> 3,
    "one-source" -> 4,
    "original-research" -> 5,
    "pov" -> 6,
    "third-party" -> 7,
    "unreliable-sources" -> 8
  )
}
