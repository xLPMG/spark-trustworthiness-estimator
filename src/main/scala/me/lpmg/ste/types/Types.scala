package me.lpmg.ste.types

import org.apache.spark.util.collection.BitSet

/** Collector class of different custom types used in the project.
  */
object Types {

  /** Map of template names to their position in the template bitset Source:
    * https://en.wikipedia.org/wiki/Wikipedia:Template_index/Cleanup
    */
  final val TemplateBitPositions: Map[String, Byte] = Map(
    "Circular" -> 0,
    "Better sources needed" -> 1,
    "Dubious" -> 2,
    "No reliable sources" -> 3,
    "Self-published" -> 4,
    "Third-party" -> 5,
    "Unreliable sources" -> 6,
    "User-generated" -> 7
  )

  def escapeTemplates(templateBitPositions: Map[String, Byte]): Map[String, Byte] = {
    templateBitPositions.map {
      case (template, position) =>
        val escapedTemplate = template.toLowerCase.replace(" ", "-")
        escapedTemplate -> position
    }
  }
}
