package me.lpmg.ste.types

import org.apache.spark.util.collection.BitSet

object Templates {

  def escapeTemplates(templateBitPositions: Map[String, Byte]): Map[String, Byte] = {
    templateBitPositions.map {
      case (template, position) =>
        val escapedTemplate = template.toLowerCase.replace(" ", "-")
        escapedTemplate -> position
    }
  }
}
