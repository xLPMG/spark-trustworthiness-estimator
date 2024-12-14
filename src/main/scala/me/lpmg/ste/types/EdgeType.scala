package me.lpmg.ste.types

/** Mappinf of edge types to byte values */
final object EdgeType {
  final val isParentOf: Byte = 0.toByte
  final val isChildOf: Byte = 1.toByte
  final val hasSource: Byte = 3.toByte
  final val isReferencedBy: Byte = 4.toByte
}
