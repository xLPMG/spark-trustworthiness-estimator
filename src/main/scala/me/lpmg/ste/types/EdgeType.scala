package me.lpmg.ste.types

/** Mappinf of edge types to byte values */
final object EdgeType {
  final val isParentOf: Byte = 0.toByte
  final val isChildOf: Byte = 1.toByte
  final val linksTo: Byte = 2.toByte
  final val linkedFrom: Byte = 3.toByte
}
