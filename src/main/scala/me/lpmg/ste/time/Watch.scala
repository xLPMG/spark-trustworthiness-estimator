package me.lpmg.ste.time

import scala.collection.mutable

object Watch {
  // ID -> Start time
  private val timeMap: mutable.Map[String, Long] = mutable.Map()

  def start(id: String): Unit = {
    timeMap.put(id, System.currentTimeMillis())
  }

  def getRuntime(id: String): Long = {
    System.currentTimeMillis() - timeMap(id)
  }

  def stop(id: String): Long = {
    val elapsed = System.currentTimeMillis() - timeMap(id)
    timeMap.remove(id)
    elapsed
  }

  def stopFormatted(id: String): String = {
    val elapsed = stop(id)
    val minutes = (elapsed / 1000) / 60
    val seconds = ((elapsed / 1000) % 60).toInt
    s"${minutes}m ${seconds}s"
  }
}