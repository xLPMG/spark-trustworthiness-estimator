package me.lpmg.ste.time

import scala.collection.mutable

/**
 * A simple utility to measure the runtime of code blocks.
 * Usage:
 * {{{
 * Watch.start("myId")
 * // Code block to measure
 * println(Watch.stopFormatted("myId"))
 * }}}
 */
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

  def stopFormatted(id: String, shortenUnits: Boolean = true): String = {
    val elapsed = stop(id)
    val seconds = elapsed / 1000
    val minutes = seconds / 60
    val hours = minutes / 60
    val days = hours / 24

    val remainingSeconds = seconds % 60
    val remainingMinutes = minutes % 60
    val remainingHours = hours % 24

    if(shortenUnits){
      if (days > 0)
        f"$days%dd, $remainingHours%02dh, $remainingMinutes%02dm, $remainingSeconds%02ds"
      else if (hours > 0)
        f"$remainingHours%dh, $remainingMinutes%02dm, $remainingSeconds%02ds"
      else if (minutes > 0)
        f"$remainingMinutes%dm, $remainingSeconds%02ds"
      else
        f"$seconds%ds"
    }else{
    if (days > 0)
      f"$days%d days, $remainingHours%02d hours, $remainingMinutes%02d minutes, $remainingSeconds%02d seconds"
    else if (hours > 0)
      f"$remainingHours%d hours, $remainingMinutes%02d minutes, $remainingSeconds%02d seconds"
    else if (minutes > 0)
      f"$remainingMinutes%d minutes, $remainingSeconds%02d seconds"
    else
      f"$seconds%d seconds"
    }
  }
}
