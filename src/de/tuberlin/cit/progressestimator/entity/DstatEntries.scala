package de.tuberlin.cit.progressestimator.entity
import java.time.Instant

/**
 * This represents the dstat hardware utiliation entry of an execution.
 */
class DstatEntries(val entries: Array[DstatEntry]) {

  /**
   * returns the average of all nodes' average  of the given property.
   */
  def getAvg(property: String): Double = {
    val hosts = entries.map(_.host).distinct
    val hostValues = hosts.map(h => getAvgByHost(property, h))
    return hostValues.sum / hostValues.length
  }

  /**
   * returns the average of the given property for the given host.
   */
  def getAvgByHost(property: String, host: String): Double = {
    val dstatEntries = entries.filter(e => e.host.equals(host) && e.property.equals(property))
    getAvg(dstatEntries)
  }
  /**
   * returns the maximum value of the given property for the given host.
   */
  def getMaxByHost(property: String, host: String): Double = {
    val dstatEntries = entries.filter(e => e.host.equals(host) && e.property.equals(property))
    getMax(dstatEntries)
  }
  /**
   * returns the minimum value of the given property for the given host.
   */
  def getMinByHost(property: String, host: String): Double = {
    val dstatEntries = entries.filter(e => e.host.equals(host) && e.property.equals(property))
    getMin(dstatEntries)
  }
  /**
   * helper returning the average of the given entries
   */
  private def getAvg(dstatEntries: Array[DstatEntry]): Double = {
    val values = dstatEntries.map(_.value)
    return values.sum / values.length
  }
  /**
   * helper returning the maximum of the given entries
   */
  private def getMax(dstatEntries: Array[DstatEntry]): Double = {
    dstatEntries.map(_.value).max
  }
  /**
   * helper returning the minimum of the given entries
   */
  private def getMin(dstatEntries: Array[DstatEntry]): Double = {
    dstatEntries.map(_.value).min
  }
}
