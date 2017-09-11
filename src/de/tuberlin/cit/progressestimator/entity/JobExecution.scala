
package de.tuberlin.cit.progressestimator.entity
import scala.collection.mutable.ListBuffer 
import java.time.{Instant, Duration}
import de.tuberlin.cit.progressestimator.history.JobHistory
/**
 * This represents a job execution.
 * For efficiency, Dstat entries are only loaded from the database when requested 
 * using the given job history.
 */

class JobExecution(jobHistory : JobHistory) {
  var id : Integer = -1
  var parameters : String = _
  var expName : String = _
  var hash : String = _
  var name : String = _
  var source : String = _
  
  var timeStart : Instant = _
  var timeFinish : Instant = _
  
  var iterations : ListBuffer[Iteration] = new ListBuffer[Iteration]()
  def runtime : Double = Duration.between(timeStart, timeFinish).toMillis().toDouble / 1000
  var input : DataSet = new DataSet()
  var outlier = false
  
   /**
   * true if the execution has finished
   */ 
  def finished : Boolean = {
    timeFinish != null &&
      Duration.between(timeStart, timeFinish).toMillis() > 1000
  }
  
  /**
   * returns the dstat entries of this execution.
   * The data is requested from the job history lazily for performance reasons.
   */
  def dstatEntries : DstatEntries = {
    jobHistory.getDstatEntries(id)
  }
  
  /**
   * returns the average value of all dstat values with the given property key.
   */
  def dstatAverage(property : String) : List[(String, Double)] = {
    jobHistory.getDstatAvg(id, property)
  }
  /**
   * returns the average value of all dstat values with the given property key, 
   * from the beginning of the execution up to the given time.
   */
  def dstatAverage(property : String, time : Instant) : List[(String, Double)] = {
    jobHistory.getDstatAvg(id, property, time)
  }  
  /**
   * returns the average value of all dstat values with the given property key, 
   * within the time span of the given start and end times.
   */
  def dstatAverage(property : String, timeStart : Instant, timeEnd : Instant) : List[(String, Double)] = {
    jobHistory.getDstatAvg(id, property, timeStart, timeEnd)
  }  
  /**
   * returns the average value of a dstat expression, within the time span of the
   * given start and end times. A Dstat expression consists of two properties and 
   * an operator. An operator can be either multiplication ("*") or division ("/")
   */
  def dstatAverage(property1 : String,property2 : String,operator : String, timeStart : Instant, timeEnd : Instant) : List[(String, Double)] = {
    jobHistory.getDstatAvgOfValueExpression(id, property1, property2, operator, timeStart, timeEnd)
  }
  /**
   * the average number of nodes across all iterations 
   */
  def avgNumberOfNodes : Double = {
    val nodes = iterations.map(_.resourceConfig.numberOfNodes.toDouble)
    if(nodes.size == 0) {
      0
    } else {
      nodes.sum / nodes.size
    }
  }
  override def toString() = "#" + id + " " + name
  override def equals(that : Any) : Boolean = {
    that match {
      case that: JobExecution => that.id == this.id
      case _ => false
    }
  }
}