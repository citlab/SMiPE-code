package de.tuberlin.cit.progressestimator.history

import de.tuberlin.cit.progressestimator.entity.JobExecution
import scala.collection.mutable.ListBuffer
import java.rmi._
import de.tuberlin.cit.progressestimator.entity.DstatEntry
import de.tuberlin.cit.progressestimator.entity.DstatEntries
import java.time.Instant

/**
 * The Job History trait provides functionality to access job history data.
 * 
 */
trait JobHistory {
  /**
   * returns all job executions with the given program name.
   */
  def getJobExecutions(name: String): ListBuffer[JobExecution]
  /**
   * returns all job executions in the history.
   */
  def getJobExecutions(): ListBuffer[JobExecution]
  /**
   * returns the job execution with the given id.
   * if no execution is found, null is returned.
   */
  def getJobExecutionById(id: Int): JobExecution
  /**
   * returns all Dstat Entries of the job execution with the given id.
   */
  def getDstatEntries(id: Int): DstatEntries
  /**
   * returns the average value of all dstat values with the given property key 
   * for the job execution with the given id.
   */
  def getDstatAvg(id: Int, property: String): List[(String, Double)]
  
  /**
   * returns the average value of all dstat values with the given property key 
   * for the job exection with the given id, from the beginning of the execution
   * up to the given time.
   */
  def getDstatAvg(id: Int, property: String, time : Instant): List[(String, Double)]
  /**
   * returns the average value of all dstat values with the given property key 
   * for the job exection with the given id, within the time span of the given start and end times.
   */
  def getDstatAvg(id: Int, property: String, timeStart : Instant, timeEnd : Instant): List[(String, Double)]
  
  /**
   * returns the average value of a dstat expression for the job execution with the given id, within 
   * the time span of the given start and end times.
   * A Dstat expression consists of two properties and an operator. An operator can be either multiplication ("*")
   * or division ("/")
   */
  def getDstatAvgOfValueExpression(id: Int, property1: String, property2: String, operator : String, timeStart : Instant, timeEnd : Instant): List[(String, Double)]
  
  /**
   * returns the id of this history.
   * The id is used e.g. in caches to identify different histories.
   */
  def getHistoryId() : String
}