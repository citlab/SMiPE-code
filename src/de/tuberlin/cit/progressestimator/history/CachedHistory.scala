package de.tuberlin.cit.progressestimator.history

import de.tuberlin.cit.progressestimator.entity.{ Iteration, JobExecution, DstatEntry }
import java.rmi._
import java.rmi.server._
import java.time.Instant
import de.tuberlin.cit.progressestimator.history.db.DB
import java.util.Date
import java.text.SimpleDateFormat
import scala.util.matching.Regex
import scala.collection.mutable.ListBuffer
import com.typesafe.config.ConfigFactory
import java.sql.{ Connection, DriverManager }
import anorm.SqlParser._
import anorm._
import de.tuberlin.cit.progressestimator.entity.JobExecution
import de.tuberlin.cit.progressestimator.entity.Iteration
import scala.language.postfixOps
import de.tuberlin.cit.progressestimator.entity.DstatEntries
import com.google.inject.Singleton
import com.google.inject.Inject
import de.tuberlin.cit.progressestimator.estimator.EstimatorConfig

/**
 * This job history caches job executions in the memory for performance reasons.
 * The data of executions and iterations is cached when the object is created from a file on disk.
 * Dstat data is not cached.
 */
@Singleton
class CachedHistory @Inject() (val config: EstimatorConfig) extends JobHistory {
  println("Build CachedHistory")

  private val wrappedHistory = new NoOutlierHistory(config.jobHistory)
  val cache = wrappedHistory.getJobExecutions()

  /**
   * see [[de.tuberlin.cit.progressestimator.history.JobHistory]]
   */
  override def getHistoryId(): String = wrappedHistory.getHistoryId()

  /**
   * see [[de.tuberlin.cit.progressestimator.history.JobHistory]]
   */
  override def getJobExecutions(): ListBuffer[JobExecution] = {
    cache.clone()
  }

  /**
   * see [[de.tuberlin.cit.progressestimator.history.JobHistory]]
   */
  override def getJobExecutions(name: String): ListBuffer[JobExecution] = {
    cache.filter(_.name.equals(name)).clone()
  }

  /**
   * see [[de.tuberlin.cit.progressestimator.history.JobHistory]]
   */
  override def getJobExecutionById(id: Int): JobExecution = {
    val list = cache.filter(_.id == id)
    if (list.length > 0) {
      list.apply(0)
    } else {
      null
    }
  }
  
  /**
   * see [[de.tuberlin.cit.progressestimator.history.JobHistory]]
   */
  override def getDstatEntries(id: Int): DstatEntries = {
    wrappedHistory.getDstatEntries(id)
  }

  /**
   * see [[de.tuberlin.cit.progressestimator.history.JobHistory]]
   */
  override def getDstatAvg(id: Int, property: String): List[(String, Double)] = {
    wrappedHistory.getDstatAvg(id, property)
  }
  
  /**
   * see [[de.tuberlin.cit.progressestimator.history.JobHistory]]
   */
  override def getDstatAvg(id: Int, property: String, time: Instant): List[(String, Double)] = {
    wrappedHistory.getDstatAvg(id, property, time)
  }
  
  /**
   * see [[de.tuberlin.cit.progressestimator.history.JobHistory]]
   */
  override def getDstatAvg(id: Int, property: String, timeStart: Instant, timeEnd: Instant): List[(String, Double)] = {
    wrappedHistory.getDstatAvg(id, property, timeStart, timeEnd)
  }

  /**
   * see [[de.tuberlin.cit.progressestimator.history.JobHistory]]
   */
  override def getDstatAvgOfValueExpression(id: Int, property1: String, property2: String, operator: String, timeStart: Instant, timeEnd: Instant): List[(String, Double)] = {
    wrappedHistory.getDstatAvgOfValueExpression(id, property1, property2, operator, timeStart, timeEnd)
  }

  /**
   * returns all job executions in the history, including outliers.
   * This may not be used for the estimation, but for information,
   * for example for a complete list of executions.
   */
  def getJobExecutionsIncludingOutlier(): ListBuffer[JobExecution] = {
    wrappedHistory.getJobExecutionsIncludingOutlier()
  }
}