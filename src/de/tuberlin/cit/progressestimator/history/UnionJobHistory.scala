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
import java.io.File

/**
 * This history combines the data of a set of job histories.
 * It loads up Database JobHistories according to the configuration file.
 */
class UnionJobHistory() extends JobHistory {

  var histories: ListBuffer[JobHistory] = new ListBuffer[JobHistory]
  loadFromConfig()

  /**
   * load Database JobHistories according to the configuration file.
   * The db.union.urls key contains the urls of the databases to load.
   * If a url points to a directory on the disk, then all databases in that
   * directory are loaded.
   */
  private def loadFromConfig() = {
    println("Build UnionJobHistory")
    val conf = ConfigFactory.load()
    var user = conf.getString("db.union.user")
    var pass = conf.getString("db.union.pass")
    if (user.equals("")) user = null
    if (pass.equals("")) pass = null

    var urls = conf.getStringList("db.union.urls").toArray()
    for (url <- urls) {
      val f = new File(url.asInstanceOf[String])
      if (f.isDirectory()) {
        f.listFiles().foreach(s => {
          if (s.getName.endsWith(".mv.db")) {
            val u = "jdbc:h2:" + s.getAbsolutePath.substring(0, s.getAbsolutePath.length() - 6) + ";CACHE_SIZE=131072;PAGE_SIZE=8192"
            val h = new DBJobHistory(u, user, pass)
            histories += h
          }
        })
      } else {
        val h = new DBJobHistory(url.asInstanceOf[String], user, pass)
        histories += h
      }
    }
  }

  /**
   * see [[de.tuberlin.cit.progressestimator.history.JobHistory]]
   */
  override def getHistoryId(): String = {
    "UnionHistory[" + histories.length + "]"
  }
  /**
   * removes duplicate job executions from the given list.
   */
  def distinctList(l: ListBuffer[JobExecution]): ListBuffer[JobExecution] = {
    val n = new ListBuffer[JobExecution]()
    n ++= l.groupBy(_.id).map(t => t._2(0))
    n
  }

  /**
   * see [[de.tuberlin.cit.progressestimator.history.JobHistory]]
   */
  override def getJobExecutions(): ListBuffer[JobExecution] = {
    distinctList(histories.flatMap(_.getJobExecutions))
  }

  /**
   * see [[de.tuberlin.cit.progressestimator.history.JobHistory]]
   */
  override def getJobExecutions(name: String): ListBuffer[JobExecution] = {
    distinctList(histories.flatMap(_.getJobExecutions(name)))
  }

  /**
   * see [[de.tuberlin.cit.progressestimator.history.JobHistory]]
   */
  override def getJobExecutionById(id: Int): JobExecution = {
    for (h <- histories) {
      val job = h.getJobExecutionById(id)
      if (job != null) {
        return job
      }
    }
    null
  }

  /**
   * see [[de.tuberlin.cit.progressestimator.history.JobHistory]]
   */
  override def getDstatEntries(id: Int): DstatEntries = {
    for (h <- histories) {
      val job = h.getJobExecutionById(id)
      if (job != null) {
        return h.getDstatEntries(id)
      }
    }
    return new DstatEntries(Array[DstatEntry]())
  }

  /**
   * see [[de.tuberlin.cit.progressestimator.history.JobHistory]]
   */
  override def getDstatAvg(id: Int, property: String): List[(String, Double)] = {
    for (h <- histories) {
      val job = h.getJobExecutionById(id)
      if (job != null) {
        return h.getDstatAvg(id, property)
      }
    }
    return List[(String, Double)]()
  }

  /**
   * see [[de.tuberlin.cit.progressestimator.history.JobHistory]]
   */
  override def getDstatAvg(id: Int, property: String, time: Instant): List[(String, Double)] = {
    for (h <- histories) {
      val job = h.getJobExecutionById(id)
      if (job != null) {
        return h.getDstatAvg(id, property, time)
      }
    }
    return List[(String, Double)]()
  }

  /**
   * see [[de.tuberlin.cit.progressestimator.history.JobHistory]]
   */
  override def getDstatAvg(id: Int, property: String, timeStart: Instant, timeEnd: Instant): List[(String, Double)] = {
    for (h <- histories) {
      val job = h.getJobExecutionById(id)
      if (job != null) {
        return h.getDstatAvg(id, property, timeStart, timeEnd)
      }
    }
    return List[(String, Double)]()
  }

  /**
   * see [[de.tuberlin.cit.progressestimator.history.JobHistory]]
   */
  override def getDstatAvgOfValueExpression(id: Int, property1: String, property2: String, operator: String, timeStart: Instant, timeEnd: Instant): List[(String, Double)] = {
    for (h <- histories) {
      val job = h.getJobExecutionById(id)
      if (job != null) {
        return h.getDstatAvgOfValueExpression(id, property1, property2, operator, timeStart, timeEnd)
      }
    }
    return List[(String, Double)]()
  }
}