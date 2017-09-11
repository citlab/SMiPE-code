

package de.tuberlin.cit.experiments.statistics
import java.security.MessageDigest
import com.typesafe.config.ConfigFactory
import java.time.Instant
import java.util.Date
import java.nio.file.{ Files, Paths, Path }
import java.io.File
import scala.util.matching.Regex
import scala.io.Source
import scala.collection.mutable.ListBuffer
import de.tuberlin.cit.progressestimator.history.db.DB
import java.sql.Connection
import de.tuberlin.cit.progressestimator.entity.JobExecution
import de.tuberlin.cit.progressestimator.history.DBJobHistory
import de.tuberlin.cit.progressestimator.entity.Iteration
import de.tuberlin.cit.progressestimator.entity.DstatEntry

/**
 * The Statistics Collector receives statistics information about a job execution
 * and stores it to the Database.
 * This simple implementation can be used for only one job concurrently.
 */
class StatisticsCollector() {

  val conn = connectToDb()

  var job: JobExecution = null
  var iteration: Iteration = null
  
  private def connectToDb(): Connection = {

    val conf = ConfigFactory.load()
    val user = if (conf.hasPath("db.user") && !conf.getString("db.user").equals("")) {
      conf.getString("db.user")
    } else { null }
    val pass = if (conf.hasPath("db.pass") && !conf.getString("db.pass").equals("")) {
      conf.getString("db.pass")
    } else { null }
    val url = if (conf.hasPath("db.url")) {
      conf.getString("db.url")
    } else { null }
    val conn = DB.getConnection(url, user, pass)
    DB.createSchema(conn)
    conn
  }
  def md5Hash(text: String): String = java.security.MessageDigest.getInstance("MD5").digest(text.getBytes()).map(0xFF & _).map { "%02x".format(_) }.foldLeft("") { _ + _ }

  /**
   * Starts a job with the given data and returns the job id
   * The job is not inserted into the DB until endJob() is called.
   */
  def startJob(name: String, experimentName: String, parameters: String, inputFile: String, inputName: String, inputSize: Long, inputRecords: Long): Int = {
    val hashString = name
    val hash = md5Hash(hashString)
    var job = new JobExecution(null)
    job.name = name
    job.hash = hash
    job.expName = experimentName
    job.parameters = parameters
    job.input.file = inputFile
    job.input.name = inputName
    job.input.size = inputSize
    job.input.numberOfRecords = inputRecords
    val now = Instant.now()
    job.timeStart = now
    job.timeFinish = now

    job.id
  }
  /**
   * Starts an iteration for the given job with the given data.
   * The iteration is not inserted into the DB until endIteration() is called.
   */
  def startIteration(jobId: Integer, nr: Integer, numberOfNodes: Integer, inputSize: Long, inputRecords: Long): Unit = {
    iteration = new Iteration()
    iteration.input.numberOfRecords = inputRecords
    iteration.input.size = inputSize
    iteration.resourceConfig.numberOfNodes = numberOfNodes
    iteration.nr = nr
    iteration.timeStart = Instant.now()
  }
  /**
   * ends the given interation and inserts it into the DB.
   */
  def endIteration(jobId: Integer, nr: Integer): Unit = {
    if (nr == iteration.nr) {
      iteration.timeFinish = Instant.now()
      DBJobHistory.insertIteration(jobId, iteration, conn)
    } else {
      log("Error - could not finish iteration " + nr);
    }
  }
  /**
   * ends the given job and inserts it into the DB.
   */
  def endJob(jobId: Int): Unit = {
    if (jobId == job.id) {
      job.timeFinish = Instant.now()
      DBJobHistory.insertExecution(job, conn)
    } else {
      log("Error - could not finish job " + jobId);
    }
  }

  /**
   * inserts the dstat log entry for the job with the given id into the DB.
   */
  def insertDstatLog(jobId : Int, time : Instant, host: String, property : String, value : Double) = {
    val dstat = new DstatEntry
    dstat.time = time
    dstat.host = host
    dstat.property = property
    dstat.value = value
    DBJobHistory.insertDstatEntry(jobId, dstat, conn)
  }
  private def log(msg: String): Unit = {
    System.out.println(msg)
  }

}