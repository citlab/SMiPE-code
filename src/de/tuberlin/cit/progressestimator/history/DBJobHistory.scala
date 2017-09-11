package de.tuberlin.cit.progressestimator.history

import de.tuberlin.cit.progressestimator.entity.{ Iteration, JobExecution, DstatEntry }
import java.time.Instant
import de.tuberlin.cit.progressestimator.history.db.DB
import java.util.Date
import java.text.SimpleDateFormat
import scala.util.matching.Regex
import scala.collection.mutable.ListBuffer
import com.typesafe.config.ConfigFactory
import java.sql.{ Connection, DriverManager }
import java.nio.file.{ Files, Paths, Path }
import java.io.{ ObjectInputStream, FileInputStream, FileOutputStream, ObjectOutputStream, File }
import anorm.SqlParser._
import anorm._
import de.tuberlin.cit.progressestimator.entity.JobExecution
import de.tuberlin.cit.progressestimator.entity.JobExecution
import de.tuberlin.cit.progressestimator.entity.Iteration
import java.rmi._
import java.rmi.server._
import scala.language.postfixOps
import de.tuberlin.cit.progressestimator.entity.DstatEntries
import com.google.inject.Singleton

/**
 * This JobHistory accesses data from a database.
 */
@Singleton
class DBJobHistory(url: String, user: String, pass: String) extends JobHistory {

  /**
   * see [[de.tuberlin.cit.progressestimator.history.JobHistory]]
   */
  override def getHistoryId(): String = url

  var connection: Connection = null
  if (url != null) {
    connection = DB.getConnection(url, user, pass)
  }

  def this() = {
    this(null, "", "")
    val conf = ConfigFactory.load()
    var user = conf.getString("db.user")
    var pass = conf.getString("db.pass")
    if (user.equals("")) user = null
    if (pass.equals("")) pass = null
    var url = conf.getString("db.url")
    connection = DB.getConnection(url, user, pass)
  }

  /** a parser for a job row  */
  val jobRowParser = {
    get[Int]("id") ~
      get[String]("name") ~
      get[String]("hash") ~
      get[String]("input_name") ~
      get[String]("input_file") ~
      get[String]("parameters") ~
      get[String]("exp_name") ~
      get[Instant]("time_start") ~
      get[Instant]("time_finish") ~
      get[Long]("input_size") ~
      get[Long]("input_records") map {
        case id ~ name ~ hash ~ inputName ~ inputFile ~ parameters ~ expName ~ timeStart ~ timeFinish ~ inputSize ~ inputRecords => {
          var j = new JobExecution(this)
          j.id = id
          j.hash = hash
          j.name = name
          j.input.file = inputFile
          j.parameters = parameters
          j.expName = expName
          j.timeFinish = timeFinish
          j.timeStart = timeStart
          j.input.name = inputName
          j.input.size = inputSize
          j.input.numberOfRecords = inputRecords
          j.source = url
          // Fix data
          if (inputName.equals("gen-20-20M") && inputRecords == 0) {
            j.input.numberOfRecords = 20L * 20000
            j.input.size = 20L * 20000000
          } else if (inputName.equals("gen-20-10M") && inputRecords == 0) {
            j.input.numberOfRecords = 20L * 10000
            j.input.size = 20L * 10000000
          } else if (inputName.equals("gen-50-750000000") && inputRecords == 0) {
            j.input.numberOfRecords = 50L * 750000
            j.input.size = 50L * 750000000
          } else if (inputName.equals("gen-25-500000000") && inputRecords == 0) {
            j.input.numberOfRecords = 25L * 500000
            j.input.size = 25L * 500000000
          } else if (inputName.equals("gen-25-250000000") && inputRecords == 0) {
            j.input.numberOfRecords = 25L * 250000
            j.input.size = 25L * 250000000
          } else if (inputName.equals("gen-50-500000000") && inputRecords == 0) {
            j.input.numberOfRecords = 50L * 500000
            j.input.size = 50L * 500000000
          } else if (inputName.equals("gen-50-100M") && inputRecords == 0) {
            j.input.numberOfRecords = 50L * 100000
            j.input.size = 50L * 100000000
          } else if (inputName.equals("gen-20-15M") && inputRecords == 0) {
            j.input.numberOfRecords = 20L * 15000
            j.input.size = 20L * 15000000
          } else if (inputName.equals("gen-30-40M") && inputRecords == 0) {
            j.input.numberOfRecords = 30L * 40000
            j.input.size = 30L * 40000000
          } else if (inputName.equals("wikipedia_links_en") && inputRecords == 0) {
            j.input.numberOfRecords = 7549376
          } else if (inputRecords == 0) {
            val t = 0
          }
          j
        }
      }
  }

  /**
   * see [[de.tuberlin.cit.progressestimator.history.JobHistory]]
   */
  def getJobExecutions(): ListBuffer[JobExecution] = {
    getJobExecutionsHelper(SQL(
      """
            |SELECT   *
            |FROM     execution
            """.stripMargin.trim))
  }

  /**
   * see [[de.tuberlin.cit.progressestimator.history.JobHistory]]
   */
  def getJobExecutionById(id: Int): JobExecution = {
    val executions =
      getJobExecutionsHelper(SQL(
        """
          |SELECT   *
          |FROM     execution
          |WHERE    id = {id}
          """.stripMargin.trim)
        .on("id" -> id))
    if (executions.size > 0) {
      return executions(0)
    } else {
      return null
    }
  }

  /**
   * see [[de.tuberlin.cit.progressestimator.history.JobHistory]]
   */
  def getJobExecutions(name: String): ListBuffer[JobExecution] = {
    getJobExecutionsHelper(SQL(
      """
          |SELECT   *
          |FROM     execution
          |WHERE    name = {name}
          """.stripMargin.trim)
      .on("name" -> name))
  }
  /**
   * helper function for parsing job executions
   */
  def getJobExecutionsHelper(query: SimpleSql[Row]): ListBuffer[JobExecution] = {
    implicit val conn = connection
    try {
      var executions = query
        .as(jobRowParser *)
      for (j <- executions) {
        getJobIterations(j)
      }
      executions.to[ListBuffer]

    } catch {
      case e: Throwable =>
        println("Error while querying ")
        throw e
    }
  }
  /**
   * a parser for an iteration row
   */
  val iterationRowParser = {
    get[Int]("iteration_nr") ~
      get[Instant]("time_start") ~
      get[Instant]("time_finish") ~
      get[Long]("input_records") ~
      get[Long]("input_size") ~
      get[Int]("number_of_nodes") map {
        case iterationNr ~ timeStart ~ timeFinish ~ inputRecords ~ inputSize ~ numberOfNodes => {
          var i = new Iteration()
          i.nr = iterationNr
          i.timeStart = timeStart
          i.timeFinish = timeFinish
          i.input.numberOfRecords = inputRecords
          i.input.size = inputSize
          i.resourceConfig.numberOfNodes = numberOfNodes
          i
        }
      }
  }
  /**
   * a helper method to retrieve iterations for a given job execution
   */
  private def getJobIterations(job: JobExecution): Unit = {
    implicit val conn = connection
    try {
      val iterations = SQL(
        """
          |SELECT   *
          |FROM     iteration
          |WHERE    app_id = {id}
          |ORDER BY iteration_nr
          """.stripMargin.trim)
        .on("id" -> job.id)
        .as(iterationRowParser *)
      for (i <- iterations) {
        job.iterations += i
      }

    } catch {
      case e: Throwable =>
        println("Error while querying ")
        throw e
    }
  }
  /** a parser for a dstat row */
  val dstatRowParser = {
    get[String]("host") ~
      get[Instant]("time") ~
      get[String]("property") ~
      get[Double]("value") map {
        case host ~ time ~ property ~ value => {
          var d = new DstatEntry()
          d.host = host
          d.property = property
          d.time = time
          d.value = value
          d
        }
      }
  }
  /** a parser for a dstat row average */
  val avgRowParser = {
    get[Double]("avg") ~
      get[String]("host") map {
        case avg ~ host => {
          (host, avg)
        }
      }
  }

  /**
   * see [[de.tuberlin.cit.progressestimator.history.JobHistory]]
   */
  override def getDstatAvg(id: Int, property: String, time: Instant): List[(String, Double)] = {
    implicit val conn = connection
    try {
      val entries = SQL(
        """
          |SELECT   host, AVG(value) as `avg`
          |FROM     dstat_entry
          |WHERE    appId = {id}
          |          AND property LIKE {property}
          |          AND time < {time}
          |GROUP BY host
          """.stripMargin.trim)
        .on("id" -> id)
        .on("property" -> property)
        .on("time" -> time)
        .as(avgRowParser *)
      return entries
    } catch {
      case e: Throwable =>
        println("Error while querying dstat")
        throw e
    }
    return List[(String, Double)]()
  }

  /**
   * see [[de.tuberlin.cit.progressestimator.history.JobHistory]]
   */
  override def getDstatAvg(id: Int, property: String, timeStart: Instant, timeEnd: Instant): List[(String, Double)] = {
    implicit val conn = connection
    try {
      val entries = SQL(
        """
          |SELECT   host, AVG(value) as `avg`
          |FROM     dstat_entry
          |WHERE    appId = {id}
          |          AND property LIKE {property}
          |          AND time >= {timeStart}
          |          AND time <= {timeEnd}
          |GROUP BY host
          """.stripMargin.trim)
        .on("id" -> id)
        .on("property" -> property)
        .on("timeStart" -> timeStart)
        .on("timeEnd" -> timeEnd)
        .as(avgRowParser *)
      return entries
    } catch {
      case e: Throwable =>
        println("Error while querying dstat")
        throw e
    }
    return List[(String, Double)]()
  }
  /**
   * see [[de.tuberlin.cit.progressestimator.history.JobHistory]]
   */
  override def getDstatAvg(id: Int, property: String): List[(String, Double)] = {
    implicit val conn = connection
    try {
      val entries = SQL(
        """
          |SELECT   host, AVG(value) as `avg`
          |FROM     dstat_entry
          |WHERE    appId = {id}
          |          AND property LIKE {property}
          |GROUP BY host
          """.stripMargin.trim)
        .on("id" -> id)
        .on("property" -> property)
        .as(avgRowParser *)
      return entries
    } catch {
      case e: Throwable =>
        println("Error while querying dstat")
        throw e
    }
    return List[(String, Double)]()
  }
  /**
   * see [[de.tuberlin.cit.progressestimator.history.JobHistory]]
   */
  override def getDstatAvgOfValueExpression(id: Int, property1: String, property2: String, operator: String, timeStart: Instant, timeEnd: Instant): List[(String, Double)] = {
    implicit val conn = connection
    val op = if (operator == "*" || operator == "/") {
      " " + operator + " "
    } else {
      println("ERROR: unsupported operator " + operator)
      " / "
    }

    val q = if (operator == "*") {
      """
          |SELECT   a.host, AVG(a.value  *  b.value) as `avg`
          |FROM     dstat_entry a JOIN dstat_entry b ON a.time = b.time AND a.host=b.host
          |WHERE    a.appId = {idA} 
          |          AND b.appId = {idB}
          |          AND a.property LIKE {property1}
          |          AND b.property LIKE {property2}
          |          AND a.time >= {timeStartA}
          |          AND a.time <= {timeEndA}
          |          AND b.time >= {timeStartB}
          |          AND b.time <= {timeEndB}
          |GROUP BY a.host
          """
    } else {
      if (operator != "/") {
        println("ERROR: unsupported operator " + operator)
      }
      """
          |SELECT   a.host, AVG(a.value  /  nullif(b.value,0) ) as `avg`
          |FROM     dstat_entry a JOIN dstat_entry b ON a.time = b.time AND a.host=b.host
          |WHERE    a.appId = {idA} 
          |          AND b.appId = {idB}
          |          AND a.property LIKE {property1}
          |          AND b.property LIKE {property2}
          |          AND a.time >= {timeStartA}
          |          AND a.time <= {timeEndA}
          |          AND b.time >= {timeStartB}
          |          AND b.time <= {timeEndB}
          |GROUP BY a.host
          """
    }
    try {
      val entries = SQL(
        q.stripMargin.trim)
        .on("idA" -> id)
        .on("idB" -> id)
        .on("property1" -> property1)
        .on("property2" -> property2)
        .on("timeStartA" -> timeStart)
        .on("timeEndA" -> timeEnd)
        .on("timeStartB" -> timeStart)
        .on("timeEndB" -> timeEnd)
        .as(avgRowParser *)
      return entries
    } catch {
      case e: Throwable =>
        println("Error while querying dstat")
        throw e
    }
    return List[(String, Double)]()
  }
  /**
   * see [[de.tuberlin.cit.progressestimator.history.JobHistory]]
   */
  override def getDstatEntries(id: Int): DstatEntries = {
    implicit val conn = connection
    try {
      val entries = SQL(
        """
          |SELECT   *
          |FROM     dstat_entry
          |WHERE    appId = {id}
          |ORDER BY time
          """.stripMargin.trim)
        .on("id" -> id)
        .as(dstatRowParser *)
      return new DstatEntries(entries.toArray)
    } catch {
      case e: Throwable =>
        println("Error while querying dstat")
        throw e
    }
    return new DstatEntries(Array[DstatEntry]())
  }
}

object DBJobHistory {
  def insertExecution(x: JobExecution, conn: Connection): Unit = {
    SQL"""
    INSERT INTO execution(id, name, exp_name, hash, parameters, input_file, input_name, input_size, input_records, time_start, time_finish) VALUES(
      ${x.id},
      ${x.name},
      ${x.expName},
      ${x.hash},
      ${x.parameters},
      ${x.input.file},
      ${x.input.name},
      ${x.input.size},
      ${x.input.numberOfRecords},
      ${x.timeStart},
      ${x.timeFinish}
    )
    """.executeInsert()(conn)
  }
  def insertIteration(jobId: Int, x: Iteration, conn: Connection): Unit = {
    SQL"""
    INSERT INTO iteration(app_id, iteration_nr, input_size, input_records, number_of_nodes, time_start, time_finish) VALUES(
      ${jobId},
      ${x.nr},
      ${x.input.size},
      ${x.input.numberOfRecords},
      ${x.resourceConfig.numberOfNodes},
      ${x.timeStart},
      ${x.timeFinish}
    )
    """.executeInsert()(conn)
  }
  def insertDstatEntry(jobId: Int, x: DstatEntry, conn: Connection): Unit = {
    SQL"""
    INSERT INTO dstat_entry (appId, time, host, property, value) VALUES(
      ${jobId},
      ${x.time},
      ${x.host},
      ${x.property},
      ${x.value}
    )
    """.executeInsert()(conn)
  }
}
