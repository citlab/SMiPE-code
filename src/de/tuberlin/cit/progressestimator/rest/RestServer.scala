package de.tuberlin.cit.progressestimator.rest

import java.io.IOException
import java.nio.file.Files
import java.nio.file.Paths

import scala.language.existentials



import com.google.inject.Inject
/**
 * Very simple web server which takes HTTP requests for the REST API.
 *
 */
import com.google.inject.Singleton

import de.tuberlin.cit.progressestimator.estimator.DynamicSimilarityThresholdExtractor
import de.tuberlin.cit.progressestimator.estimator.Estimator
import de.tuberlin.cit.progressestimator.estimator.EstimatorConfig
import de.tuberlin.cit.progressestimator.estimator.SimilarityMatching
import de.tuberlin.cit.progressestimator.estimator.SimilarityWeightsSolver
import de.tuberlin.cit.progressestimator.history.JobHistory
import de.tuberlin.cit.progressestimator.similarity.DstatCache
import de.tuberlin.cit.progressestimator.simulator.Simulator
import de.tuberlin.cit.progressestimator.util.reports.JobIndex
import de.tuberlin.cit.progressestimator.util.reports.JobReport
import de.tuberlin.cit.progressestimator.util.reports.Reports
import fi.iki.elonen.NanoHTTPD
import fi.iki.elonen.NanoHTTPD._
import de.tuberlin.cit.progressestimator.history.CachedHistory

/**
 * This class implements a Nano Http Server to provide a REST API for the estimator
 * functionality.
 */
@Singleton
class RestServer @Inject() (val reports: Reports, val jobHistory: JobHistory, val simulator: Simulator,
                                val estimator: Estimator, val simThresholdExtractor: DynamicSimilarityThresholdExtractor,
                                val simQualService : SimilarityQualityService, val simulationService : SimulationService,
                                similarJobsExtractor: SimilarityMatching,
                                val config: EstimatorConfig, val dstatCache: DstatCache, val weightsSolver : SimilarityWeightsSolver) extends NanoHTTPD(3000) {
  private val NANOHTTPD_SOCKET_READ_TIMEOUT = 20000
  start(NANOHTTPD_SOCKET_READ_TIMEOUT, false)
  val port = 3000
  println("\nREST API server running: http://localhost:" + port + "/ \n")

  override def serve(session: IHTTPSession): Response = {

    try {

      session.parseBody(new java.util.HashMap[String, String]())
      val uri = session.getUri()
      val uriParts = uri.split("/")

      if (uri.equals("/simqual")) {
        return serveSimilarityQuality(session)
      } else if (uri.startsWith("/file")) {
        return serveFile(session)
      } else if (uri.equals("/jobs") || uri.equals("/jobs/")) {
        return serveJobList(session)
      } else if (uri.startsWith("/simulationSuite")) {
        return serveSimulationSuite(session)
      } else if (uri.startsWith("/jobs")) {
        if (uriParts.length > 3) {
          if (uriParts(3).equals("report")) {
            return serveJobReport(session)
          } else if (uriParts(3).equals("simulate")) {
            return serveSimulation(session)
          }
        }
      } else {
        return serveUtil(session)
      }
    } catch {
      case e: Exception => {
        e.printStackTrace()
        return internalError(session)
      }
    }
    return malformedRequest(session)
  }

  private def serveFile(session: IHTTPSession): Response = {

    val parameters = session.getParms
    if (parameters.containsKey("file")) {
      val file = config.directoryReports + "\\" + parameters.get("file")
      if (Files.exists(Paths.get(file))) {
        println(s"serve file $file")
        val code = scala.io.Source.fromFile(file).mkString
        val res = newFixedLengthResponse(code)
        return res
      }
    }
    return newFixedLengthResponse("file not found")
  }
  private def serveSimulationSuite(session: IHTTPSession): Response = {
    val fileName = simulationService.serveSimulationSuite(session)
    return newFixedLengthResponse(s"<a href='/file?file=$fileName'>$fileName</a>")
  }
  
  
  private def serveSimulation(session: IHTTPSession): Response = {
    val uri = session.getUri()
    val uriParts = uri.split("/")
    if (uriParts.length < 3) {
      return malformedRequest(session)
    } else {
      val id = uriParts(2).toInt
      val parameters = session.getParms
      var iteration = 10
      if (parameters.containsKey("iteration")) {
        iteration = parameters.get("iteration").toInt
      }
      
      val fileName = simulator.simulate(id, iteration)
      println(s"ID: ${id} iterations= ${iteration}")
      return newFixedLengthResponse(s"<a href='/file?file=${fileName}'>${fileName}</a>")
    }
  }

  private def serveJobReport(session: IHTTPSession): Response = {
    val uri = session.getUri()
    val uriParts = uri.split("/")
    if (uriParts.length < 4) {
      return malformedRequest(session)
    } else {
      val id = uriParts(2).toInt
      val jobOption = jobHistory.asInstanceOf[CachedHistory].getJobExecutionsIncludingOutlier().find(_.id == id)
      var code = ""
      if (jobOption.isEmpty) {
        code = "Job not found"
      } else {
        val job = jobOption.get
        var file = config.directoryReports + "\\job_" + job.id + "_" + job.name + ".html"
        var forceReportCreation = false
        if (session.getParms.containsKey("force")) {
          forceReportCreation = true
        }
        if (!forceReportCreation && Files.exists(Paths.get(file))) {
          println(s"serve file $file")
          code = scala.io.Source.fromFile(file).mkString
        } else {
          println(s"create report for  ${job.id}")
          reports.createJobReport(job)
          code = (new JobReport()).createReport(job)
        }
      }
      return newFixedLengthResponse(code)
    }
  }

  private def serveSimilarityQuality(session: IHTTPSession): Response = {
    simQualService.serveSimilarityQuality()
    return newFixedLengthResponse(s"SimQual Report")
  }

  private def serveJobList(session: IHTTPSession): Response = {

    val sorted = jobHistory.getJobExecutions().sortBy(_.timeStart).reverse.sortBy(_.parameters).sortBy(_.input.name).sortBy(_.name)
    val withFileName = sorted.map(j => ((j, s"/jobs/${j.id}/report")))
    val indexReport = new JobIndex
    val html = indexReport.createReport(withFileName)
    return newFixedLengthResponse(html)
  }
  private def serveUtil(session: IHTTPSession): Response = {
    try {
      val file = new String(Files.readAllBytes(Paths.get("rest.html")))
      return newFixedLengthResponse(file)
    } catch {
      case e: IOException => {

        val file = "An IOException occurred."
        e.printStackTrace()
        return newFixedLengthResponse(file)
      }
    }
  }

  private def malformedRequest(session: IHTTPSession): Response = {
    val msg = "<html><body><h1>Malformed Request</h1>\n"
    return newFixedLengthResponse(msg + "</body></html>\n")
  }
  private def internalError(session: IHTTPSession): Response = {
    val msg = "<html><body><h1>Internal Error</h1>\n"
    return newFixedLengthResponse(msg + "</body></html>\n")
  }
}