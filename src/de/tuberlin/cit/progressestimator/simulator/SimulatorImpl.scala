package de.tuberlin.cit.progressestimator.simulator

import de.tuberlin.cit.progressestimator.history.JobHistory
import de.tuberlin.cit.progressestimator.entity.JobExecution
import de.tuberlin.cit.progressestimator.util.reports.{ Reports, SimilarityReport }
import scala.collection.mutable.ListBuffer
import com.google.inject.Inject
import de.tuberlin.cit.progressestimator.estimator.Estimator
import java.time.format.DateTimeFormatter
import java.time.ZoneOffset
import java.time.Instant
import de.tuberlin.cit.progressestimator.util.reports.SimilarityReportDummy

class SimulatorImpl @Inject() (val jobHistory: JobHistory,
                               val reports: Reports,
                               val estimator: Estimator) extends Simulator {

  /**
   * performs a simulation for the given simulation request and returns the repsonse.
   */
  override def simulate(request: SimulationRequest, createReport: Boolean = true): SimulationResponse = {
    
    val t0 = System.nanoTime()
    val timestamp = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss").withZone(ZoneOffset.ofHours(2)).format(Instant.now())
    val reportFileName = s"simulationReport-$timestamp-${request.currentJobId}-${request.iteration}.html"
    val response = new SimulationResponse
    response.iteration = request.iteration
    response.reportFileName = reportFileName
    var allJobs = jobHistory.getJobExecutions()
    val currentJob = jobHistory.getJobExecutionById(request.currentJobId)

    if (currentJob != null) {
      response.job = currentJob
      val actual = currentJob.iterations.drop(request.iteration).map(_.runtime).sum
      response.runtimeActual = actual
      println(s"Start Simulation for job name=${currentJob.name} id=${currentJob.id} ")
      val simReport = if (createReport) {
        new SimilarityReport(currentJob: JobExecution, allJobs: ListBuffer[JobExecution], response)
      } else {
        new SimilarityReportDummy
      }
      val estimate = estimator.estimate(currentJob, request.iteration, request.jobHistoryFilter, response, request.weightsSolver)

      if (createReport) {
        reports.writeFile(simReport.createReport(), reportFileName)
      }
      response.removeLogs()
      response.runtimeEstimate = estimate
    } else {
      response.failed = true
    }
    val t1 = System.nanoTime()
    response.simulationTime = t1-t0
    response
  }
  override def simulate(jobId: Int, iteration: Int): String = {
    val request = new SimulationRequest(iteration, jobId)
    val response = simulate(request)
    response.reportFileName
  }
}