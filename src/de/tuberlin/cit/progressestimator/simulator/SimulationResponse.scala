package de.tuberlin.cit.progressestimator.simulator

import de.tuberlin.cit.progressestimator.entity.JobExecution
import scala.collection.mutable.HashMap
import scala.collection.mutable.ListBuffer
import de.tuberlin.cit.progressestimator.estimator.JobExecutionWithEstimationDetails

/**
 * A Simulation Response contains the information of a simulation.
 * This includes the estimation result, as well as additional information
 * used for reporting.
 */
class SimulationResponse {
  var reportFileName: String = ""
  var failed = false
  var runtimeActual: Double = -1
  var runtimeEstimate: Double = -1
  var iteration = -1
  var job: JobExecution = null
  var jobCount = -1
  var jobCountSimilar = -1 
  var loweredThresholds = 0
  
  var simulationTime = -1L
  
  var addSimilarityLogs = true

  var simVectorIds : ListBuffer[String] = null
  
  val jobEstimationDetails = HashMap.empty[Integer, JobExecutionWithEstimationDetails]
  var reportJobLogs = HashMap.empty[Integer, ListBuffer[(String, String)]]
  var reportLogs = new ListBuffer[(String, String)]()
  var diagrams = HashMap.empty[Integer, ListBuffer[(ListBuffer[Double], ListBuffer[Double], String)]]

  def removeLogs() = {
    jobEstimationDetails.clear()
    reportJobLogs.clear()
    reportLogs.clear()
    diagrams.clear()
    simVectorIds = null
  }
  /**
   * returns the relative estimation error
   */
  def deviationPercentage: Double = {
    if (runtimeActual < 0 || runtimeEstimate < 0) {
      -1
    } else {
      val diff = runtimeEstimate - runtimeActual
      val percentage = diff / runtimeActual
      percentage
    }
  }

  def addJobEstimationDetails(jobs: ListBuffer[JobExecutionWithEstimationDetails]) = {
    jobs.foreach(j => jobEstimationDetails.put(j.job.id, j))
  }
  def addSimilarityLog(otherJob: JobExecution, message: String, value: String = ""): Unit = {
    if(addSimilarityLogs) {
      addJobLog(otherJob, message, value)
    }
  }
  def addJobLog(otherJob: JobExecution, message: String, value: String = ""): Unit = {
    var list = ListBuffer[(String, String)]()
    if (reportJobLogs.contains(otherJob.id)) {
      list = reportJobLogs.get(otherJob.id).get
    } else {
      reportJobLogs.put(otherJob.id, list)
    }
    list += ((message, value))
  }

  def addLog(message: String, value: String = ""): Unit = {
    reportLogs += ((message, value))
  }
  def addDiagram(otherJob: JobExecution,
                 points1: ListBuffer[Double], points2: ListBuffer[Double], title: String = ""): Unit = {
    var list = ListBuffer[(ListBuffer[Double], ListBuffer[Double], String)]()
    if (diagrams.contains(otherJob.id)) {
      list = diagrams.get(otherJob.id).get
    } else {
      diagrams.put(otherJob.id, list)
    }
    list += ((points1, points2, title))

  }

  def addSimilarJobsDiagrams(similarJobs: ListBuffer[JobExecutionWithEstimationDetails]): Unit = {
    val currentJobIterationNodes = job.iterations.map(_.resourceConfig.numberOfNodes.toDouble)
    similarJobs.foreach(otherJob => addDiagram(otherJob.job, currentJobIterationNodes,
      otherJob.job.iterations.take(iteration).map(_.resourceConfig.numberOfNodes.toDouble), "Nodes"))

  }
}