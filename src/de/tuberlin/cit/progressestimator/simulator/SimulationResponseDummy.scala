package de.tuberlin.cit.progressestimator.simulator

import de.tuberlin.cit.progressestimator.entity.JobExecution
import scala.collection.mutable.HashMap
import scala.collection.mutable.ListBuffer
import de.tuberlin.cit.progressestimator.estimator.JobExecutionWithEstimationDetails

/**
 * a dummy report which does not save any data.
 * This is used for efficiency if no response is needed, e.g. for testing
 * or the quality computation
 */
class SimulationResponseDummy extends SimulationResponse {
 
  override def deviationPercentage: Double = .0
  override def addJobEstimationDetails(jobs: ListBuffer[JobExecutionWithEstimationDetails]) = {}
  override def addJobLog(otherJob: JobExecution, message: String, value: String = ""): Unit = { }
  override def addLog(message: String, value: String = ""): Unit = { }
  override def addDiagram(otherJob: JobExecution,
                 points1: ListBuffer[Double], points2: ListBuffer[Double], title: String = ""): Unit = { }
  override def addSimilarJobsDiagrams(similarJobs: ListBuffer[JobExecutionWithEstimationDetails]): Unit = { }
}