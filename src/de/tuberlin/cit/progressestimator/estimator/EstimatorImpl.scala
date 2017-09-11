package de.tuberlin.cit.progressestimator.estimator

import scala.collection.mutable.ListBuffer

import com.google.inject.Inject
import com.google.inject.Singleton

import de.tuberlin.cit.progressestimator.costfactor.CostFactorExtractor
import de.tuberlin.cit.progressestimator.entity.Iteration
import de.tuberlin.cit.progressestimator.entity.JobExecution
import de.tuberlin.cit.progressestimator.history.JobHistory
import de.tuberlin.cit.progressestimator.scaleout.CachedBellAdapter
import de.tuberlin.cit.progressestimator.simulator.SimulationResponse
import de.tuberlin.cit.progressestimator.util.reports.Reports

/** Helper class containing information of an iteration, which is used by different estimation components */
class IterationDetails {
  var adjustedRuntime = -1.0
  var costFactor = -1.0
  var costFactorWeight = -1.0
  var outlier = false
}
/**
 * This class wraps a job executions and adds information needed for the estimation by different estimation components.
 */
case class JobExecutionWithEstimationDetails(job: JobExecution) {
  var adjustedTimes = new ListBuffer[Double]
  var iterationDetails: Array[IterationDetails] = null
  var discarded = false
  var similarity = -1.0
  var costFactor = -1.0
  var prediction = -1.0
  var simVector: Array[Double] = null
  var flagExactPowell = false
}

/**
 * The implementation of the estimator performs the estimation.
 */
@Singleton
class EstimatorImpl @Inject() (val reports: Reports,
                               val jobHistory: JobHistory,
                               similarJobsExtractor: SimilarityMatching,
                               costFactorExtractor: CostFactorExtractor,
                               config: EstimatorConfig) extends Estimator {

  val bell = new CachedBellAdapter(jobHistory, config)

  /**
   * see [[de.tuberlin.cit.progressestimator.estimator.Estimator]]
   */
  override def estimate(currentJob: JobExecution, currentIteration: Int, jobFilter: JobExecution => Boolean, simResponse: SimulationResponse,
                        simWeightsSolver: SimilarityWeightsSolver = null): Double = {

    val allJobs = getJobExecutionWithEstimationDetails(currentJob, getAllJobs(currentJob, currentIteration, jobFilter))
    simResponse.jobCount = allJobs.length

    adjustRuntimes(currentJob, allJobs, simResponse, currentIteration)

    var similarJobs = similarJobsExtractor.getSimilarJobs(simResponse, currentJob, allJobs, currentIteration, false)

    if (simResponse.jobCountSimilar > 0) {
      costFactorExtractor.getCostFactors(currentJob, similarJobs, currentIteration, simResponse)
      getJobPredictions(currentJob, similarJobs, currentIteration, simResponse)

      addWeightsTrainingData(simWeightsSolver, similarJobs, simResponse)

      simResponse.addJobEstimationDetails(similarJobs)

      val estimate = finalEstimate(simResponse, similarJobs)
      estimate
    } else {
      // No estimate could be calculated
      Double.NaN
    }
  }
  /**
   * returns individual estimates of all given job executions.
   * This is used for the calculation of similarity quality mesures.
   */
  def estimateWithGivenJobs(currentJob: JobExecution, allJobs: ListBuffer[JobExecution], currentIteration: Int,
                            simResponse: SimulationResponse): ListBuffer[JobExecutionWithEstimationDetails] = {
    val jobs = getJobExecutionWithEstimationDetails(currentJob, allJobs)
    adjustRuntimes(currentJob, jobs, simResponse, currentIteration)
    costFactorExtractor.getCostFactors(currentJob, jobs, currentIteration, simResponse)
    getJobPredictions(currentJob, jobs, currentIteration, simResponse)

    jobs
  }

  /**
   *  adds estimation data to the weights solver, which uses this data
   *  for the efficient calculation of the mean errors in the objective function
   *  which is minimized in the weights training.
   */
  def addWeightsTrainingData(simWeightsSolver: SimilarityWeightsSolver, allJobs: ListBuffer[JobExecutionWithEstimationDetails], simResponse: SimulationResponse) = {

    if (simWeightsSolver != null) {
      if (simWeightsSolver.simIds == null) {
        simWeightsSolver.simIds = simResponse.simVectorIds
      }

      val exactPowellJobs = allJobs.filter(_.flagExactPowell)
      if (exactPowellJobs.length > 0) {
        val exactPowellVectors = exactPowellJobs.map(j => {
          (j.simVector, j.prediction)
        }).toArray
        val vector = new PowellJobData(exactPowellVectors, simResponse.runtimeActual, simResponse.job.name, simResponse.job.input.name)
        simWeightsSolver.addJobData(vector)
      }
    }
  }
  private def getJobExecutionWithEstimationDetails(currentJob: JobExecution, allJobs: ListBuffer[JobExecution]) = {
    allJobs.map({ job =>
      val j = new JobExecutionWithEstimationDetails(job)
      val maxIterations = Math.max(currentJob.iterations.length, job.iterations.length)
      j.iterationDetails = (new Array[IterationDetails](maxIterations)).map(_ => new IterationDetails)
      j
    })

  }
  /**
   * adjust runtimes of all previous job executions.
   */
  private def adjustRuntimes(currentJob: JobExecution, otherJobs: ListBuffer[JobExecutionWithEstimationDetails], simResponse: SimulationResponse, currentIteration: Int) = {
    val useRuntimeAdjustment = true

    if (useRuntimeAdjustment) {
      otherJobs.foreach(j => {
        j.adjustedTimes = adjustIterations(currentJob, j.job, currentIteration)
        var i = 0
        j.adjustedTimes.foreach(adjustedRuntime => {
          j.iterationDetails.apply(i).adjustedRuntime = adjustedRuntime
          i += 1
        })
        simResponse.addDiagram(j.job, j.job.iterations.map(_.runtime), j.adjustedTimes, "Adjusted Runtimes")
      })
    } else {
      otherJobs.foreach(j => {
        j.adjustedTimes = j.job.iterations.map(_.runtime)
        var i = 0
        j.adjustedTimes.foreach(adjustedRuntime => {
          j.iterationDetails.apply(i).adjustedRuntime = adjustedRuntime
          i += 1
        })
      })

    }
  }
  case class JobPrediction(job: JobExecution, similarity: Double, prediction: Double)
  /**
   * calculates the individual estimates for each similar execution
   */
  private def getJobPredictions(currentJob: JobExecution,
                        jobsWithCostFactor: ListBuffer[JobExecutionWithEstimationDetails],
                        currentIteration: Integer,
                        simResponse: SimulationResponse) = {
    for (t <- jobsWithCostFactor) {
      if (!t.discarded || t.flagExactPowell) {
        getJobPrediction(t, currentIteration, currentJob, simResponse)
      }
    }
  }
  /**
   * calculates the estimate of one similar execution
   */
  private def getJobPrediction(t: JobExecutionWithEstimationDetails, currentIteration: Integer, currentJob: JobExecution, simResponse: SimulationResponse) = {
    val similarJob = t.job
    val adjustedIterations = t.adjustedTimes
    val remainingIterations = adjustedIterations.drop(currentIteration)
    t.prediction = remainingIterations.sum * t.costFactor
    if (java.lang.Double.isNaN(t.prediction)) {
      val o = 0
    }
    simResponse.addJobLog(similarJob, "Prediction Based on this job", "%.3f".format(t.prediction))
    simResponse.addJobLog(similarJob, "Avg CostFactor Factor", "%.3f".format(t.costFactor))

  }

  /**
   * adjusts the runtimes of the given other job to match the scale-out of the given current job.
   */
  private def adjustIterations(currentJob: JobExecution, otherJob: JobExecution, currentIteration: Int): ListBuffer[Double] = {
    val numberOfNodesAfterCurrentIteration = currentJob.iterations.apply(currentIteration - 1).resourceConfig.numberOfNodes

    val adjusted = otherJob.iterations.map(t => {
      val otherJobIteration = t
      var currentNodes = -1
      var currentJobRuntime = "-"
      if (otherJobIteration.nr < currentIteration) {
        val currentJobIteration = currentJob.iterations.apply(otherJobIteration.nr)
        currentNodes = currentJobIteration.resourceConfig.numberOfNodes
        currentJobRuntime = "" + currentJobIteration.runtime
      } else {
        // use value of last iteration of the current job
        currentNodes = numberOfNodesAfterCurrentIteration
      }
      val otherNodes = otherJobIteration.resourceConfig.numberOfNodes
      if (currentNodes == otherNodes) {
        otherJobIteration.runtime
      } else {
        val predictionOtherNodes = bell.getRuntimePrediction(otherJob, otherJobIteration.input.numberOfRecords, otherNodes, otherJobIteration.nr)
        val predictionCurrentNodes = bell.getRuntimePrediction(otherJob, otherJobIteration.input.numberOfRecords, currentNodes, otherJobIteration.nr)
        adjustIteration(predictionOtherNodes, predictionCurrentNodes, otherJobIteration)
      }
    })
    return adjusted
  }

  /**
   * adjusts the runtime of the given iteration using
   * the given scaleout data, which is used to calculate a factor and multiplying it
   * with the runtime.
   */
  private def adjustIteration(predictionOtherNodes: Double, predictionCurrentNodes: Double, otherJobIteration: Iteration): Double = {

    var result = .0
    if (predictionOtherNodes != 0) {
      val factor = predictionCurrentNodes / predictionOtherNodes
      val adjustedRuntime = otherJobIteration.runtime * factor
      if (factor > 3 || factor < 0.4) {
        // sanity check
        result = otherJobIteration.runtime
      } else {
        result = adjustedRuntime
      }
    } else {
      result = otherJobIteration.runtime
    }
    if (java.lang.Double.isNaN(result)) {
      result = otherJobIteration.runtime
      // error!
    }
    result
  }
  /**
   * calculate the final estimate using the individual estimates from the similar executions set
   */
  private def finalEstimate(simResponse: SimulationResponse, predictionsWithSimilarity: ListBuffer[JobExecutionWithEstimationDetails]): Double = {
    val similarJobs = predictionsWithSimilarity.filter(!_.discarded)
    var avg = weightPredictions(similarJobs, 0)
    var avgWeightedSimilarities = weightPredictions(similarJobs, 1)
    val finalEstimate = avgWeightedSimilarities
    simResponse.addLog(s"Average Prediction", "%.3f".format(avg))
    simResponse.addLog(s"Average Prediction Weighted with Similarities", "%.3f".format(avgWeightedSimilarities))
    finalEstimate
  }
  /**
   * helper calculating the weighted average, applying the given power to the similarity value and
   * using it as the weight
   */
  private def weightPredictions(predictions: ListBuffer[JobExecutionWithEstimationDetails], pow: Double): Double = {
    predictions.map(t => Math.pow(t.similarity, pow) * t.prediction).sum / predictions.map(t => Math.pow(t.similarity, pow)).sum
  }
  private def getAllJobs(currentJob: JobExecution, currentIteration: Integer, filter: JobExecution => Boolean): ListBuffer[JobExecution] = {
    var allJobs = jobHistory.getJobExecutions(currentJob.name).filter(_.finished).filter(_.id != currentJob.id).filter(_.iterations.size > currentIteration)
    allJobs.filter(filter)

  }

  def bellToDisk() = {
    bell.saveToDisk()
  }

}