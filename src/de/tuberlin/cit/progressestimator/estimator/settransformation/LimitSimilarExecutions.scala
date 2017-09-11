package de.tuberlin.cit.progressestimator.estimator.settransformation

import de.tuberlin.cit.progressestimator.estimator.JobExecutionWithEstimationDetails
import de.tuberlin.cit.progressestimator.similarity.Similarity
import de.tuberlin.cit.progressestimator.entity.JobExecution
import scala.collection.mutable.ListBuffer
import de.tuberlin.cit.progressestimator.simulator.SimulationResponse
import de.tuberlin.cit.progressestimator.estimator.EstimatorConfig

/**
 * Limits the number of similar executions to the given value.
 * The executions with the lowest similarities are discarded.
 */
class LimitSimilarExecutions(val config: EstimatorConfig) extends SimilarSetTransformation {

  override def transform(l: ListBuffer[JobExecutionWithEstimationDetails], currentJob: JobExecution, currentIteration: Integer, simResponse : SimulationResponse): Unit = {
    l.filter(!_.discarded).sortBy(_.similarity).reverse.drop(config.maxSimilarJobs).foreach(j => {
      j.discarded = true
      simResponse.addJobLog(j.job, "Discarded [only top " + config.maxSimilarJobs + " similar jobs were kept]")

    })
    
  }
}