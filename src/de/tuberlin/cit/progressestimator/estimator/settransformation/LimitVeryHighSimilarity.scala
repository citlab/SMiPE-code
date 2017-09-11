package de.tuberlin.cit.progressestimator.estimator.settransformation

import de.tuberlin.cit.progressestimator.estimator.JobExecutionWithEstimationDetails
import de.tuberlin.cit.progressestimator.similarity.Similarity
import de.tuberlin.cit.progressestimator.entity.JobExecution
import scala.collection.mutable.ListBuffer
import de.tuberlin.cit.progressestimator.simulator.SimulationResponse

/**
 * If there are more than x jobs with a very high similarity,
 * the other jobs are discarded.
 */
class LimitVeryHighSimilarity(val x : Int, val veryHighSimilarity: Double) extends SimilarSetTransformation {

  override def transform(l: ListBuffer[JobExecutionWithEstimationDetails], currentJob: JobExecution, currentIteration: Integer, simResponse : SimulationResponse): Unit = {
     val verySimilarJobs = l.filter(j => !j.discarded && j.similarity >= veryHighSimilarity)
    if (verySimilarJobs.length >= x) {
      l.filter(j => !j.discarded && j.similarity < veryHighSimilarity).foreach(j => {
        j.discarded = true
        simResponse.addJobLog(j.job, "Discarded [enough jobs with high similarity of >= " + veryHighSimilarity + "]")
      })
    }
     
    
  }
}