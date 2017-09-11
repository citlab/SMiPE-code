package de.tuberlin.cit.progressestimator.estimator.settransformation

import de.tuberlin.cit.progressestimator.estimator.JobExecutionWithEstimationDetails
import de.tuberlin.cit.progressestimator.similarity.Similarity
import de.tuberlin.cit.progressestimator.entity.JobExecution
import scala.collection.mutable.ListBuffer
import de.tuberlin.cit.progressestimator.simulator.SimulationResponse

/**
 * If there are n undiscarded executions with a similarity sim of >= x, then discard
 * executions with a similarity <= y.
 */
class ConditionalDiscard(val x: Double, val y: Double, val n: Integer, val sim: Similarity) extends SimilarSetTransformation {

  override def transform(l: ListBuffer[JobExecutionWithEstimationDetails], currentJob: JobExecution, currentIteration: Integer, simResponse : SimulationResponse): Unit = {
    val listWithSimilarities = l.map(j => {
      val s = sim.getSimilarity(currentJob, j.job, currentIteration, j.adjustedTimes)
      (s._2, j)
    })

    val greaterThanX = listWithSimilarities.filter(t => t._1 > x && !t._2.discarded).length
    if (greaterThanX >= n) {
      listWithSimilarities.foreach(t => {
        if (t._1 < y) {
          t._2.discarded = true
        }
      })
    }
  }
}