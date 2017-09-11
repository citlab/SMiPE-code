package de.tuberlin.cit.progressestimator.similarity
import de.tuberlin.cit.progressestimator.entity.JobExecution
import scala.collection.mutable.ListBuffer
import de.tuberlin.cit.progressestimator.util.reports.SimilarityReport
import de.tuberlin.cit.progressestimator.simulator.SimulationResponse
import de.tuberlin.cit.progressestimator.costfactor.CostFactorExtractorImpl
import de.tuberlin.cit.progressestimator.costfactor.CostFactorExtractor
import de.tuberlin.cit.progressestimator.estimator.JobExecutionWithEstimationDetails
import de.tuberlin.cit.progressestimator.estimator.EstimatorConfig

class CostFactorOutlierSimilarity(val simResponse: SimulationResponse, val config: EstimatorConfig) extends Similarity {
  val costFactorExtractor = new CostFactorExtractorImpl(config)
  KEEP_THRESHOLD = 0.5
  def getIdentifier(): String = {
    val id = "CostFactorOutlierSimilarity"
    id
  }
  def getSimilarity(job1: JobExecution, job2: JobExecution, currentIteration: Int, job2AdjustedRuntimes: ListBuffer[Double] = ListBuffer[Double]()): (Boolean, Double) = {

    val costFactors = costFactorExtractor.getCostFactorsPerJob(job1, job2, job2AdjustedRuntimes, currentIteration)

    val numberOfFactors = costFactors.length
    val numberOfOutliers = costFactors.filter(_._2).length

    val similarity = (numberOfFactors - numberOfOutliers).toDouble / numberOfFactors

    simResponse.addSimilarityLog(job2, s"CostFactor Outlier Similarity: ", "%.4f".format(similarity))

    return (similarity > KEEP_THRESHOLD, similarity)
  }

}