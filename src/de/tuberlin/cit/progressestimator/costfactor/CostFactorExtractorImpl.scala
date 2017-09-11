package de.tuberlin.cit.progressestimator.costfactor

import scala.collection.mutable.ListBuffer
import de.tuberlin.cit.progressestimator.entity.JobExecution
import de.tuberlin.cit.progressestimator.estimator.JobExecutionWithEstimationDetails
import de.tuberlin.cit.progressestimator.simulator.SimulationResponse
import de.tuberlin.cit.progressestimator.estimator.IterationDetails
import com.google.inject.Singleton
import de.tuberlin.cit.progressestimator.util.WeightFunctions
import de.tuberlin.cit.progressestimator.estimator.IterationDetails
import de.tuberlin.cit.progressestimator.estimator.EstimatorConfig
import com.google.inject.Inject

/**
 * Implementation for the cost factor extraction which uses
 * the iteration factor. Additionally, it performs outlier removal
 * and iteration weighting as per the given configuration.
 */
@Singleton
class CostFactorExtractorImpl @Inject() (val config: EstimatorConfig) extends CostFactorExtractor {

  /**
   * calculates the cost factors of all given similar executions.
   * The cost factors are stored in the 
   * JobExecutionWithEstimationDetails instance of the respective execution.
   */
  override def getCostFactors(currentJob: JobExecution, similarJobs: ListBuffer[JobExecutionWithEstimationDetails],
                              currentIteration: Integer, simResponse: SimulationResponse) = {
    val costfactor = getCostFactor()
    for (j <- similarJobs) {
      if (!j.discarded || j.flagExactPowell) {
        val costFactors = List.range(0, (currentIteration + 0)).map(i => costfactor.getCostFactor(currentJob, j.job, i, j.adjustedTimes))
        j.costFactor = getCostFactorAverage(currentIteration, costFactors, j.iterationDetails)
      }
    }
  }
  
  /**
   * calculates cost factors of all iterations for the given other (similar) job execution.
   * It returns a list of tuples (costfactor, outlier) with outlier set to true if the cost factor
   * is considered an outlier.
   */
  def getCostFactorsPerJob(currentJob: JobExecution, otherJob: JobExecution, otherJobAdjustedRuntimes: ListBuffer[Double],
                           currentIteration: Integer): Array[(Double, Boolean)] = {
    val job2Details = new JobExecutionWithEstimationDetails(otherJob)
    job2Details.iterationDetails = (new Array[IterationDetails](otherJob.iterations.length)).map(_ => new IterationDetails)
    val costfactor = getCostFactor()
    val costFactors = List.range(0, (currentIteration + 0)).map(i => costfactor.getCostFactor(currentJob, otherJob, i, otherJobAdjustedRuntimes))
    getCostFactorAverage(currentIteration, costFactors, job2Details.iterationDetails)
    job2Details.iterationDetails.take(currentIteration).map(d => (d.costFactor, d.outlier))
  }

  /**
   * helper creating the cost factor instance
   */
  private def getCostFactor(): CostFactor = {
    val factor = new IterationRuntimeCostFactor()
    return factor
  }

  /**
   * returns the average cost factor of the similar execution with the given iteration details.
   * This incorporates outlier removal and iteration weighting.
   */
  private def getCostFactorAverage(currentIteration: Int, costFactors: List[Double], iterationDetails: Array[IterationDetails]): Double = {
    val numberOfIterations = costFactors.size
    val weightFunction = getWeightFunction()
    
    val weights = List.range(0, (numberOfIterations)).map(weightFunction(numberOfIterations, _))

    var i = 0
    val costFactorsWithWeights = costFactors.zip(weights).map(t => {
      val details = iterationDetails.apply(i)
      details.costFactor = t._1
      details.costFactorWeight = t._2
      i += 1
      (details.costFactor, details.costFactorWeight)
    })

    val costFactorsWithWeightsFiltered = filterCostFactorOutliers(costFactorsWithWeights, iterationDetails)
    val weightsSum = costFactorsWithWeightsFiltered.map(_._2).sum
    val avg = costFactorsWithWeightsFiltered.map(t => t._1 * t._2).sum / weightsSum
    return avg
  }
  /**
   * this method finds outliers in the cost factors and sets the outlier flag to true in the
   * respective IterationDetails instance.
   * A cost factor is considered an outlier if it considers by a factor of 2 in either direction
   * from the mean of the average.
   */
  private def filterCostFactorOutliers(costFactorsWithWeights: List[(Double, Double)], iterationDetails: Array[IterationDetails]): List[(Double, Double)] = {

    val avgCostFactor = getAvgCostFactor(costFactorsWithWeights)
    var i = 0
    val filterOutliers = config.costFactorOutliersFilter
    val filtered = if(filterOutliers) {
      costFactorsWithWeights.filter(t => {
      val details = iterationDetails.apply(i)
      i += 1
      val factor1 = t._1 / avgCostFactor
      val factor2 = avgCostFactor / t._1
      if (factor1 > 2 || factor2 > 2) {
        details.outlier = true
        false
      } else {
        true
      }
    })
    } else {
      costFactorsWithWeights
    }
    filtered
  }
  
  /**
   * returns the average cost factor, but only using the middle 80% of all values.
   * This is done to remove the influence of extreme factors from the average.
   */
  private def getAvgCostFactor(costFactorsWithWeights: List[(Double, Double)]) : Double = {
    val middleValues = getMiddleValues(80, costFactorsWithWeights)
    val avgCostFactor = middleValues.map(_._1).sum / middleValues.length
    avgCostFactor
  }

  
  /**
   * returns the middle p % of all values in the given cost factor 
   * list after sorting it by the cost factor
   */
  private def getMiddleValues(p: Int, costFactorlist: List[(Double, Double)]) : List[(Double, Double)] = {
    require(0 <= p && p <= 100) 
    require(!costFactorlist.isEmpty)
    
    val sorted = costFactorlist.sortBy(_._1)
    val k1 = (100- p) * costFactorlist.length / 100 
    val k2 = p * costFactorlist.length / 100 
    
    val list1 = sorted.splitAt(k2)._1
    val list2 = list1.splitAt(k1)._2
    list2
  }
  
  
  /**
   * returns the weight function according to the configuration
   */
  private def getWeightFunction() :  ((Int, Int) => Double) = {
    if(config.costFactorWeightFunction.equals("one")) {
      WeightFunctions.weightFunction1
    } else if(config.costFactorWeightFunction.equals("gauss")) {
      WeightFunctions.weightFunctionGaussian
    } else if(config.costFactorWeightFunction.equals("inverse")) {
      WeightFunctions.weightFunction1OverX
    } else if(config.costFactorWeightFunction.equals("inverseSqrt")) {
      WeightFunctions.weightFunction1OverSquareRootX
    } else if(config.costFactorWeightFunction.equals("linear")) {
      WeightFunctions.weightFunctionLinear
    } else {
      WeightFunctions.weightFunction1OverX
    }
  }

}
