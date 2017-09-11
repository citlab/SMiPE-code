package de.tuberlin.cit.progressestimator.estimator

import scala.collection.mutable.ListBuffer

import com.google.inject.Inject

import de.tuberlin.cit.progressestimator.entity.JobExecution
import de.tuberlin.cit.progressestimator.estimator.settransformation.ConditionalDiscard
import de.tuberlin.cit.progressestimator.estimator.settransformation.LimitSimilarExecutions
import de.tuberlin.cit.progressestimator.estimator.settransformation.LimitVeryHighSimilarity
import de.tuberlin.cit.progressestimator.estimator.settransformation.SimilarSetTransformation
import de.tuberlin.cit.progressestimator.similarity.ActiveDataRecordDistributionSimilarity
import de.tuberlin.cit.progressestimator.similarity.AdjustedRuntimeDistributionSimilarity
import de.tuberlin.cit.progressestimator.similarity.AvgNumberOfNodesSimilarity
import de.tuberlin.cit.progressestimator.similarity.CostFactorOutlierSimilarity
import de.tuberlin.cit.progressestimator.similarity.DstatCache
import de.tuberlin.cit.progressestimator.similarity.DstatNodeDistributionSimilarity
import de.tuberlin.cit.progressestimator.similarity.ExtremeRuntimeValuesSimilarity
import de.tuberlin.cit.progressestimator.similarity.InputDataSetSizeSimilarity
import de.tuberlin.cit.progressestimator.similarity.Similarity
import de.tuberlin.cit.progressestimator.similarity.WeightedSimilarityCombination
import de.tuberlin.cit.progressestimator.similarity.WeightedSimilarityCombinationPower
import de.tuberlin.cit.progressestimator.simulator.SimulationResponse
import de.tuberlin.cit.progressestimator.simulator.SimulationResponseDummy
import de.tuberlin.cit.progressestimator.util.WeightFunctions
import de.tuberlin.cit.progressestimator.similarity.ProductSimilarity

/**
 * This component performs the similarity matching.
 * It selects similar executions for an estimation according to similarity criteria.
 */
class SimilarityMatchingImpl @Inject() (val simThresholdExtractor: DynamicSimilarityThresholdExtractor,
                                        val config: EstimatorConfig, val dstatCache: DstatCache, val weightsSolver: SimilarityWeightsSolver) extends SimilarityMatching {

  var setTransformations = createSetTransformations()

  /**
   * returns the similar executions set, which determines which executions
   * were discarded by the matching algorithm.
   */
  def getSimilarJobs(simResponse: SimulationResponse,
                     currentJob: JobExecution,
                     jobs: ListBuffer[JobExecutionWithEstimationDetails],
                     currentIteration: Int, forceManualWeights: Boolean): ListBuffer[JobExecutionWithEstimationDetails] = {
    val similarity = getSimilarity(simResponse, forceManualWeights) // TODO: This could be cached. no need to recreate for every simulation. BUT then: make copy for .lowerThresholds()
    simResponse.simVectorIds = similarity.getVectorSimIds()
    performMatching(jobs, simResponse, similarity, currentJob, currentIteration)

    lowerThresholdsIfNecessary(jobs, simResponse, similarity, currentJob, currentIteration)

    // transform set and recalculate counts
    setTransformations.foreach(t =>
      t.transform(jobs, currentJob, currentIteration, simResponse))
    simResponse.jobCountSimilar = jobs.filter(_.discarded == false).length
    val discardedCount = jobs.filter(_.discarded).length

    simResponse.addLog(s"Kept ${simResponse.jobCountSimilar} similar jobs out of ${jobs.size} ($discardedCount discarded)")
//    println(s"Kept ${simResponse.jobCountSimilar} similar jobs out of ${jobs.size} ($discardedCount discarded)")
    simResponse.addSimilarJobsDiagrams(jobs)

    return jobs
  }

  /**
   * performs the matching process for each execution using the given similarity
   * containing the used thresholds and weights.
   */
  private def performMatching(jobs: ListBuffer[JobExecutionWithEstimationDetails],
                              simResponse: SimulationResponse, similarity: WeightedSimilarityCombinationPower,
                              currentJob: JobExecution, currentIteration: Integer) = {
    jobs.foreach(j => {
      if (simResponse.loweredThresholds == 0) {
        simResponse.addSimilarityLogs = true
      }
      val (keep, sim, simVector) = similarity.getSimilarityWithVector(currentJob, j.job, currentIteration, j.adjustedTimes)
      if (simResponse.loweredThresholds == 0) {
        simResponse.addSimilarityLogs = false
      }
      j.simVector = simVector
      j.similarity = sim
      if (!keep) {
        j.discarded = true
        j.flagExactPowell = false
      } else {
        j.discarded = false
        j.flagExactPowell = true
      }
    })

    simResponse.jobCountSimilar = jobs.filter(!_.discarded).length
  }

  /**
   * lower thresholds if no jobs could be matched. This is repeated for a maximum of
   * m times, which is set by in config.lowerThresholdTimes.
   */
  private def lowerThresholdsIfNecessary(jobs: ListBuffer[JobExecutionWithEstimationDetails],
                                         simResponse: SimulationResponse, similarity: WeightedSimilarityCombinationPower,
                                         currentJob: JobExecution, currentIteration: Integer) = {
    simResponse.loweredThresholds = 0
    while (simResponse.jobCountSimilar == 0 && simResponse.loweredThresholds <= config.lowerThresholdTimes) {
      simResponse.loweredThresholds += 1
      similarity.lowerThresholds(config.lowerThresholdFactor)
      performMatching(jobs, simResponse, similarity, currentJob, currentIteration)
      simResponse.jobCountSimilar = jobs.filter(!_.discarded).length
    }
  }

  /**
   *  The set transformations
   *
   */
  def createSetTransformations(): Seq[SimilarSetTransformation] = {
    val dummy = new SimulationResponseDummy

    val limitSimilarExecutions = new LimitSimilarExecutions(config)

    val activeRecordSim = new ActiveDataRecordDistributionSimilarity(dummy, normalize = false)
    val costFactorOutlier = new CostFactorOutlierSimilarity(dummy, config)
    val conditionalDiscardActiveRecords1 = new ConditionalDiscard(0.995, 0.995, 2, activeRecordSim)
    val conditionalDiscardActiveRecords2 = new ConditionalDiscard(0.997, 0.997, 2, activeRecordSim)
    val conditionalDiscardActiveRecords3 = new ConditionalDiscard(0.98, 0.95, 1, activeRecordSim)
    val conditionalDiscardOutlier = new ConditionalDiscard(0.995, 0.8, 3, costFactorOutlier)

    val limitVeryHighSimilarities = new LimitVeryHighSimilarity(10, config.veryHighSimilarity)
    Seq(limitVeryHighSimilarities, conditionalDiscardActiveRecords1, conditionalDiscardActiveRecords2, conditionalDiscardActiveRecords3,
      conditionalDiscardOutlier, limitSimilarExecutions)
  }

  /**
   * adds the given similarity to the weighted combination.
   * A trained weight is assigned to the similarity, if this is enabled
   * in the configuration.
   */
  private def addSimilarityToCombination(similarity: Similarity, combination: WeightedSimilarityCombination, weightManual: Int, useForSolver: Boolean, simResponse: SimulationResponse, forceManualWeights: Boolean) = {

    val weight = if (!forceManualWeights && config.useDynamicSimilarityWeights) {
      val algo = simResponse.job.name
      val dataset = null
      val dynamicWeight = weightsSolver.getFromCache(algo, dataset, similarity.getIdentifier())
      if (!dynamicWeight.equals(Double.NaN) && dynamicWeight >= 0) {
        dynamicWeight.toInt
      } else {
        0
      }
    } else {
      weightManual
    }
    combination.addWeightedSimilarity(weight, similarity, useForSolver)
  }
  /**
   * returns all used similarities as a weighted combination, so it can directly be used
   * in the matching algorithm
   */
  private def getSimilarity(simResponse: SimulationResponse, forceManualWeights: Boolean = false): WeightedSimilarityCombinationPower = {

    var combination = getSimilarityCombinationHelper(simResponse, forceManualWeights)
    // Weights Sanity Check
    val weightsSum = combination.getTotalWeight()
    if (weightsSum == 0) {

      System.out.println("Invalid weights configuration! Disable Dynamic Weights and use standard values.")
      config.useDynamicSimilarityWeights = false
      combination = new WeightedSimilarityCombination(simResponse, config.individualSimilarityExponent)
    }

    retrieveDynamicThresholds(combination, simResponse)

    val exponent = config.finalSimilarityExponent
    val powerSim = new WeightedSimilarityCombinationPower(exponent, combination, simResponse)

    return powerSim
  }

  /**
   * retrieve the trained similarity threshold values if enabled in the configuration
   */
  private def retrieveDynamicThresholds(combination: WeightedSimilarityCombination, simResponse: SimulationResponse) = {
    val useThresholdWithDatasetGranularity = false
    if (config.useDynamicThresholds) {
      combination.similarities.foreach(t => {
        val sim = t._2
        val key = if (useThresholdWithDatasetGranularity) {
          simResponse.job.name + "-" + simResponse.job.input.name
        } else {
          simResponse.job.name + "-all"
        }
        val threshold = simThresholdExtractor.getThreshold(key, sim)
        if (threshold != null && !threshold.equals(Double.NaN)) {
          sim.KEEP_THRESHOLD = threshold
        } else {
          val r = 0
        }
      })
    }
  }

  /**
   * creates individual similarities and adds them to a weighted combination
   */
  private def getSimilarityCombinationHelper(simResponse: SimulationResponse, forceManualWeights: Boolean): WeightedSimilarityCombination = {
    var costFactorOutlierSimilarity = new CostFactorOutlierSimilarity(simResponse, config)
    var extremeRuntimeValues = new ExtremeRuntimeValuesSimilarity(simResponse)
    var inputSizeSimilarity = new InputDataSetSizeSimilarity(simResponse)
    var activeRecordDistributionSimilarity = new ActiveDataRecordDistributionSimilarity(simResponse, normalize = false).useDeltaDeviation()
    var activeRecordDistributionSimilarityWeighted = new ActiveDataRecordDistributionSimilarity(simResponse, normalize = false).setWeightFunction(WeightFunctions.weightFunction1OverX, "weightFunction1OverX").useDeltaDeviation()
    var runtimeSimilarityAdjusted = (new AdjustedRuntimeDistributionSimilarity(simResponse)).setWeightFunction(WeightFunctions.weightFunction1OverX, "weightFunction1OverX").useDeltaDeviation()
    var nodesSimilarity = new AvgNumberOfNodesSimilarity(simResponse)

    var dstatDistrCpu = new DstatNodeDistributionSimilarity(simResponse, dstatCache, "idl")
    val dstatDistrIo = new DstatNodeDistributionSimilarity(simResponse, dstatCache, "writ")
    val dstatDistrSend = new DstatNodeDistributionSimilarity(simResponse, dstatCache, "send")
    val dstatDistrRecv = new DstatNodeDistributionSimilarity(simResponse, dstatCache, "recv")

    val adjustedRuntimeWithInput = new ProductSimilarity(simResponse).addSimilarity(runtimeSimilarityAdjusted).addSimilarity(inputSizeSimilarity)
    val activeAbsoluteDeltaWithInput = new ProductSimilarity(simResponse).addSimilarity(activeRecordDistributionSimilarity).addSimilarity(inputSizeSimilarity)
    val adjustedRuntimeWithActiveRecordWithInput = new ProductSimilarity(simResponse).addSimilarity(adjustedRuntimeWithInput).addSimilarity(activeRecordDistributionSimilarity)

    val combAdjustedCpuM = new ProductSimilarity(simResponse).addSimilarity(adjustedRuntimeWithInput).addSimilarity(dstatDistrCpu)
    val combAdjustedSendM = new ProductSimilarity(simResponse).addSimilarity(adjustedRuntimeWithInput).addSimilarity(dstatDistrSend)
    val combAdjustedRecM = new ProductSimilarity(simResponse).addSimilarity(adjustedRuntimeWithInput).addSimilarity(dstatDistrRecv)
    val combAdjustedIoM = new ProductSimilarity(simResponse).addSimilarity(adjustedRuntimeWithInput).addSimilarity(dstatDistrIo)

    val combAdjustedActiveAbsCpuM = new ProductSimilarity(simResponse).addSimilarity(adjustedRuntimeWithActiveRecordWithInput).addSimilarity(dstatDistrCpu)
    val combAdjustedActiveAbsSendM = new ProductSimilarity(simResponse).addSimilarity(adjustedRuntimeWithActiveRecordWithInput).addSimilarity(dstatDistrSend)
    val combAdjustedActiveAbsRecM = new ProductSimilarity(simResponse).addSimilarity(adjustedRuntimeWithActiveRecordWithInput).addSimilarity(dstatDistrRecv)
    val combAdjustedActiveAbsIoM = new ProductSimilarity(simResponse).addSimilarity(adjustedRuntimeWithActiveRecordWithInput).addSimilarity(dstatDistrIo)

    var combination = new WeightedSimilarityCombination(simResponse, config.individualSimilarityExponent)

    addSimilarityToCombination(inputSizeSimilarity, combination, 0, true, simResponse, forceManualWeights)
    addSimilarityToCombination(activeRecordDistributionSimilarityWeighted, combination, 1, true, simResponse, forceManualWeights)
    addSimilarityToCombination(runtimeSimilarityAdjusted, combination, 4, true, simResponse, forceManualWeights)
    addSimilarityToCombination(nodesSimilarity, combination, 0, false, simResponse, forceManualWeights)
    addSimilarityToCombination(dstatDistrCpu, combination, 0, true, simResponse, forceManualWeights)
    addSimilarityToCombination(dstatDistrIo, combination, 0, true, simResponse, forceManualWeights)
    addSimilarityToCombination(dstatDistrSend, combination, 0, true, simResponse, forceManualWeights)
    addSimilarityToCombination(dstatDistrRecv, combination, 0, true, simResponse, forceManualWeights)
    addSimilarityToCombination(costFactorOutlierSimilarity, combination, 0, true, simResponse, forceManualWeights)
    addSimilarityToCombination(extremeRuntimeValues, combination, 4, true, simResponse, forceManualWeights)

    addSimilarityToCombination(combAdjustedCpuM, combination, 0, true, simResponse, forceManualWeights)
    addSimilarityToCombination(combAdjustedSendM, combination, 0, true, simResponse, forceManualWeights)
    addSimilarityToCombination(combAdjustedActiveAbsRecM, combination, 0, true, simResponse, forceManualWeights)
    addSimilarityToCombination(combAdjustedIoM, combination, 0, true, simResponse, forceManualWeights)
    addSimilarityToCombination(combAdjustedActiveAbsCpuM, combination, 0, true, simResponse, forceManualWeights)
    addSimilarityToCombination(combAdjustedActiveAbsSendM, combination, 0, true, simResponse, forceManualWeights)
    addSimilarityToCombination(combAdjustedActiveAbsRecM, combination, 0, true, simResponse, forceManualWeights)
    addSimilarityToCombination(combAdjustedActiveAbsIoM, combination, 0, true, simResponse, forceManualWeights)

    combination
  }

}