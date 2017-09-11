package de.tuberlin.cit.progressestimator.estimator
import de.tuberlin.cit.progressestimator.entity.JobExecution
import de.tuberlin.cit.progressestimator.util.reports.SimilarityReport
import de.tuberlin.cit.progressestimator.simulator.SimulationResponse
import com.google.inject.Singleton
import scala.collection.mutable.ListBuffer
import com.typesafe.config.ConfigFactory

/**
 * The estimator config contains settings and parameters for the estimator.
 * The configuration is loaded from a configuration file using Typesafe Config.
 */
@Singleton
class EstimatorConfig {
  val conf = ConfigFactory.load()
  
  // directories to use for reports and caches
  var directoryReports = conf.getString("estimator.directoryReports")
  var directoryCaches = conf.getString("estimator.directoryCaches")
  
  // create individual simulation report if a suite is executed
  var simulationSuiteCreateIndividualReports = conf.getBoolean("estimator.simulationSuiteCreateIndividualReports")

  var costFactorWeightFunction = conf.getString("estimator.costFactorWeightFunction")
  var costFactorOutliersFilter = conf.getBoolean("estimator.filterCostFactorOutliers")

  // use trained similarity thresholds
  var useDynamicThresholds = conf.getBoolean("estimator.dynamicThresholds.use")
  // parameters for the similarity threshold training
  var dynamicThresholdsMinimumAccuracy = conf.getDouble("estimator.dynamicThresholds.minAccuracy")
  var dynamicThresholdsMinimumJobShare = conf.getDouble("estimator.dynamicThresholds.minJobShare")
  var dynamicThresholdsStandardThreshold = conf.getDouble("estimator.dynamicThresholds.standardThreshold")

  // use trained similarity weights
  var useDynamicSimilarityWeights = conf.getBoolean("estimator.dynamicSimilarityWeights.use")

  // transformation of final / individual similarity values
  var finalSimilarityExponent = conf.getDouble("estimator.finalSimilarityExponent.general")
  var individualSimilarityExponent = conf.getDouble("estimator.individualSimilarityExponent")

  var similarityQualityAccuracyDelta = conf.getBoolean("estimator.similarityQualityAccuracyDelta")

  // BELL: train with runtimes of the same iteration only
  var trainingDataSameIterationOnly = conf.getBoolean("estimator.trainingData.sameIterationOnly")
  // BELL: train with executions of the same input dataset only
  var trainingDataSameDatasetOnly = conf.getBoolean("estimator.trainingData.sameDatasetOnly")

  // the number of times that thresholds are lowered if no similar jobs are found
  var lowerThresholdTimes = conf.getInt("estimator.lowerThresholdTimes")
  // the factor by which thresholds are lowered
  var lowerThresholdFactor = conf.getDouble("estimator.lowerThresholdFactor")

  // the limit on the similar executions set
  var maxSimilarJobs = conf.getInt("estimator.maxSimilarJobs")
  // similarities with a value greater than this are considered "very high"
  var veryHighSimilarity = conf.getDouble("estimator.veryHighSimilarity")

  // the job history id
  var jobHistory = conf.getString("estimator.jobHistory")

  // todo remove. always true
  var useDstartFromFirstIteratonStart = true
  
  var bellScaleoutConstraint = (1, 40) 
  
  
  def reload() = {
    // todo: remove
  }

  /**
   * return all values of the configuration as a key value list of strings.
   */
  def getAllValues(): ListBuffer[(String, String)] = {

    var l = ListBuffer[(String, String)]()
    val configVars = this.getClass.getDeclaredFields
    for (v <- configVars) {
      v.setAccessible(true)
      if (!v.getName.equals("conf")) {
        l += ((v.getName, v.get(this).toString))
      }
    }
    l
  }
}