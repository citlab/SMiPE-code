package de.tuberlin.cit.progressestimator.estimator;

import de.tuberlin.cit.progressestimator.similarity.Similarity

/**
 * This class extract trained values for the similarity thresholds.
 * It stores and retrieves similarity quality data which is used to find the 
 * thresholds.
 * 
 */
trait DynamicSimilarityThresholdExtractor {
 def addHistogramToCache(jobName: String, sim : Similarity, histogram: Array[(Double, Double)], count: Array[(Double, Double)])
  def getThreshold(jobName: String, sim: Similarity): Double
}