package de.tuberlin.cit.progressestimator.simulator

import de.tuberlin.cit.progressestimator.entity.JobExecution
import de.tuberlin.cit.progressestimator.estimator.SimilarityWeightsSolver

/**
 * This encapsulates a simulation request.
 */
class SimulationRequest(val iteration : Int, val currentJobId : Int, val weightsSolver : SimilarityWeightsSolver = null) {
  
  var jobHistoryFilter : JobExecution => Boolean = j => true
}