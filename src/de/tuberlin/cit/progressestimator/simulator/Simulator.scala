package de.tuberlin.cit.progressestimator.simulator

/**
 * The simulator performs simulations of estimates.
 */
trait Simulator {
  def simulate(jobId : Int, iteration : Int): String
  def simulate(request : SimulationRequest, createReport: Boolean = true): SimulationResponse
}