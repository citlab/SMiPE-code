package de.tuberlin.cit.progressestimator.entity
import java.time.{Instant, Duration}

/**
 * This represents an iteration of an execution
 */
class Iteration extends Serializable {
  var nr : Int = _
  var timeStart : Instant = _
  var timeFinish : Instant = _
  def runtime : Double = Duration.between(timeStart, timeFinish).toMillis().toDouble / 1000
  var input : DataSet = new DataSet
  var resourceConfig: ResourceConfiguration = new ResourceConfiguration
}