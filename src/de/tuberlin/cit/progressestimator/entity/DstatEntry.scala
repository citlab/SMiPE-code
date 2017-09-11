package de.tuberlin.cit.progressestimator.entity
import java.time.Instant
/**
 * This represents a Dstat entry
 */
class DstatEntry extends Serializable {
  var time: Instant = _
  var host: String = _
  var property: String = _
  var value: Double = _
}