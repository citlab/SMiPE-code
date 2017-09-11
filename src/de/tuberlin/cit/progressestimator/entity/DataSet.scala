package de.tuberlin.cit.progressestimator.entity

/**
 * This represents an Input Dataset of an execution.
 */

class DataSet extends Serializable {
  var name : String = _
  var file : String = _
  var size : Long = _
  var numberOfRecords : Long = _
}