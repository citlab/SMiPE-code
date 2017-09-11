package de.tuberlin.cit.progressestimator.util.reports
import de.tuberlin.cit.progressestimator.entity.JobExecution
import scala.collection.mutable.ListBuffer
import de.tuberlin.cit.progressestimator.estimator.EstimatorConfig
 
/**
 * Create an html report with information on the selection
 * of similar jobs
 */
class SimilarityQualityReport(val config: EstimatorConfig) extends Report {

  templatePreCode()
  createHeading()
  def createReport(): String = {
    addConfig()
    templateAfterCode()
    return c.toString;
  }
  def createHeading(): Unit = {
    c.append( "<h1>Similarity Quality Report</h1>")
  }
  private def addConfig()  = {
     c.append("<h3>Config</h3>")
     for(v <- config.getAllValues()){
      c.append(s"<div><span style='display: inline-block; width: 450px;'>${v._1}:</span> ${v._2}</div>")
    }
  }
  
}