package de.tuberlin.cit.progressestimator.util.reports
import de.tuberlin.cit.progressestimator.entity.JobExecution
import scala.collection.mutable.ListBuffer
import de.tuberlin.cit.progressestimator.estimator.JobExecutionWithEstimationDetails

/**
 * Create an html report with information on the selection
 * of similar jobs
 */
class SimilarityReportDummy extends SimilarityReport(null, null, null) {

  override def createReport(): String = {
    return "";
  }
  override def createHeading(): Unit = {
  }
  override def createStats(): Unit = {
  }
  override def createLogs(): Unit = {
  }
  override def createAccordion(): Unit = {
  }
  override def createEventList(): Unit = {
  }

}