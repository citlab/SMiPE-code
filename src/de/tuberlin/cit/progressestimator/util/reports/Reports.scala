package de.tuberlin.cit.progressestimator.util.reports
import de.tuberlin.cit.progressestimator.entity.JobExecution

/**
 * class creating reports and writing them to a file
 */
trait Reports {
  def createJobReport(job: JobExecution): String
  def writeFile(content: String, fileName: String): Unit
}