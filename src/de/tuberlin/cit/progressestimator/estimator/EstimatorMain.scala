package de.tuberlin.cit.progressestimator.estimator

import java.io.IOException

import com.google.inject.Guice
import com.google.inject.Singleton

import de.tuberlin.cit.progressestimator.GuiceModule
import de.tuberlin.cit.progressestimator.rest.RestServer

/**
 * The main method starts up the REST server providing an API
 * to use the estimator functionality.
 */
object EstimatorMain {
  val injector = Guice.createInjector(new GuiceModule())
  def main(args: Array[String]) {
    startRestServer()
  }
  def startRestServer(): Unit = {
    try {
      val restServer = injector.getInstance(classOf[RestServer])
    } catch {
      case e: IOException => e.printStackTrace()
    }
  }
}
