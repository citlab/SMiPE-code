package de.tuberlin.cit.progressestimator
import com.google.inject.AbstractModule
import com.google.inject.Scopes

import de.tuberlin.cit.progressestimator.costfactor.CostFactorExtractor
import de.tuberlin.cit.progressestimator.costfactor.CostFactorExtractorImpl
import de.tuberlin.cit.progressestimator.estimator.DynamicSimilarityThresholdExtractor
import de.tuberlin.cit.progressestimator.estimator.DynamicSimilarityThresholdExtractorImpl
import de.tuberlin.cit.progressestimator.estimator.Estimator
import de.tuberlin.cit.progressestimator.estimator.EstimatorConfig
import de.tuberlin.cit.progressestimator.estimator.EstimatorImpl
import de.tuberlin.cit.progressestimator.estimator.SimilarityMatching
import de.tuberlin.cit.progressestimator.estimator.SimilarityMatchingImpl
import de.tuberlin.cit.progressestimator.estimator.SimilarityWeightsSolver
import de.tuberlin.cit.progressestimator.history.CachedHistory
import de.tuberlin.cit.progressestimator.history.JobHistory
import de.tuberlin.cit.progressestimator.rest.RestServer
import de.tuberlin.cit.progressestimator.rest.SimilarityQualityService
import de.tuberlin.cit.progressestimator.rest.SimulationService
import de.tuberlin.cit.progressestimator.similarity.DstatCache
import de.tuberlin.cit.progressestimator.simulator.Simulator
import de.tuberlin.cit.progressestimator.simulator.SimulatorImpl
import de.tuberlin.cit.progressestimator.util.reports.Reports
import de.tuberlin.cit.progressestimator.util.reports.ReportsImpl

/**
 * This Guice module loads the dependencies for the estimator.
 */
class GuiceModule extends AbstractModule {

  override def configure() = {
    bind(classOf[EstimatorConfig])
    bind(classOf[JobHistory]).to(classOf[CachedHistory]).in(Scopes.SINGLETON)
    bind(classOf[Reports]).to(classOf[ReportsImpl])
    bind(classOf[Simulator]).to(classOf[SimulatorImpl])
    bind(classOf[RestServer])
    bind(classOf[Estimator]).to(classOf[EstimatorImpl])
    bind(classOf[DstatCache])
    bind(classOf[SimilarityQualityService])
    bind(classOf[SimulationService])
    bind(classOf[SimilarityWeightsSolver])
    bind(classOf[SimilarityMatching]).to(classOf[SimilarityMatchingImpl])
    bind(classOf[CostFactorExtractor]).to(classOf[CostFactorExtractorImpl])
    bind(classOf[DynamicSimilarityThresholdExtractor]).to(classOf[DynamicSimilarityThresholdExtractorImpl])
  }
}