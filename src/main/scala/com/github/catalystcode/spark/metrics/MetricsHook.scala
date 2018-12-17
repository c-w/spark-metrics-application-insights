package com.github.catalystcode.spark.metrics

import java.util.concurrent.TimeUnit

import com.codahale.metrics.MetricRegistry
import com.microsoft.applicationinsights.log4j.v1_2.ApplicationInsightsAppender
import org.apache.log4j.Logger
import org.apache.spark.SparkEnv
import org.apache.spark.metrics.sink.AppInsightsSink
import org.apache.spark.metrics.sink.AppInsightsSink._

object MetricsHook {
  private var sink: Option[AppInsightsSink] = None

  def getRegistry: MetricRegistry = {
    val sparkEnv = SparkEnv.get
    val msMethod = sparkEnv.getClass.getDeclaredMethod("metricsSystem")
    msMethod.setAccessible(true)
    val msObj = msMethod.invoke(sparkEnv)
    val regField = msObj.getClass.getDeclaredField("org$apache$spark$metrics$MetricsSystem$$registry")
    regField.setAccessible(true)
    regField.get(msObj).asInstanceOf[MetricRegistry]
  }

  def injectLogAppender(instKey: String): Unit = {
    val appender = new ApplicationInsightsAppender()
    appender.setInstrumentationKey(instKey)
    appender.activateOptions()
    Logger.getRootLogger.addAppender(appender)
    Logger.getRootLogger.info(s"${appender.getClass.getSimpleName} injected!")
  }

  def hook(
    instrKey: String,
    period: Int = AI_DEFAULT_PERIOD,
    periodUnit: TimeUnit = AI_DEFAULT_PERIOD_UNIT,
    rateUnit: TimeUnit = AI_DEFAULT_RATE_UNIT,
    durationUnit: TimeUnit = AI_DEFAULT_DURATION_UNIT,
    name: String = "",
    prefix: String = ""
  ): Unit = {
    val registry = getRegistry
    injectLogAppender(instrKey)

    if (sink.isEmpty) {
      val newSink = AppInsightsSink.create(registry, instrKey,period, periodUnit, rateUnit, durationUnit, name, prefix)
      sink = Some(newSink)
      newSink.start()
    }
  }

  def flushMetrics(): Unit = {
    sink.foreach(_.report())
  }
}
