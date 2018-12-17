package com.github.catalystcode.spark

import java.util.concurrent.TimeUnit

import com.codahale.metrics._
import com.github.catalystcode.spark.metrics.MetricsHook
import org.apache.spark.metrics.CustomMetricsSource
import org.apache.spark.metrics.sink.AppInsightsSink._

package object telemetry {
  class SparkTimer extends Timer {
    def timeFn[T](block: => T): T = {
      val context = super.time()
      try {
        block
      } finally {
        context.close()
      }
    }
  }

  class SparkGauge[T](default: T) extends Gauge[T] {
    private var box: T = default

    override def getValue: T = box

    def setValue(newValue: T): Unit = box = newValue
  }

  type SparkHistogram = Histogram
  type SparkCounter = Counter
  type SparkMeter = Meter

  def counter(name: String): SparkCounter = CustomMetricsSource.counter(name)
  def gauge[T](name: String, default: T): SparkGauge[T] = CustomMetricsSource.gauge(name, default)
  def histogram(name: String): SparkHistogram = CustomMetricsSource.histogram(name)
  def meter(name: String): SparkMeter = CustomMetricsSource.meter(name)
  def timer(name: String): SparkTimer = CustomMetricsSource.timer(name)

  def setup(
    instrKey: String,
    period: Int = AI_DEFAULT_PERIOD,
    periodUnit: TimeUnit = AI_DEFAULT_PERIOD_UNIT,
    rateUnit: TimeUnit = AI_DEFAULT_RATE_UNIT,
    durationUnit: TimeUnit = AI_DEFAULT_DURATION_UNIT,
    name: String = "",
    prefix: String = ""
  ): Unit = {
    MetricsHook.hook(instrKey, period, periodUnit, rateUnit, durationUnit, name, prefix)
  }

  def flush(): Unit = MetricsHook.flushMetrics()
}
