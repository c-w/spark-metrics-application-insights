package org.apache.spark.metrics

import com.codahale.metrics.MetricRegistry
import com.github.catalystcode.spark.telemetry._
import org.apache.spark.metrics.sink.AppInsightsSink
import org.apache.spark.metrics.source.Source

import scala.collection.JavaConverters._

class CustomMetricsSource extends Source{
  private val registry = new MetricRegistry
  private var name: String = "Custom"

  override def metricRegistry: MetricRegistry = registry

  override def sourceName: String = name
}

object CustomMetricsSource {
  lazy val instance: CustomMetricsSource = {
    val source = new CustomMetricsSource()
    AppInsightsSink.instance.get.addRegistry(source.registry, start = true)
    source
  }

  def setName(name: String): Unit = {
    instance.name = name
  }

  def counter(name: String): SparkCounter = {
    instance.registry.counter(name)
  }

  def gauge[T](name: String, default: T): SparkGauge[T] = {
    instance.registry.getGauges.asScala.get(name) match {
      case Some(gauge) =>
        gauge.asInstanceOf[SparkGauge[T]]

      case None =>
        val gauge = new SparkGauge[T](default)
        instance.registry.register(name, gauge)
        gauge
    }
  }

  def histogram(name: String): SparkHistogram = {
    instance.registry.histogram(name)
  }

  def meter(name: String): SparkMeter = {
    instance.registry.meter(name)
  }

  def timer(name: String): SparkTimer = {
    instance.registry.getMetrics.asScala.get(name) match {
      case Some(timer) =>
        timer.asInstanceOf[SparkTimer]

      case None =>
        val timer = new SparkTimer
        instance.registry.register(name, timer)
        timer
    }
  }
}
