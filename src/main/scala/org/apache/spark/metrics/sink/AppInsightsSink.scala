package org.apache.spark.metrics.sink

import java.util.Properties
import java.util.concurrent.TimeUnit

import com.codahale.metrics.MetricRegistry
import com.github.shtratos.metrics.appinsights.AppInsightsReporter
import com.microsoft.applicationinsights.{TelemetryClient, TelemetryConfiguration}
import org.apache.spark.internal.Logging
import org.apache.spark.metrics.MetricsSystem
import org.apache.spark.metrics.sink.AppInsightsSink._

import scala.collection.mutable

class AppInsightsSink(
  val property: Properties,
  val registry: MetricRegistry
) extends Sink with Logging {

  private def getProp(prop: String): Option[String] = Option(property.getProperty(prop))
  private def getTimeUnitProp(prop: String, default: TimeUnit): TimeUnit = getProp(prop).map(_.toUpperCase).map(TimeUnit.valueOf).getOrElse(default)
  private def getIntProp(prop: String, default: Int): Int = getProp(prop).map(_.toInt).getOrElse(default)

  if (getProp(AI_INSTKEY_KEY).isEmpty) {
    throw new IllegalArgumentException(s"${getClass.getSimpleName} requires option $AI_INSTKEY_KEY")
  }

  @transient val instrumentationKey: String = getProp(AI_INSTKEY_KEY).get
  @transient val reportedName: Option[String] = getProp(AI_NAME_KEY)
  @transient val metricPrefix: Option[String] = getProp(AI_PREFIX_KEY)
  @transient val pollPeriod: Int = getIntProp(AI_PERIOD_KEY, AI_DEFAULT_PERIOD)
  @transient val pollUnit: TimeUnit = getTimeUnitProp(AI_PERIOD_UNIT_KEY, AI_DEFAULT_PERIOD_UNIT)
  @transient val rateUnit: TimeUnit = getTimeUnitProp(AI_RATE_UNIT_KEY, AI_DEFAULT_RATE_UNIT)
  @transient val durationUnit: TimeUnit = getTimeUnitProp(AI_DURATION_UNIT_KEY, AI_DEFAULT_DURATION_UNIT)

  private val reporters = mutable.MutableList.empty[AppInsightsReporter]

  def addRegistry(reg: MetricRegistry, start: Boolean = false): Unit = {
    val telemetryConfig = TelemetryConfiguration.createDefault()
    telemetryConfig.setInstrumentationKey(instrumentationKey)

    val telemetryClient = new TelemetryClient(telemetryConfig)

    val baseReporter = AppInsightsReporter
      .forRegistry(reg)
      .telemetryClient(telemetryClient)
      .durationUnit(durationUnit)
      .rateUnit(rateUnit)

    val withName = reportedName match {
      case Some(s) => baseReporter.name(s)
      case None => baseReporter
    }

    val withPrefix = metricPrefix match {
      case Some(p) => withName.metricNamePrefix(p)
      case None => withName
    }

    val reporter = withPrefix.build()
    reporters += reporter

    if (start) {
      reporter.start(pollPeriod, pollUnit)
    }
  }

  override def start(): Unit = {
    reporters.foreach(_.start(pollPeriod, pollUnit))
    logInfo(s"${getClass.getSimpleName} started with prefix: '$metricPrefix'")
  }

  override def stop(): Unit = {
    reporters.foreach(_.stop())
    logInfo(s"${getClass.getSimpleName} stopped")
  }

  override def report(): Unit = reporters.foreach(_.report())

  MetricsSystem.checkMinimalPollingPeriod(pollUnit, pollPeriod)

  addRegistry(registry)

  AppInsightsSink.instance = Some(this)
}

object AppInsightsSink {
  val AI_DEFAULT_PERIOD = 10
  val AI_DEFAULT_PERIOD_UNIT = TimeUnit.SECONDS
  val AI_DEFAULT_RATE_UNIT = TimeUnit.SECONDS
  val AI_DEFAULT_DURATION_UNIT = TimeUnit.MILLISECONDS

  val AI_INSTKEY_KEY = "instrumentationkey"
  val AI_NAME_KEY = "name"
  val AI_PREFIX_KEY = "prefix"
  val AI_PERIOD_KEY = "period"
  val AI_PERIOD_UNIT_KEY = "periodunit"
  val AI_RATE_UNIT_KEY = "rateunit"
  val AI_DURATION_UNIT_KEY = "durationunit"

  var instance: Option[AppInsightsSink] = None

  def create(
    registry: MetricRegistry,
    instrKey: String,
    period: Int = AI_DEFAULT_PERIOD,
    periodUnit: TimeUnit = AI_DEFAULT_PERIOD_UNIT,
    rateUnit: TimeUnit = AI_DEFAULT_RATE_UNIT,
    durationUnit: TimeUnit = AI_DEFAULT_DURATION_UNIT,
    name: String = "",
    prefix: String = ""
  ): AppInsightsSink = {
    val props = new Properties()
    props.setProperty(AI_INSTKEY_KEY, instrKey)
    props.setProperty(AI_PERIOD_KEY, period.toString)
    props.setProperty(AI_PERIOD_UNIT_KEY, periodUnit.toString)
    props.setProperty(AI_RATE_UNIT_KEY, rateUnit.toString)
    props.setProperty(AI_DURATION_UNIT_KEY, durationUnit.toString)

    if (name.nonEmpty) {
      props.setProperty(AI_NAME_KEY, name)
    }

    if (prefix.nonEmpty) {
      props.setProperty(AI_PREFIX_KEY, prefix)
    }

    new AppInsightsSink(props, registry)
  }
}
