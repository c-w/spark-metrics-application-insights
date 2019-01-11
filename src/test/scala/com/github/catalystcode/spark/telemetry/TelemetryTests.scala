package com.github.catalystcode.spark.telemetry

import java.lang.System.getenv
import java.util.UUID.randomUUID

import com.github.catalystcode.spark.telemetry
import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.FunSuite

import scala.concurrent.duration.Duration

class TelemetryTests extends FunSuite with SharedSparkContext {
  private val InstrumentationKey = Option(getenv("TEST_APPLICATION_INSIGHTS_IKEY")).getOrElse("")

  test("counter") {
    val metricName = setup()

    val counter = telemetry.counter(metricName)

    counter.inc()
    counter.inc(3)
    counter.dec()
    counter.dec(2)
    assert(counter.getCount == 1)

    teardown(metricName)
  }

  test("gauge") {
    val metricName = setup()

    val gauge = telemetry.gauge(metricName, 1)
    assert(gauge.getValue == 1)

    gauge.setValue(2)
    assert(gauge.getValue == 2)

    teardown(metricName)
  }

  test("histogram") {
    val metricName = setup()

    val histogram = telemetry.histogram(metricName)

    histogram.update(2)
    assert(histogram.getSnapshot.getMin == 2)

    histogram.update(1)
    assert(histogram.getSnapshot.getMin == 1)

    teardown(metricName)
  }

  test("meter") {
    val metricName = setup()

    val meter = telemetry.meter(metricName)

    meter.mark()
    meter.mark()
    meter.mark()
    assert(meter.getCount == 3)

    teardown(metricName)
  }

  // scalastyle:off magic.number
  test("timer") {
    val metricName = setup()

    val rdd = sc.parallelize(Range(0, 1000))

    val timer = telemetry.timer(metricName).time()

    rdd.count()
    assert(timer.stop() < Duration(5, "seconds").toNanos)

    teardown(metricName)
  }
  // scalastyle:on magic.number

  private def setup(): String = {
    if (InstrumentationKey.isEmpty) {
      cancel("No application insights instrumentation key defined")
    }

    telemetry.setup(InstrumentationKey)

    randomUUID().toString
  }

  private def teardown(metricName: String): Unit = {
    telemetry.flush()

    // TODO: fetch metricName from application insights and assert it exists
  }
}
