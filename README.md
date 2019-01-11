# spark-metrics-application-insights

[![Build Status](https://travis-ci.org/CatalystCode/spark-metrics-application-insights.svg?branch=master)](https://travis-ci.org/CatalystCode/spark-metrics-application-insights)

## What's this?

Spark Metrics integration for [Application Insights](https://docs.microsoft.com/en-us/azure/application-insights/app-insights-overview).

## Usage example

```scala
import com.github.catalystcode.spark.telemetry

val appInsightsKey = "SET ME"
telemetry.setup(appInsightsKey)

val timerName = "SET ME"
telemetry.timer(timerName).timeFn({
  // some spark code here
})

telemetry.flush()
```
