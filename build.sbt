name := "spark-metrics-application-insights"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.0" % "provided"
libraryDependencies += "io.dropwizard.metrics" % "metrics-core" % "3.1.5"
libraryDependencies += "com.microsoft.azure" % "applicationinsights-logging-log4j1_2" % "2.2.0"
libraryDependencies += "com.microsoft.azure" % "applicationinsights-core" % "2.2.0" excludeAll(
  ExclusionRule(organization = "org.apache.http"),
  ExclusionRule(organization = "eu.infomas.annotation"),
  ExclusionRule(organization = "org.apache.commons"),
  ExclusionRule(organization = "javax.annotation"),
  ExclusionRule(organization = "com.google"),
  ExclusionRule(organization = "io.grpc"),
  ExclusionRule(organization = "io.opencensus"))

test in assembly := {}

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

artifact in (Compile, assembly) := {
  val art = (artifact in (Compile, assembly)).value
  art.copy(`classifier` = Some("assembly"))
}

addArtifact(artifact in (Compile, assembly), assembly)

assemblyMergeStrategy in assembly := {
  case PathList("org", "apache", "spark", "metrics", "sink", "AppInsightsSink", xs@ _*) => MergeStrategy.first
  case PathList("org", "apache", "spark", "metrics", "CustomMetricsSource", xs@ _*) => MergeStrategy.first
  case PathList("org", "slf4j", xs @ _*) => MergeStrategy.discard
  case PathList("org", "apache", "log4j", xs @ _*) => MergeStrategy.discard
  case PathList("META-INF", _ @ _*) => MergeStrategy.discard
  case _ => MergeStrategy.first
}
