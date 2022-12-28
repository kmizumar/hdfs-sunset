lazy val commonSettings = Seq(
  version := "0.0.1-SNAPSHOT",
  organization := "org.example",
  scalaVersion := "2.11.12"
)

lazy val sparkVersion = "2.4.7"
lazy val scoptVersion = "4.1.0"
lazy val scalatestVersion = "3.2.14"

lazy val root = (project in file("."))
  .settings(
    commonSettings,
    assemblySettings,
    name := "hdfs-sunset",
    assembly / mainClass := Some("hdfs.sunset.App"),
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
      "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
      "com.github.scopt" %% "scopt" % scoptVersion,
      "org.scalactic" %% "scalactic" % scalatestVersion,
      "org.scalatest" %% "scalatest" % scalatestVersion % Test
    ),
    assemblyJarName := { s"hdfs-sunset-${version.value}.jar" }
  )

lazy val assemblySettings = Seq(
  assembly / assemblyMergeStrategy := {
    case PathList("org", "apache", xs@_*) => MergeStrategy.last
    case "about.html" => MergeStrategy.rename
    case "META-INF/ECLIPSEF.RSA" => MergeStrategy.last
    case "META-INF/mailcap" => MergeStrategy.last
    case "META-INF/mimetypes.default" => MergeStrategy.last
    case "plugin.properties" => MergeStrategy.last
    case "log4j.properties" => MergeStrategy.last
    case "module-info.class" => MergeStrategy.discard
    case x =>
      val oldStrategy = (assembly / assemblyMergeStrategy).value
      oldStrategy(x)
  }
)
