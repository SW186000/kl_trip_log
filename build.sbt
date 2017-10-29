name := "kl_trip_log"

version := "0.1"

scalaVersion := "2.11.11"

resolvers += "mapr-release" at "http://repository.mapr.com/maven"

libraryDependencies ++= {
  Seq("org.apache.spark" %% "spark-core" % "2.0.1-mapr-1611" % "provided" withSources(),
    "org.apache.spark" %% "spark-sql" % "2.0.1-mapr-1611" % "provided" withSources(),
    "org.apache.spark" %% "spark-hive" % "2.0.1-mapr-1611" % "provided" withSources()
  )
}

  assemblyMergeStrategy in assembly := {
    case PathList("javax", "servlet", xs @ _*) => MergeStrategy.last
    case PathList("javax", "activation", xs @ _*) => MergeStrategy.last
    case PathList("org", "apache", xs @ _*) => MergeStrategy.last
    case PathList("com", "google", xs @ _*) => MergeStrategy.last
    case PathList("com", "esotericsoftware", xs @ _*) => MergeStrategy.last
    case PathList("com", "codahale", xs @ _*) => MergeStrategy.last
    case PathList("com", "yammer", xs @ _*) => MergeStrategy.last
    case PathList("javax", xs @ _*) => MergeStrategy.last
    case PathList("javax.jdo", xs @ _*) => MergeStrategy.last
    case PathList("javax.xml.stream", xs @ _*) => MergeStrategy.last
    case "about.html" => MergeStrategy.rename
    case "META-INF/ECLIPSEF.RSA" => MergeStrategy.last
    case "META-INF/mailcap" => MergeStrategy.last
    case "META-INF/mimetypes.default" => MergeStrategy.last
    case "plugin.properties" => MergeStrategy.last
    case "log4j.properties" => MergeStrategy.last
    case x =>
      val oldStrategy = (assemblyMergeStrategy in assembly).value
      oldStrategy(x)
  }



