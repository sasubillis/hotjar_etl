import AssemblyKeys._ // put this at the top of the file,leave the next line blank
seq(assemblySettings: _*)

name := "hotjar-ingestion"

version := "0.0.1-SNAPSHOT"

scalaVersion := "2.11.8"




libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.0.2" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.0.2" % "provided",
  "org.apache.spark" %% "spark-streaming" % "2.0.2" % "provided",
  "com.typesafe.akka" %% "akka-actor" % "2.3.15" % "provided",
  "com.datastax.spark" %% "spark-cassandra-connector" % "2.0.0-M2" % "provided",
  "com.datastax.cassandra" % "cassandra-driver-core" % "3.1.2" % "provided",
  "org.apache.cassandra" % "cassandra-all" % "3.9",
  "net.java.dev.jna" % "jna" % "4.2.2",
  "joda-time" % "joda-time" % "2.9.3",
  "com.typesafe" % "config" % "1.3.0",
  "com.jcraft" % "jsch" % "0.1.53",
  "org.apache.commons" % "commons-csv" % "1.3",
  "org.bouncycastle" % "bcpg-jdk15on" % "1.46",
  "org.bouncycastle" % "bcprov-jdk15on" % "1.46"
)


mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
{
  case PathList("javax", "servlet", xs @ _*) => MergeStrategy.last
  case PathList("org", "apache", xs @ _*) => MergeStrategy.last
  case PathList("com", "esotericsoftware", xs @ _*) => MergeStrategy.last
  case "about.html" => MergeStrategy.rename
  case x => old(x)
}
}
