name := "SomeTestActivity"

version := "1.0"

scalaVersion := "2.12.3"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

mainClass in assembly := Some("HdfsWorker")

libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-client" % "2.7.3",
  "org.scala-lang" % "scala-library" % "2.12.3"
)
