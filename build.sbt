

name := "spark-prediction"
version := "1.0"
scalaVersion := "2.11.8"

resolvers ++= Seq(
  Resolver.sonatypeRepo("releases"),
  Resolver.sonatypeRepo("snapshots")
)

libraryDependencies ++= Seq(
  "org.rogach"                % "scallop_2.11"          % "2.0.0",
  "org.apache.spark"          % "spark-core_2.11"       % "2.0.0" % "provided",
  "org.apache.spark"          % "spark-mllib_2.11"      % "2.0.0" % "provided",
  "org.sameersingh.scalaplot" % "scalaplot"             % "0.0.4"
)

parallelExecution in Test := false

artifact in (Compile, assembly) := {
  val art = (artifact in (Compile, assembly)).value
  art.copy(`classifier` = Some("assembly"))
}
addArtifact(artifact in (Compile, assembly), assembly)
publishArtifact := true
    