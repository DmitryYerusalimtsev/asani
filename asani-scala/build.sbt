ThisBuild / version := "0.1.0"
ThisBuild / scalaVersion := "3.3.4"

val testing = Seq(
  "org.scalatest" %% "scalatest" % V.scalatest % Test)

val arrow = Seq(
  "org.apache.arrow" % "arrow-vector" % V.arrow,
  "org.apache.arrow" % "flight-core" % V.arrow
)

lazy val root = (project in file("."))
  .settings(
    name := "asani-scala",
    libraryDependencies ++= arrow ++ testing
  )
