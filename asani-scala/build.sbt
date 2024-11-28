ThisBuild / version := "0.1.0"

ThisBuild / scalaVersion := "3.3.4"

val scalaLibs = Seq(
  "com.softwaremill.magnolia1_3" %% "magnolia" % V.magnolia
)

val testing = Seq(
  "org.scalatest" %% "scalatest" % V.scalatest % Test)

val arrow = Seq(
  "org.apache.arrow" % "arrow-vector" % V.arrow,
  "org.apache.arrow" % "flight-core" % V.arrow
)

lazy val root = (project in file("."))
  .settings(
    name := "asani-scala",
    libraryDependencies ++= scalaLibs ++ arrow ++ testing
  )
