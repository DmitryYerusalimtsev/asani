ThisBuild / organization := "org.dyeru"
ThisBuild / version := "0.1.0"
ThisBuild / scalaVersion := "3.3.4"
ThisBuild / publishMavenStyle := true
ThisBuild / description := "Lightning-fast communication framework based on Apache Arrow and gRPC.\n\nThere are 2 implementation planned:\n\nIPC using Apache Arrow memory-mapped files\nRPC using Arrow Flight"
ThisBuild / licenses := Seq(
  "Apache-2.0" -> url("https://www.apache.org/licenses/LICENSE-2.0")
)

homepage := Some(url("https://github.com/DmitryYerusalimtsev/asani"))

developers := List(
  Developer(
    id = "DmitryYerusalimtsev",
    name = "Dmitry Yerusalimtsev",
    email = "dima.yerusalimtsev@gmail.com",
    url = url("https://www.linkedin.com/in/dmytro-yerusalimtsev-9bba8b58/")
  )
)

scmInfo := Some(
  ScmInfo(
    url("https://github.com/DmitryYerusalimtsev/asani"),
    "scm:git@github.com:DmitryYerusalimtsev/asani.git"
  )
)

publishTo := Some(
  "GitHub DmitryYerusalimtsev Apache Maven Packages" at "https://maven.pkg.github.com/DmitryYerusalimtsev/asani"
)

credentials += Credentials(
  "GitHub Package Registry",
  "maven.pkg.github.com",
  "DmitryYerusalimtsev",
  System.getenv("GITHUB_TOKEN")
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
    libraryDependencies ++= arrow ++ testing
  )