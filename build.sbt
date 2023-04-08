ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.10"

addCompilerPlugin("com.olegpy" %% "better-monadic-for" % "0.3.1")

lazy val root = (project in file("."))
  .settings(
    name := "ArchiveDownloader"
  )

libraryDependencies ++= Seq(
  // config
  "com.typesafe" % "config" % "1.4.2",
  "com.iheart" %% "ficus" % "1.5.2",
  //db
  "mysql" % "mysql-connector-java" % "8.0.32",
  //  "org.xerial" % "sqlite-jdbc" % "3.40.1.0",
  "org.scalikejdbc" %% "scalikejdbc" % "4.0.0",
  "org.scalikejdbc" %% "scalikejdbc-config" % "4.0.0",
  //fs2
  "co.fs2" %% "fs2-core" % "3.6.1",
  "co.fs2" %% "fs2-io" % "3.6.1",
  //http4s
  "org.http4s" %% "http4s-client" % "0.23.18",
  //cats
  "org.typelevel" %% "cats-effect" % "3.4.8",
  "org.scalactic" %% "scalactic" % "3.2.15",
  "ch.qos.logback" % "logback-classic" % "1.4.6",
  "org.scalatest" %% "scalatest" % "3.2.15" % "test"
)
