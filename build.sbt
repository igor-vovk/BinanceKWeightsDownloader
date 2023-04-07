ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.10"

addCompilerPlugin("com.olegpy" %% "better-monadic-for" % "0.3.1")

lazy val root = (project in file("."))
  .settings(
    name := "ArchiveDownloader"
  )

libraryDependencies ++= Seq(
  "com.typesafe" % "config" % "1.4.2",
  "com.iheart" %% "ficus" % "1.5.2",

  "mysql" % "mysql-connector-java" % "8.0.32",
  //  "org.xerial" % "sqlite-jdbc" % "3.40.1.0",
  "org.scalikejdbc" %% "scalikejdbc" % "4.0.0",
  "org.scalikejdbc" %% "scalikejdbc-config" % "4.0.0",

  "org.typelevel" %% "cats-effect" % "3.4.8",
  "org.scalactic" %% "scalactic" % "3.2.15",

  "ch.qos.logback"  %  "logback-classic"   % "1.4.6",
  "org.scalatest" %% "scalatest" % "3.2.15" % "test"
)