
lazy val root = (project in file("."))
  .settings(
    name         := "BigData",
    organization := "addinn.kais",
    scalaVersion := "2.11.12",
    version      := "0.1.0-SNAPSHOT",
    libraryDependencies += "org.scalafx" % "scalafx_2.11" % "8.0.102-R11",
    libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.4.0",
    libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.4.0",
    libraryDependencies += "org.apache.spark" % "spark-mllib_2.11" % "2.4.0",
    libraryDependencies += "org.apache.spark" % "spark-graphx_2.11" % "2.4.0"

  )
