import sbt._
import Keys._
import sbtassembly.Plugin._
import AssemblyKeys._

object Build extends Build {
  val printDependencyClasspath = taskKey[Unit]("Prints location of the dependencies")
  
  val sharedSettings = Defaults.coreDefaultSettings ++ net.virtualvoid.sbt.graph.Plugin.graphSettings ++ assemblySettings ++ Seq(
     organization := "com.twitter",
     scalaVersion := "2.10.4",
     javacOptions ++= Seq("-source", "1.7", "-target", "1.7"),
     javacOptions in doc := Seq("-source", "1.7"),
     libraryDependencies ++= Seq(
       "org.mockito" % "mockito-all" % "1.8.5" % "test",
       "org.scalatest" %% "scalatest" % "2.2.4"
     ),
     resolvers ++= Seq(
        "Local Maven Repository" at "file://" + Path.userHome.absolutePath + "/.m2/repository",
        "snapshots" at "https://oss.sonatype.org/content/repositories/snapshots",
        "releases" at "https://oss.sonatype.org/content/repositories/releases",
        "Concurrent Maven Repo" at "http://conjars.org/repo",
        "Clojars Repository" at "http://clojars.org/repo",
        "Twitter Maven" at "http://maven.twttr.com"
      ),
      javaOptions in Test ++= Seq("-Xmx2048m", "-XX:ReservedCodeCacheSize=384m", "-XX:MaxPermSize=384m"),
      scalacOptions ++= Seq("-unchecked", "-deprecation"),
      logLevel in assembly := Level.Warn,
      libraryDependencies ++= Seq(
        "com.twitter" % "algebird-core_2.10" % "0.7.0",
        "com.twitter" % "bijection-core_2.10" % "0.6.3",
        "com.twitter" % "storehaus-algebra_2.10" % "0.9.0",
        "com.twitter" % "storehaus-core_2.10" % "0.9.0",
        "com.twitter" % "util-collection_2.10" % "6.11.0",
        "com.twitter" % "util-core_2.10" % "6.11.0",
        "com.twitter" % "util-logging_2.10" % "6.13.2"
      )
  )
  
  lazy val lsh = Project(id = "hellsh", base = file("."), settings = sharedSettings)
}
       