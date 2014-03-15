/*import java.text.SimpleDateFormat

name := "ostrich"

version := "9.1.2.digaku-SNAPSHOT"

organization := "com.twitter"

crossScalaVersions := Seq("2.9.2", "2.10.0")

javacOptions ++= Seq("-source", "1.6", "-target", "1.6")

javacOptions in doc := Seq("-source", "1.6")

parallelExecution in Test := false

net.virtualvoid.sbt.graph.Plugin.graphSettings

resolvers += "twitter repo" at "http://maven.twttr.com"

libraryDependencies ++= Seq(
  "com.twitter" %% "util-core" % "6.3.7",
  "com.twitter" %% "util-eval" % "6.3.7",
  "com.twitter" %% "util-logging" % "6.3.7",
  "com.twitter" %% "util-jvm" % "6.3.7",
  "com.twitter" %% "scala-json" % "3.0.1",
  "com.netflix.astyanax" % "astyanax-core" % "1.56.37",
  "com.netflix.astyanax" % "astyanax-cassandra" % "1.56.37",
  "com.netflix.astyanax" % "astyanax-thrift" % "1.56.37",
  "com.netflix.astyanax" % "astyanax-recipes" % "1.56.37"
)

libraryDependencies ++= Seq(
  "org.scala-tools.testing" %% "specs" % "1.6.9" % "test" cross CrossVersion.binaryMapped {
    case "2.9.2" => "2.9.1"
    case "2.10.0" => "2.10"
    case x => x
  },
  "junit" % "junit" % "4.8.1" % "test",
  "cglib" % "cglib" % "2.1_3" % "test",
  "asm" % "asm" % "1.5.3" % "test",
  "org.objenesis" % "objenesis" % "1.1" % "test",
  "org.hamcrest" % "hamcrest-all" % "1.1" % "test",
  "org.jmock" % "jmock" % "2.4.0" % "test"
)

publishMavenStyle := true

//publishTo <<= version { (v: String) =>
//  val nexus = "https://oss.sonatype.org/"
//  if (v.trim.endsWith("SNAPSHOT"))
//    Some("sonatype-snapshots" at nexus + "content/repositories/snapshots")
//  else
//    Some("sonatype-releases"  at nexus + "service/local/staging/deploy/maven2")
//}

publishTo <<= version { (v:String) =>
  val repoUrl = "http://scala.repo.ansvia.com/nexus"
  if(v.trim.endsWith("SNAPSHOT") || """.+\-\d{8}+$""".r.pattern.matcher(v.trim).matches())
    Some("snapshots" at repoUrl + "/content/repositories/snapshots")
  else
    Some("releases" at repoUrl + "/service/local/staging/deploy/maven2")
}

version <<= version { (v:String) =>
  if (v.trim.endsWith("-SNAPSHOT")){
    val dateFormatter = new SimpleDateFormat("yyyyMMdd")
    v.trim.split("-").apply(0) + "-" + dateFormatter.format(new java.util.Date()) + "-SNAPSHOT"
  }else
    v
}

credentials += Credentials(Path.userHome / ".ivy2" / ".credentials-ansvia")


publishArtifact in Test := false

pomIncludeRepository := { x => false }

pomExtra := (
  <url>https://github.com/twitter/ostrich</url>
  <licenses>
    <license>
      <name>Apache 2</name>
      <url>http://www.apache.org/licenses/LICENSE-2.0.txt</url>
      <distribution>repo</distribution>
      <comments>A business-friendly OSS license</comments>
    </license>
  </licenses>
  <scm>
    <url>git@github.com:twitter/ostrich.git</url>
    <connection>scm:git:git@github.com:twitter/ostrich.git</connection>
  </scm>
  <developers>
    <developer>
      <id>twitter</id>
      <name>Twitter Inc.</name>
      <url>https://www.twitter.com/</url>
    </developer>
    <developer>
      <id>anvie</id>
      <name>Robin Sy</name>
      <url>https://www.twitter.com/anvie</url>
    </developer>
  </developers>
)
*/