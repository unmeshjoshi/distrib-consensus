import Settings._

val `distrib-consensus` = project
  .in(file("."))
  .enablePlugins(DeployApp, DockerPlugin)
  .settings(defaultSettings: _*)
  .settings(
     libraryDependencies ++= Dependencies.Dist, parallelExecution in Test := false
)


