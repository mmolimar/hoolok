import org.scalastyle.sbt.ScalastylePlugin.autoImport.scalastyle
import sbt.Keys._
import sbt.{Def, Test, taskKey, _}

object TestSettings {
  lazy val testScalastyle = taskKey[Unit]("testScalastyle")
  testScalastyle := scalastyle.in(Test).toTask("").value
  (test in Test) := ((test in Test) dependsOn testScalastyle).value

  lazy val IntegrationTest = config("it") extend Test
  lazy val testAll = TaskKey[Unit]("test-all")

  lazy val unitTest = Seq(
    fork in Test := true,
    parallelExecution in Test := false
  )
  lazy val itTest: Seq[Def.Setting[_]] = inConfig(IntegrationTest)(Defaults.testSettings) ++
    Seq(
      fork in IntegrationTest := true,
      parallelExecution in IntegrationTest := false,
      scalaSource in IntegrationTest := baseDirectory.value / "src" / "it" / "scala"
    )

  lazy val hoolokSettings: Seq[Def.Setting[_]] = unitTest ++ itTest ++
    Seq(testAll := (test in IntegrationTest).dependsOn(test in Test).value)

}
