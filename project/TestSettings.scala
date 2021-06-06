import org.scalastyle.sbt.ScalastylePlugin.autoImport.scalastyle
import sbt.Keys._
import sbt.{Def, Test, taskKey, _}

object TestSettings {
  lazy val testScalastyle = taskKey[Unit]("testScalastyle")
  testScalastyle := (Test / scalastyle).toTask("").value
  Test / test := ((Test / test) dependsOn testScalastyle).value

  lazy val IntegrationTest = config("it") extend Test
  lazy val testAll = TaskKey[Unit]("test-all")

  lazy val unitTest = Seq(
    Test / fork := true,
    Test / parallelExecution := false
  )
  lazy val itTest: Seq[Def.Setting[_]] = inConfig(IntegrationTest)(Defaults.testSettings) ++
    Seq(
      IntegrationTest / fork := true,
      IntegrationTest / parallelExecution := false,
      IntegrationTest / scalaSource := baseDirectory.value / "src" / "it" / "scala"
    )

  lazy val hoolokSettings: Seq[Def.Setting[_]] = unitTest ++ itTest ++
    Seq(testAll := (IntegrationTest / test).dependsOn(Test / test).value)

}
