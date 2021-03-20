package com.github.mmolimar.hoolok

// scalastyle:off
object annotations {

  case class InputKind(kind: String) extends scala.annotation.StaticAnnotation

  case class StepKind(kind: String) extends scala.annotation.StaticAnnotation

  case class OutputKind(kind: String) extends scala.annotation.StaticAnnotation

}
