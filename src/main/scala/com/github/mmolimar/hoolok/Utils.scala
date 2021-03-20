package com.github.mmolimar.hoolok

import com.github.mmolimar.hoolok.annotations.{InputKind, OutputKind, StepKind}
import com.github.mmolimar.hoolok.inputs.Input
import com.github.mmolimar.hoolok.outputs.Output
import com.github.mmolimar.hoolok.steps.Step
import org.reflections.Reflections

import java.lang.reflect.Modifier
import scala.collection.JavaConverters._
import scala.reflect.runtime.universe._

object Utils {

  private val reflect = new Reflections("")

  def inspectInputs: Map[String, Class[_ <: Input]] = {
    classesByName[InputKind, Input]
  }

  def inspectSteps: Map[String, Class[_ <: Step]] = {
    classesByName[StepKind, Step]
  }

  def inspectOutputs: Map[String, Class[_ <: Output]] = {
    classesByName[OutputKind, Output]
  }

  private def classesByName[A: TypeTag, C: TypeTag]: Map[String, Class[_ <: C]] = {
    val mirror = runtimeMirror(getClass.getClassLoader)
    val baseClass = Class.forName(typeOf[C].typeSymbol.asClass.fullName)
    reflect.getSubTypesOf(baseClass).asScala
      .filter { subtype =>
        val modifiers = subtype.getModifiers
        !Modifier.isInterface(modifiers) && !Modifier.isAbstract(modifiers) && Modifier.isPublic(modifiers)
      }
      .map { subtype =>
        val name = mirror.staticClass(subtype.getName).annotations
          .find(_.tree.tpe =:= typeOf[A])
          .flatMap { a =>
            a.tree.children.tail.collectFirst {
              case Literal(Constant(name: String)) => name
            }
          }
        name -> subtype
      }
      .collect {
        case (Some(n), c: Class[_]) => n -> c.asInstanceOf[Class[C]]
      }
      .toMap
  }

}
