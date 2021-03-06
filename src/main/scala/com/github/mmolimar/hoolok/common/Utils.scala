package com.github.mmolimar.hoolok.common

import com.github.mmolimar.hoolok.annotations._
import com.github.mmolimar.hoolok.inputs.Input
import com.github.mmolimar.hoolok.outputs.{Output, StreamingPolicy}
import com.github.mmolimar.hoolok.schemas.Schema
import com.github.mmolimar.hoolok.steps.Step
import org.reflections.Reflections

import java.lang.reflect.Modifier
import scala.collection.JavaConverters._
import scala.reflect.runtime.universe._

object Utils {

  private val reflect = new Reflections("")

  def inspectSchemas: Map[String, Class[_ <: Schema]] = {
    classesByName[SchemaKind, Schema]
  }

  def inspectBatchInputs: Map[String, Class[_ <: Input]] = {
    classesByName[InputBatchKind, Input]
  }

  def inspectStreamInputs: Map[String, Class[_ <: Input]] = {
    classesByName[InputStreamKind, Input]
  }

  def inspectSteps: Map[String, Class[_ <: Step]] = {
    classesByName[StepKind, Step]
  }

  def inspectBatchOutputs: Map[String, Class[_ <: Output]] = {
    classesByName[OutputBatchKind, Output]
  }

  def inspectStreamOutputs: Map[String, Class[_ <: Output]] = {
    classesByName[OutputStreamKind, Output]
  }

  def inspectStreamingPolicies: Map[String, Class[_ <: StreamingPolicy]] = {
    classesByName[StreamingPolicyKind, StreamingPolicy]
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

  def closer[C <: AutoCloseable, R](resource: C)(block: C => R): R = {
    try {
      block(resource)
    } finally {
      resource.close()
    }
  }

}
