package com.github.mmolimar.hoolok

package object annotations {

  class HoolokAnnotation extends scala.annotation.StaticAnnotation

  private[annotations] class StreamKind(kind: String = "stream", subtype: String) extends HoolokAnnotation

  private[annotations] class BatchKind(kind: String = "batch", subtype: String) extends HoolokAnnotation

}
