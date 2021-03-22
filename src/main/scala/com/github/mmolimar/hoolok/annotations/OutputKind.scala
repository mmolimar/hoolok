package com.github.mmolimar.hoolok.annotations

case class OutputBatchKind(subtype: String) extends BatchKind(subtype = subtype)

case class OutputStreamKind(subtype: String) extends StreamKind(subtype = subtype)
