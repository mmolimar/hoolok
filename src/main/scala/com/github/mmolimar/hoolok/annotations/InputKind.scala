package com.github.mmolimar.hoolok.annotations

case class InputBatchKind(subtype: String) extends BatchKind(subtype = subtype)

case class InputStreamKind(subtype: String) extends StreamKind(subtype = subtype)
