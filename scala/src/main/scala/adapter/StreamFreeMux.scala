package adapter

import spinal.core._
import spinal.lib._

import scala.language.postfixOps

object StreamFreeMux {

  def apply[T <: Data](input: Vec[Stream[T]]) = {
    val ret = Stream(input.head.payload)
    ret.valid := input.map(_.valid).orR
    ret.payload := MuxOH(
      input.map(_.valid),
      input.map(_.payload)
    )
    input.foreach(_.ready := ret.ready)
    ret
  }
}
