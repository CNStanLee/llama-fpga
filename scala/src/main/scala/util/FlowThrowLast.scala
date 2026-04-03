package util

import spinal.core._
import spinal.lib._

object FlowThrowLast {

  def apply[T<:Data](input:Flow[Fragment[T]])={
    val ret = Flow(input.fragmentType)
    ret.valid := input.valid
    ret.payload := input.payload
    ret
  }

}
