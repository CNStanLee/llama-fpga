package util

import spinal.core._
import spinal.lib._

object FlowAddLast {

  def apply[T<:Data](input:Flow[T],last:Bool)={
    val ret = Flow(Fragment(input.payloadType))
    ret.valid := input.valid
    ret.last := last
    ret.fragment := input.payload
    ret
  }

}
