package util

import spinal.core._
import spinal.lib._

import scala.language.postfixOps

object FlowLastAlign {

  def apply[T <: Data](input: Flow[T], last: Bool) = {
    val ret = Flow(Fragment(input.payload))
    val pipe = input.toStream.m2sPipe()
    val syncLast = input.valid & last
    val notSyncLast = !input.valid & last
    val dlyLast = RegNext(syncLast, init = False)
    pipe.ready := input.valid || last || dlyLast

    ret.valid := pipe.fire
    ret.fragment := pipe.payload
    ret.last := notSyncLast || dlyLast
    ret
  }

}
