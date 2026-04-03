package util

import spinal.core._
import spinal.lib._

import scala.language.postfixOps

object StreamRepeat {
  def apply[T <: Data](src: Stream[T], bound: UInt) = {
    val ret = Stream(src.payloadType)
    val cnt = UInt(bound.getWidth bits).setAsReg().init(0)
    val cntOvf = cnt === bound
    when(ret.fire) {
      cnt := cnt + 1
      when(cntOvf) {
        cnt.clearAll()
      }
    }
    ret.valid := src.valid
    ret.payload := src.payload
    src.ready := ret.ready & cntOvf
    ret
  }

  def apply[T <: Data](src: Stream[T], bound: List[UInt]) = {
    val ret = Stream(src.payloadType)
    val (_, cntOvf) = LoopsCntGen.wireOvf(bound, ret.fire)
    ret.valid := src.valid
    ret.payload := src.payload
    src.ready := ret.ready & cntOvf.reduce(_ & _)
    ret
  }
}
