package busdemux

import spinal.core._
import spinal.lib._

import scala.language.postfixOps

class SparseCase(tagMap: List[(Int, Int, Int)]) extends Component {

  val io = new Bundle {
    val tag = in Bits (6 bits)
    val bus = slave(Event)
    val main = master(Event)
    val misc = master(Event)
  }

//  io.bus.valid.addAttribute("mark_debug", "true")
//  io.main.addAttribute("mark_debug", "true")
//  io.misc.addAttribute("mark_debug", "true")

  val tagHit = tagMap.map(_._1 === io.tag).asBits()
  val miscLenVec = tagMap.map(x => U(x._2, 8 bits))
  val mainLenVec = tagMap.map(x => U(x._3, 8 bits))
  val miscLen = MuxOH(tagHit, miscLenVec)
  val mainLen = MuxOH(tagHit, mainLenVec)

  val deMux = new StreamDemux(NoData, 2)
  deMux.io.input << io.bus
  deMux.io.outputs(1) >> io.main
  deMux.io.outputs(0) >> io.misc

  val cntLv1 = UInt(8 bits).setAsReg().init(0)
  val cntLv2 = UInt(8 bits).setAsReg().init(0)
  val cntLv1Ovf = cntLv1 === miscLen - 1
  val cntLv2Ovf = cntLv2 === mainLen - 1

  val sel = UInt(1 bits).setAsReg().init(0)

  when(io.misc.fire) {
    cntLv1 := cntLv1 + 1
    when(cntLv1Ovf) {
      cntLv1 := 0
      sel := 1
    }
  }

  when(io.main.fire) {
    cntLv2 := cntLv2 + 1
    when(cntLv2Ovf) {
      cntLv2 := 0
      sel := 0
    }
  }
  deMux.io.select := sel
}
