package busdemux

import spinal.core._
import spinal.lib._

import scala.language.postfixOps

class DenseCase(busWidth: Int) extends Component {

  val scaleWidth = 16
  val zeroPointWidth = 4
  val scalePerBeat = busWidth / scaleWidth

  val io = new Bundle {
    val bus = slave(Event)
    val main = master(Event)
    val misc = master(Event)
  }

  val deMux = new StreamDemux(NoData, 2)
  deMux.io.input << io.bus
  deMux.io.outputs(1) >> io.main
  deMux.io.outputs(0) >> io.misc

  val cntLv1 = UInt(8 bits).setAsReg().init(0)
  val cntLv2 = UInt(8 bits).setAsReg().init(0)
  val cntLv1Ovf = cntLv1 === scalePerBeat - 1
  val cntLv2Ovf = cntLv2 === scaleWidth / zeroPointWidth - 1

  val sel = UInt(2 bits).setAsReg().init(0)
  val selZero = 0
  val selScale = 1
  val selMain = 2

  when(io.misc.fire & sel === selZero) {
    sel := selScale
  }
  when(io.misc.fire & sel === selScale) {
    sel := selMain
  }
  when(io.main.fire) {
    cntLv1 := cntLv1 + 1
    when(cntLv1Ovf) {
      cntLv1 := 0
      cntLv2 := cntLv2 + 1
      sel := selScale
      when(cntLv2Ovf) {
        cntLv2 := 0
        sel := selZero
      }
    }
  }

  when(sel === selZero || sel === selScale) {
    deMux.io.select := 0
  }.otherwise {
    deMux.io.select := 1
  }
}
