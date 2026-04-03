package util

import spinal.core._
import spinal.lib._

import scala.language.postfixOps

class ConsecutiveDetector(width: Int) extends Component {

  val io = new Bundle {
    val input = slave(Flow(Fragment(UInt(width bits))))
    val output = master(Flow(Fragment(Linked(UInt(width bits), UInt(width bits)))))
  }

  val indexPipe = RegNextWhen(io.input.fragment, io.input.valid)
  val lastPipe = RegNext(io.input.last, init = False)
  val vldPipe = RegNext(io.input.valid, init = False)

  val consecutive = io.input.fragment === indexPipe + 1
  val outVld = Bool()

  val cnt = UInt(width bits).setAsReg().init(0)
  when(vldPipe) {
    cnt := cnt + 1
    when(outVld) {
      cnt.clearAll()
    }
  }

  val maxPack = 64
  val maxPackOvf = cnt === maxPack - 1
  outVld := vldPipe & (~consecutive || lastPipe || maxPackOvf)

//  maxPackOvf.addAttribute("mark_debug", "true")

  io.output.valid := outVld
  io.output.A := indexPipe - cnt
  io.output.B := cnt + 1
  io.output.last := lastPipe

//  io.input.addAttribute("mark_debug", "true")
//  io.output.addAttribute("mark_debug", "true")
}

object ConsecutiveDetector {
  def apply(index: Flow[Fragment[UInt]]) = {
    val detector = new ConsecutiveDetector(index.fragment.getWidth)
    detector.io.input << index
    detector.io.output
  }
}