package util

import spinal.core._
import spinal.lib._

import scala.language.postfixOps

class FlowFragmentAlign[T<:Data](dataType:HardType[T]) extends Component {
  val io = new Bundle {
    val input = slave(Flow(Fragment(dataType())))
    val output = master(Flow(Fragment(dataType())))
  }

  io.output << io.input.m2sPipe

//  val dly = Stream(dataType())
//  val dlyPipe = dly.m2sPipe()
//
//  dly.valid := io.input.valid
//  dly.payload := io.input.payload
//
//  val alignDly = RegNext(io.input.valid & io.input.last, False)
//  val notAlignLast = ~io.input.valid & io.input.last
//  val notAlignReady = io.input.valid || io.input.last
//
//  dlyPipe.ready := notAlignReady || alignDly
//
//  io.output.valid := RegNext(dlyPipe.fire,init=False)
//  io.output.fragment := RegNext(dlyPipe.payload)
//  io.output.last := RegNext(notAlignLast || alignDly,init=False)
}

object FlowFragmentAlign extends App {
  SpinalVerilog(new FlowFragmentAlign(UInt(8 bits)))
}