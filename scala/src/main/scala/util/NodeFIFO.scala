package util

import spinal.core._
import spinal.lib._

import scala.language.postfixOps

class NodeFIFO extends Component {

  val width = 16
  val io = new Bundle {
    val input = slave(Flow(util.Linked(Bits(width bits), Bool())))
    val output = master(Stream(util.Linked(Bits(width bits), Bool())))
  }

  val inputFlow = Flow(Bits(width bits))
  val outputStream = Stream(Bits(width bits))
  val outputPipe = outputStream.m2sPipe()
  val uramFifo = new StreamFifo(Bits(64 bits), 4096, forFMax = true)
  val tagFifo = new StreamFifo(Bool(), 4096 * 4, forFMax = true)

  uramFifo.logic.ram.addAttribute("ram_style", "ultra")
  StreamWidthAdapter(inputFlow.toStream, uramFifo.io.push)
  StreamWidthAdapter(uramFifo.io.pop, outputStream)

  inputFlow.valid := io.input.valid
  inputFlow.payload := io.input.A

  io.output.arbitrationFrom(outputPipe)
  io.output.A := outputPipe.payload
  io.output.B := tagFifo.io.pop.payload

  tagFifo.io.push.valid := io.input.valid
  tagFifo.io.pop.ready := io.output.fire
  tagFifo.io.push.payload := io.input.B
}

object NodeFIFO extends App{
  SpinalVerilog(new NodeFIFO)
}