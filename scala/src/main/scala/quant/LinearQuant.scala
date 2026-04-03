package quant

import spinal.core._
import spinal.lib._
import util.StreamRepeat

import scala.language.postfixOps

class LinearQuant(
                   quantWidth: Int,
                   div_func: (Flow[Bits], Flow[Bits]) => Flow[Bits],
                   convert_func: Flow[Bits] => Flow[Bits]
                 ) extends Component {

  val extQuantWidth = quantWidth + 2

  val io = new Bundle {
    val x = slave(Flow(Bits(16 bits)))
    val scale = slave(Flow(Bits(16 bits)))
    val zero = slave(Stream(Bits(quantWidth bits)))
    val q = master(Flow(Bits(quantWidth bits)))
  }

  noIoPrefix()
  util.AxiStreamSpecRenamer(io.x)
  util.AxiStreamSpecRenamer(io.scale)
  util.AxiStreamSpecRenamer(io.zero)
  util.AxiStreamSpecRenamer(io.q)

  val divRes = div_func(io.x.m2sPipe, io.scale.m2sPipe)
  val convRes = convert_func(divRes.m2sPipe)

  io.zero.ready := convRes.valid

  val add = SInt(extQuantWidth bits)
  add := convRes.payload.asSInt.resize(extQuantWidth) +
    io.zero.payload.resize(extQuantWidth).asSInt

  val q = Bits(quantWidth bits)
  q := add.resize(quantWidth).asBits
  when(add < 0) {
    q := 0
  }
  when(add >= (1 << quantWidth)) {
    q := (1 << quantWidth) - 1
  }

  io.q.valid := convRes.valid
  io.q.payload := q
}

object LinearQuant extends App {
  SpinalVerilog(new LinearQuant(8, util.fp16div12.div, util.fp16toint9d4.to))
}