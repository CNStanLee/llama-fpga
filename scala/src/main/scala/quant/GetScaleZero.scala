package quant

import spinal.core._
import spinal.lib._

import scala.language.postfixOps

class GetScaleZero(
                    quantWidth: Int,
                    maxIntFP16Init : Int,
                    sub_func: (Flow[Bits], Flow[Bits]) => Flow[Bits],
                    div_func: (Flow[Bits], Flow[Bits]) => Flow[Bits],
                    convert_func: Flow[Bits] => Flow[Bits]
                  ) extends Component {

  val io = new Bundle {
    val max = slave(Flow(Bits(16 bits)))
    val min = slave(Flow(Bits(16 bits)))
    val scale = master(Flow(Bits(16 bits)))
    val zero = master(Flow(Bits(quantWidth bits)))
  }

  noIoPrefix()
  util.AxiStreamSpecRenamer(io.max)
  util.AxiStreamSpecRenamer(io.min)
  util.AxiStreamSpecRenamer(io.scale)
  util.AxiStreamSpecRenamer(io.zero)

  val maxIntFlow = Flow(Bits(16 bits))
  maxIntFlow.valid.set()
  maxIntFlow.payload := maxIntFP16Init

  val fifo = new StreamFifo(Bits(16 bits), 32)
  fifo.io.push.valid := io.min.valid
  fifo.io.push.payload := io.min.payload

  val minFlowDly = Flow(Bits(16 bits))

  val diff = sub_func(io.max, io.min)
  val scale = Flow(Bits(16 bits))
  scale << div_func(diff, maxIntFlow)

  val divRes = div_func(minFlowDly, scale)
  val divRev = Flow(Bits(16 bits))
  divRev.valid := divRes.valid
  divRev.payload := ~divRes.payload.takeHigh(1) ## divRes.payload.dropHigh(1)

  val convRes = convert_func(divRev)

  minFlowDly.valid := fifo.io.pop.valid
  minFlowDly.payload := fifo.io.pop.payload
  fifo.io.pop.ready := scale.valid

  val maxInt = U((1 << quantWidth) - 1, quantWidth bits)
  val zero = Bits(quantWidth bits)
  zero := convRes.payload.resized
  when(convRes.payload.asUInt > maxInt) {
    zero := maxInt.asBits
  }

  io.zero.valid := convRes.valid
  io.zero.payload := zero
  io.scale << scale

//  scale.addAttribute("mark_debug", "true")
//  divRev.addAttribute("mark_debug", "true")
//  io.zero.addAttribute("mark_debug", "true")
//
//  val flag = Bool().setAsReg().init(False)
//  flag.toggleWhen(io.zero.valid)
//  flag.addAttribute("mark_debug", "true")
}

object GetScaleZero extends App {
  SpinalVerilog(new GetScaleZero(8, 0x5bf8, util.fp16sub8.sub, util.fp16div12.div, util.fp16toint9d4.to))
}