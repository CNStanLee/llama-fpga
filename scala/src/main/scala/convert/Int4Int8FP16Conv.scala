package convert

import spinal.core._
import spinal.lib._

import scala.language.postfixOps

class Int4Int8FP16Conv(
                        bankLen: Int,
                        convertLatency: Int,
                        convertor: Flow[Bits] => Flow[Bits]
                      ) extends Component {

  val io = new Bundle {
    val selInt8 = in Bool()
    val inputData = slave(Flow(Bits (4 * bankLen bits)))
    val zeroInt4 = slave(Stream(Bits(8 bits)))
    val zeroInt8 = slave(Stream(Bits(8 bits)))
    val output = master(Flow(Bits(16 * bankLen bits)))
  }

  noIoPrefix()
  util.AxiStreamSpecRenamer(io.inputData)
  util.AxiStreamSpecRenamer(io.zeroInt4)
  util.AxiStreamSpecRenamer(io.zeroInt8)
  util.AxiStreamSpecRenamer(io.output)

  //  io.zeroInt4.valid.addAttribute("mark_debug", "true")
  //  io.zeroInt4.ready.addAttribute("mark_debug", "true")

  val int8InVld = io.inputData.valid & io.selInt8
  val int4InVld = io.inputData.valid & ~io.selInt8

  //  val (zeroInt8Rep, _) = io.zeroInt8.repeat(2)
  val zeroMux = new StreamMux(Bits(8 bits), 2)
  zeroMux.io.inputs(0) << io.zeroInt4
  zeroMux.io.inputs(1) << io.zeroInt8
  zeroMux.io.select := io.selInt8.asUInt

  val int8VldFlip = Bool().setAsReg().init(False)
  int8VldFlip.toggleWhen(int8InVld)
  val dataDly = RegNextWhen(io.inputData.payload, int8InVld)

  val int8Vld = int8VldFlip & int8InVld
  val int4Vld = int4InVld
  val int8Data = (io.inputData.payload ## dataDly).subdivideIn(bankLen slices)
  val int4Data = io.inputData.payload.subdivideIn(bankLen slices).map(_.resize(8))

  val vldConv = Mux(io.selInt8, int8Vld, int4Vld)
  val dataConv = Mux(io.selInt8, int8Data, Vec(int4Data))
  val zeroConv = zeroMux.io.output
  zeroConv.ready := vldConv

  //  val zero = zeroConv.payload.resize(8).asUInt.expand.asSInt
  //  val subDiv = dataConv

  val zero = SInt(9 bits).setAsReg()
  zero := zeroConv.payload.resize(8).asUInt.expand.asSInt
  zero.addAttribute("max_fanout", 100)

  val subDiv = RegNext(dataConv)
  val dSub = subDiv.map(ds => (ds.asUInt.expand.asSInt - zero).asBits)
  val dDly = (Vec(dSub)).map(_.resize(16))
  val d = Vec(Flow(Bits(16 bits)), bankLen)
  d.foreach(_.valid.set())
  (d, dDly).zipped.foreach(_.payload := _)

  val w = d.map(convertor)
  val vld = Delay(vldConv, convertLatency, init = False)
  val wPy = w.map(_.payload).asBits

  io.output.valid := vld
  io.output.payload := util.Fp16ScaleDown.vec(wPy, 2)
}

object Int4Int8FP16Conv extends App {
  SpinalVerilog(new Int4Int8FP16Conv(16, 4 + 1, util.fp16int9d4.from))
}
