package convert

import spinal.core._
import spinal.lib._

import scala.language.postfixOps

class Int2FP16(
                dataWidth: Int,
                busWidth: Int,
                bankLen: Int,
                convertLatency: Int,
                convertor: Flow[Bits] => Flow[Bits]
              ) extends Component {

  val byteAlignWidth = (dataWidth + 7) / 8 * 8
  val exByteAlignWidth = (dataWidth + 1 + 7) / 8 * 8

  val io = new Bundle {
    val i = slave(Flow(Bits(busWidth bits)))
    val o = master(Flow(Bits(bankLen * 16 bits)))
    val z = slave(Stream(Bits(byteAlignWidth bits)))
  }

  noIoPrefix()
  util.AxiStreamSpecRenamer(io.i)
  util.AxiStreamSpecRenamer(io.o)
  util.AxiStreamSpecRenamer(io.z)

  io.z.ready := io.i.valid
  val zero = io.z.payload.resize(dataWidth).asUInt.expand.asSInt

//  val d = Vec(Flow(Bits(exByteAlignWidth bits)), busWidth / dataWidth)
//  //  d.foreach(_.valid := io.i.valid)
//  d.foreach(_.valid.set())
//  (d, io.i.payload.subdivideIn(busWidth / dataWidth slices)).zipped.foreach(
//    (dst, src) => dst.payload := (src.asUInt.expand.asSInt - zero).asBits.resize(exByteAlignWidth)
//  )

  val subDiv = io.i.payload.subdivideIn(busWidth / dataWidth slices)
  val dSub = subDiv.map(ds=> (ds.asUInt.expand.asSInt - zero).asBits.resize(exByteAlignWidth))
  val dDly = RegNext(Vec(dSub))
  val d = Vec(Flow(Bits(exByteAlignWidth bits)), busWidth / dataWidth)
  d.foreach(_.valid.set())
  (d, dDly).zipped.foreach(_.payload := _)

  val w = d.map(convertor)
  val vld = Delay(io.i.valid, convertLatency, init = False)
  val wPy = w.map(_.payload).asBits

  val group = bankLen / (busWidth / dataWidth)

  val through = (group == 1) generate new Area {
    io.o.valid := vld
    io.o.payload := wPy
  }

  val pack = (group > 1) generate new Area {
    val vldIn = io.i.valid
    val vldDly = Delay(vldIn, convertLatency, init = False)
    vldDly.addAttribute("max_fanout", 100)

    val wPyDly = History(wPy, group, when = vldDly).reverse.asBits()
    val cnt = UInt(log2Up(group) bits).setAsReg().init(0)
    val cntOvf = cnt === group - 1
    when(vldDly) {
      cnt := cnt + 1
      when(cntOvf) {
        cnt := 0
      }
    }

    io.o.valid := vldDly & cntOvf
    io.o.payload := wPyDly
  }
}

object Int2FP16 extends App {
  SpinalVerilog(new Int2FP16(4, 512, 128, 5, util.fp16int5d4.from))
}