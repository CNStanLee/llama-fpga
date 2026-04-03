package rope

import spinal.core._
import spinal.lib._

import scala.language.postfixOps

class CosSinIndexGen(
                      dim: Int,
                      points: Int,
                      mul_func: (Flow[Bits], Flow[Bits]) => Flow[Bits],
                      fromInt_func: Flow[Bits] => Flow[Bits],
                      toInt_func: Flow[Bits] => Flow[Bits]
                    ) extends Component {

  val pointWidth = log2Up(points)
  val byteAlignWidth = (pointWidth + 7) / 8 * 8

  val io = new Bundle {
    val pos = slave(Flow(Bits(16 bits)))
    val index = master(Stream(Bits(byteAlignWidth bits)))
  }

  noIoPrefix()
  util.AxiStreamSpecRenamer(io.index)

  val rom = new InvFreqRom(dim, points)

  val flag = Bool().setAsReg().init(False)
  flag.setWhen(io.pos.valid)
  val posCnt = UInt(log2Up(dim) bits).setAsReg().init(0)
  when(flag){
    posCnt := posCnt + 1
    when(posCnt === dim - 1) {
      posCnt.clearAll()
      flag.clear()
    }
  }

  val posIn = Flow(Bits(16 bits))
  val posOut = fromInt_func(posIn)
  posIn.valid := flag
  posIn.payload := io.pos.payload

  val invFreqFlow = Flow(Bits(32 bits))
  val mulFlow = mul_func(posOut, invFreqFlow)
  val intFlow = toInt_func(mulFlow)

  rom.io.invFreq.ready := posOut.valid
  invFreqFlow.valid := posOut.valid
  invFreqFlow.payload := rom.io.invFreq.payload

  val pipe = new StreamFifo(UInt(log2Up(points) bits), 128)
  pipe.io.push.valid := intFlow.valid
  pipe.io.push.payload := intFlow.payload.take(log2Up(points)).asUInt

  val pipeOut = pipe.io.pop.m2sPipe()

  io.index.arbitrationFrom(pipeOut)
  io.index.payload := pipeOut.payload.resize(byteAlignWidth).asBits
}

object CosSinIndexGen extends App {
  val cfg = SpinalConfig(inlineRom = true)
  cfg.generateVerilog(new CosSinIndexGen(128, 1 << 14, util.fp32mul8.mul, util.fp32int16d4.from, util.fp32toint32d6.to))
}