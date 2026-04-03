package rope

import adapter.{FlowGate, TagMap}
import spinal.core._
import spinal.lib._
import util.AxiFrame

import scala.language.postfixOps

class SerialRoPE32(
                  dim: Int,
                  points: Int,
                  mix_mul_func: (Flow[Bits], Flow[Bits]) => Flow[Bits],
                  highPcs_mul_func: (Flow[Bits], Flow[Bits]) => Flow[Bits],
                  add_conv_func: (Flow[Bits], Flow[Bits]) => Flow[Bits],
                  toInt_func: Flow[Bits] => Flow[Bits],
                  fromInt_func: Flow[Bits] => Flow[Bits]
                ) extends Component {

  val io = new Bundle {
    val pos = in Bits(16 bits)
    val input = slave(Flow(util.AxiFrame(Bits(16 bits), userBit = 6)))
    val output = master(Flow(util.AxiFrame(Bits(16 bits), userBit = 6)))
  }

  noIoPrefix()
  util.AxiStreamSpecRenamer(io.input)
  util.AxiStreamSpecRenamer(io.output)

  val ropeIn = io.input.m2sPipe()
  util.AxiStreamSpecRenamer(ropeIn)

  val inCnt = UInt(log2Up(dim) bits).setAsReg().init(0)
  val inCntOvf = inCnt === dim - 1
  val inCntZero = inCnt === 0
  val usrStream = Stream(Bits(6 bits))
  val usrStreamPipe = usrStream.m2sPipe().m2sPipe()
  usrStream.valid := inCntZero & ropeIn.valid
  usrStream.payload := ropeIn.tuser
  when(ropeIn.valid) {
    inCnt := inCnt + 1
    when(inCntOvf) {
      inCnt.clearAll()
    }
  }

  val rotator = new RoPERotate(Bits(16 bits), dim)
  val indexGen = new CosSinIndexGen(dim, points, highPcs_mul_func, fromInt_func, toInt_func)
  val cosSinGen = new CosSinGen32(points)

  val indexMon = Flow(Bits(16 bits))
  indexMon.valid := indexGen.io.index.fire
  indexMon.payload := indexGen.io.index.payload
  util.AxiStreamSpecRenamer(indexMon)

  rotator.cfg.dim := dim - 1
  rotator.io.input << ropeIn.translateWith(ropeIn.tdata)
  indexGen.io.pos.valid := ropeIn.valid & inCntZero
  indexGen.io.pos.payload := io.pos
  cosSinGen.io.index << indexGen.io.index

  val joinEvent = Flow(NoData())
  joinEvent.valid := rotator.io.output.valid
  cosSinGen.io.sinCos.ready := rotator.io.output.valid

  val qkFlow = Flow(Bits(16 bits))
  val cosFlow = Flow(Bits(32 bits))
  val qkRotateFlow = Flow(Bits(16 bits))
  val sinFlow = Flow(Bits(32 bits))

  util.AxiStreamSpecRenamer(cosFlow)
  util.AxiStreamSpecRenamer(sinFlow)

  qkFlow.valid := joinEvent.valid
  qkFlow.payload := rotator.io.output.A
  cosFlow.valid := joinEvent.valid
  cosFlow.payload := cosSinGen.io.sinCos.payload.take(32)

  qkRotateFlow.valid := joinEvent.valid
  qkRotateFlow.payload := rotator.io.output.B
  sinFlow.valid := joinEvent.valid
  sinFlow.payload := cosSinGen.io.sinCos.payload.drop(32)

  val qkCos = mix_mul_func(qkFlow, cosFlow)
  val qkSin = mix_mul_func(qkRotateFlow, sinFlow)
  val ret = add_conv_func(qkCos, qkSin)

  io.output.valid := ret.valid
  io.output.tdata := ret.payload
  io.output.tuser := usrStreamPipe.payload

  val outCnt = UInt(log2Up(dim) bits).setAsReg().init(0)
  val outCntOvf = outCnt === dim - 1
  usrStreamPipe.ready := ret.valid & outCntOvf
  when(ret.valid) {
    outCnt := outCnt + 1
    when(outCntOvf) {
      outCnt.clearAll()
    }
  }
}

object SerialRoPE32 extends App {
  val cfg = SpinalConfig(inlineRom = true)
  cfg.generateVerilog(
    new SerialRoPE32(
      128, 1 << 14,
      util.fp16mulFp32.apply,
      util.fp32mul8.mul,
      util.fp32add2Fp16.apply,
      util.fp32toint32d6.to,
      util.fp32int16d4.from
    ))
}
