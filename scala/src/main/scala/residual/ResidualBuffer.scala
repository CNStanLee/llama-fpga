package residual

import spinal.core._
import spinal.lib._
import util.{Fp16ScaleDown, LargeBankFifo}

import scala.language.postfixOps

class ResidualBuffer(
                      dim: Int,
                      numOfCore: Int,
                      width: Int,
                      bankLen: Int,
                      split: Int = 4
                    ) extends Component {

  val partialDim = dim / numOfCore
  val parallelWidth = width * bankLen
  val depth = partialDim / bankLen

  val io = new Bundle {
    val serialIn = slave(Flow(Bits(width bits)))
    val parallelIn = slave(Flow(Bits(parallelWidth bits)))
    val serialOut = master(Stream(Bits(width bits)))
    val parallelOut = master(Stream(Bits(parallelWidth bits)))
  }

  noIoPrefix()
  util.AxiStreamSpecRenamer(io.serialIn)
  util.AxiStreamSpecRenamer(io.parallelIn)
  util.AxiStreamSpecRenamer(io.serialOut)
  util.AxiStreamSpecRenamer(io.parallelOut)

  val serialIn = Flow(Bits(width bits))
  val parallelIn = Flow(Bits(parallelWidth bits))
  serialIn.valid.setAsReg().init(False)
  parallelIn.valid.setAsReg().init(False)
  serialIn.payload.setAsReg()
  parallelIn.payload.setAsReg()
  serialIn.valid := io.serialIn.valid
  parallelIn.valid := io.parallelIn.valid
  serialIn.payload := io.serialIn.payload
  parallelIn.payload := io.parallelIn.payload

  serialIn.valid.addAttribute("max_fanout", 100)
  parallelIn.valid.addAttribute("max_fanout", 100)

  val inMuxSel = Bool().setAsReg().init(False) // parallel first
  val outMuxSel = Bool().setAsReg().init(False) // serial first

  val cnt = UInt(8 bits).setAsReg().init(0)
  val cntAbout2Ovf = cnt === bankLen - 2
  //  val cntOvf = cnt === bankLen - 1
  val cntOvfReg = Bool().setAsReg().init(False)
  when(serialIn.valid) {
    cnt := cnt + 1
    when(cntAbout2Ovf) {
      cntOvfReg.set()
    }
    when(cntOvfReg) {
      cnt.clearAll()
      cntOvfReg.clear()
    }
  }
  val adaptedSerial = Flow(Bits(parallelWidth bits))
  adaptedSerial.valid := serialIn.valid & cntOvfReg
  adaptedSerial.payload := History(
    //    Fp16ScaleDown(io.serialIn.payload, 4),
    serialIn.payload,
    bankLen,
    when = serialIn.valid
  ).reverse.asBits

  val resBuf = new LargeBankFifo(Bits(parallelWidth bits), depth, forFMax = true, split = split)
  resBuf.io.push.valid := Mux(inMuxSel, adaptedSerial.valid, parallelIn.valid)
  resBuf.io.push.payload := Mux(inMuxSel, adaptedSerial.payload, parallelIn.payload)

  val deMux = new StreamDemux(Bits(parallelWidth bits), 2)
  deMux.io.input << resBuf.io.pop
  deMux.io.select := outMuxSel.asUInt

  val serialOut = Stream(Bits(width bits))
  val parallelOut = Stream(Bits(parallelWidth bits))

  StreamWidthAdapter(deMux.io.outputs(0), serialOut)
  parallelOut << deMux.io.outputs(1)
  io.serialOut << serialOut
  io.parallelOut << parallelOut

  val bufInCnt = UInt(log2Up(depth) bits).setAsReg().init(0)
  val bufInCntOvf = bufInCnt === depth - 1
  when(resBuf.io.push.fire) {
    bufInCnt := bufInCnt + 1
    when(bufInCntOvf) {
      bufInCnt.clearAll()
      inMuxSel := ~inMuxSel
    }
  }

  val bufOutCnt = UInt(log2Up(depth) bits).setAsReg().init(0)
  val bufOutCntOvf = bufOutCnt === depth - 1
  when(resBuf.io.pop.fire) {
    bufOutCnt := bufOutCnt + 1
    when(bufOutCntOvf) {
      bufOutCnt.clearAll()
      outMuxSel := ~outMuxSel
    }
  }
}

object ResidualBuffer extends App {
  SpinalVerilog(new ResidualBuffer(4096, 2, 16, 128))
}