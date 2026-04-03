package norm

import adapter.{FlowGate, TagMap}
import spinal.core._
import spinal.lib._

import scala.language.postfixOps

class RMSLayerNorm(
                    width: Int,
                    dim: Int,
                    numOfCore: Int,
                    numOfInPort: Int,
                    numOfSqrPort: Int,
                    inputTagMap: List[(Int, Int)],
                    sqrSumTagMap: List[(Int, Int)],
                    mulLatency: Int,
                    mul_func: (Flow[Bits], Flow[Bits]) => Flow[Bits],
                    mul_func_block: (Stream[Bits], Stream[Bits]) => Stream[Bits],
                    rsqrt_func: Flow[Bits] => Flow[Bits]
                  ) extends Component {

  val fullDim = dim
  val partialDim = fullDim / numOfCore
  val sqrSumNeedScale = log2Up(fullDim) % 2 == 1

  val io = new Bundle {
    val input = Vec(slave(Flow(util.AxiFrame(Bits(width bits), userBit = 6))), numOfInPort)
    val squareSum = Vec(slave(Flow(util.AxiFrame(Bits(width bits), userBit = 6))), numOfSqrPort)
    val scale = slave(Stream(Bits(width bits)))
    val output = master(Flow(Fragment(util.AxiFrame(Bits(width bits), userBit = 6))))
  }

  val sqrt1over2 = Flow(Bits(width bits))
  sqrt1over2.valid.set()
  sqrt1over2.payload := 0x3800

  val (inTgM, inErr) = TagMap(io.input, inputTagMap)
  val input = inTgM.m2sPipe()

  //  input.valid.addAttribute("mark_debug")
  //  io.output.valid.addAttribute("mark_debug")
  //  io.output.last.addAttribute("mark_debug")

  val (sqrTgM, sqrErr) = TagMap(io.squareSum, sqrSumTagMap)
  val squareSumIn = sqrTgM.m2sPipe()

  val squareSumInFlow = squareSumIn.translateWith(squareSumIn.tdata)

  val squareSum = Flow(Bits(width bits))
  util.AxiStreamSpecRenamer(squareSum)

  if (sqrSumNeedScale) {
    val scaled = mul_func(squareSumInFlow, sqrt1over2)
    squareSum << scaled
  }
  else {
    squareSum << squareSumInFlow
  }

  val cntBound = U(partialDim - 1)
  val inCnt = UInt(log2Up(dim) bits).setAsReg().init(0)
  val inCntOvf = inCnt === cntBound
  val inCntZero = inCnt === 0
  when(input.valid) {
    inCnt := inCnt + 1
    when(inCntOvf) {
      inCnt.clearAll()
    }
  }

  //  inCntOvf.addAttribute("mark_debug", "true")

  val outCnt = UInt(log2Up(dim) bits).setAsReg().init(0)
  val outCntOvf = outCnt === cntBound
  when(io.output.valid) {
    outCnt := outCnt + 1
    when(outCntOvf) {
      outCnt.clearAll()
    }
  }

  val userStream = Stream(Bits(6 bits))
  val userStreamPipe = userStream.stage()
  userStream.valid := inCntZero & input.valid
  userStream.payload := input.tuser
  userStreamPipe.ready := outCntOvf & io.output.valid
  io.output.tuser := userStreamPipe.payload

  val fifo = StreamFifo(Bits(width bits), dim)

  val rsqrtOut = rsqrt_func(squareSum)
  val rsqrtOutLock = Flow(Bits(width bits))
  rsqrtOutLock.valid.setAsReg().init(False)
  rsqrtOutLock.payload.setAsReg()
  when(rsqrtOut.valid) {
    rsqrtOutLock.valid := True
    rsqrtOutLock.payload := rsqrtOut.payload
  }

  val enRsqrtCnt = Bool()
  val rsqrtCnt = UInt(16 bits).setAsReg().init(0)
  val rsqrtCntOvf = rsqrtCnt === cntBound
  when(enRsqrtCnt) {
    rsqrtCnt := rsqrtCnt + 1
    when(rsqrtCntOvf) {
      rsqrtCnt.clearAll()
      rsqrtOutLock.valid.clear()
    }
  }

  fifo.io.push.valid := input.valid
  fifo.io.push.payload := input.tdata

  val scaledOut = mul_func_block(fifo.io.pop, io.scale)
  val scaledOutFlow = Flow(Bits(width bits))
  val normOut = mul_func(scaledOutFlow, rsqrtOutLock)
  val scaledOutFire = scaledOut.fire
  scaledOutFlow.valid := scaledOutFire
  scaledOutFlow.payload := scaledOut.payload
  scaledOut.ready := rsqrtOutLock.valid
  enRsqrtCnt := scaledOutFire

  //  scaledOut.valid.addAttribute("mark_debug", "true")
  //  scaledOut.ready.addAttribute("mark_debug", "true")

  io.output.valid := normOut.valid
  io.output.tdata := normOut.payload
  io.output.last := outCntOvf

  //  val last = rsqrtOutLock.valid & rsqrtCntOvf
  //  val normOut = mul_func(fifo.io.pop.toFlow, rsqrtOutLock)
  //  fifo.io.pop.ready.removeAssignments()
  //  fifo.io.pop.ready := rsqrtOutLock.valid
  //
  //  val lastDly = Delay(last, mulLatency * 2 + 1, init = False)
  //  val scaleFlow = Flow(Bits(width bits))
  //  val scaleOut = mul_func(normOut.m2sPipe, scaleFlow.m2sPipe)
  //  scaleFlow.valid := io.scale.valid
  //  scaleFlow.payload := io.scale.payload
  //  io.scale.ready := normOut.valid
  //
  //  scaleFlow.valid.addAttribute("mark_debug", "true")
  //  normOut.valid.addAttribute("mark_debug", "true")
  //
  //  io.output.valid := scaleOut.valid
  //  io.output.tdata := scaleOut.payload
  //  io.output.last := lastDly

  //  val lastDly = Delay(last, mulLatency, init = False)
  //  io.scale.ready.set()
  //  io.output.valid := normOut.valid
  //  io.output.tdata := normOut.payload
  //  io.output.last := lastDly
}

object RMSLayerNorm extends App {
  SpinalVerilog(
    new RMSLayerNorm(
      16,
      4096,
      2,
      2,
      2,
      List((1, 2), (2, 3)),
      List((1, 2), (2, 3)),
      mulLatency = 6,
      util.fp16mul6.mul,
      util.fp16mul7s.mul,
      util.fp16rsqrt4.rsqrt
    )
  )
}
