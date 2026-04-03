package norm

import spinal.core._
import spinal.lib._

import scala.language.postfixOps

class RMSNormFp32(
                   dim: Int,
                   id: Int,
                   numOfCore: Int,
                   toFp32_func: Flow[Bits] => Flow[Bits],
                   toFp32_func_block: Stream[Bits] => Stream[Bits],
                   toFp16_func: Flow[Bits] => Flow[Bits],
                   mul_func: (Flow[Bits], Flow[Bits]) => Flow[Bits],
                   mul_func_block: (Stream[Bits], Stream[Bits]) => Stream[Bits],
                   acc_func: Flow[Fragment[Bits]] => Flow[Fragment[Bits]],
                   rsqrt_func: Flow[Bits] => Flow[Bits]
                 ) extends Component {

  val io = new Bundle {
    val toBeNorm = slave(Flow(Bits(16 bits)))
    val scale = slave(Stream(Bits(16 bits)))
    val normOut = master(Flow(Bits(16 bits)))
  }

  val isAttnLn = in Bool()
  val isLmHeadLn = in Bool()

  val toBeNormFp32 = toFp32_func(io.toBeNorm)
  val squared = mul_func(toBeNormFp32, toBeNormFp32)

  val sqrCnt = UInt(log2Up(dim) bits).setAsReg().init(0)
  val sqrCntOvf = sqrCnt === dim - 1
  when(squared.valid) {
    sqrCnt := sqrCnt + 1
    when(sqrCntOvf) {
      sqrCnt.clearAll()
    }
  }

  val accIn = Flow(Fragment(Bits(32 bits)))
  accIn.valid := squared.valid
  accIn.fragment := squared.payload
  accIn.last := sqrCntOvf

  val accOut = Flow(Fragment(Bits(32 bits)))
  accOut << acc_func(accIn)

  val rsqrtIn = Flow(Bits(32 bits))
  rsqrtIn.valid := accOut.valid & accOut.last
  rsqrtIn.payload := util.Fp32ScaleDown(accOut.fragment, log2Up(dim))
  val rsqrtOut = rsqrt_func(rsqrtIn)

  val rsqrtOutLock = Flow(Bits(32 bits))
  rsqrtOutLock.valid.setAsReg().init(False)
  rsqrtOutLock.payload.setAsReg()
  when(rsqrtOut.valid) {
    rsqrtOutLock.valid := True
    rsqrtOutLock.payload := rsqrtOut.payload
  }

  val partialDim = dim / numOfCore
  val coreCnt = UInt(scala.math.max(log2Up(numOfCore), 1) bits).setAsReg().init(0)
  val dimPerCoreCnt = UInt(log2Up(partialDim) bits).setAsReg().init(0)
  when(io.toBeNorm.valid) {
    dimPerCoreCnt := dimPerCoreCnt + 1
    when(dimPerCoreCnt === partialDim - 1) {
      dimPerCoreCnt.clearAll()
      coreCnt := coreCnt + 1
      when(coreCnt === numOfCore - 1) {
        coreCnt.clearAll()
      }
    }
  }

  //  val fifo = new StreamFifo(Bits(16 bits), dim, forFMax = true)
  //  fifo.io.push.valid := (if (numOfCore != 4)
  //    io.toBeNorm.valid & Mux(isAttnLn, True, Mux(isLmHeadLn, coreCnt === 0, coreCnt === id))
  //  else
  //    io.toBeNorm.valid & Mux(isAttnLn, True, coreCnt === id))
  //  fifo.io.push.payload := io.toBeNorm.payload

  val fifo = if (numOfCore != 4) new StreamFifo(Bits(16 bits), dim, forFMax = true) else null
  val buf = if (numOfCore == 4) new NormBuffer(id, 16, dim, numOfCore) else null
  val bufOut = if (numOfCore == 4) buf.io.output else fifo.io.pop
  if (numOfCore != 4) {
    fifo.io.push.valid := io.toBeNorm.valid & Mux(isAttnLn, True, Mux(isLmHeadLn, coreCnt === 0, coreCnt === id))
    fifo.io.push.payload := io.toBeNorm.payload
  }
  else {
    buf.io.input << io.toBeNorm
    buf.io.isAttnLn := isAttnLn
    buf.io.isMlpLn := ~isAttnLn & ~isLmHeadLn
  }
  //  io.normOut.addAttribute("mark_debug", "true")

  val inputFp32 = Stream(Bits(32 bits))
  val scaleFp32 = Stream(Bits(32 bits))
  val scaledOut = mul_func_block(inputFp32, scaleFp32)
  inputFp32 << toFp32_func_block(bufOut)
  scaleFp32 << toFp32_func_block(io.scale)

  val enRsqrtCnt = Bool()
  val rsqrtCnt = UInt(log2Up(dim) bits).setAsReg().init(0)
  val rsqrtCntOvf = rsqrtCnt === Mux(isAttnLn, U(dim - 1), U(partialDim - 1))
  when(enRsqrtCnt) {
    rsqrtCnt := rsqrtCnt + 1
    when(rsqrtCntOvf) {
      rsqrtCnt.clearAll()
      rsqrtOutLock.valid.clear()
    }
  }

  val scaledOutFlow = Flow(Bits(32 bits))
  val normOut = mul_func(scaledOutFlow, rsqrtOutLock)
  val scaledOutFire = scaledOut.fire
  scaledOutFlow.valid := scaledOutFire
  scaledOutFlow.payload := scaledOut.payload
  scaledOut.ready := rsqrtOutLock.valid
  enRsqrtCnt := scaledOutFire

  val normOutFp16 = toFp16_func(normOut)
  io.normOut.valid := normOutFp16.valid
  io.normOut.payload := normOutFp16.payload
}
