package top

import adapter.{FlowGate, FlowMux, TagMap}
import norm.RMSNormFp32
import spinal.core._
import spinal.lib._

import scala.language.postfixOps

class NormSubModNew(
                     id: Int,
                     dim: Int,
                     numOfCore: Int,
                     lnInTagMap: List[(Int, Int)],
                     attnLnTag: Int,
                     logitsLnTag: Int,
                     fp16toFp32_func: Flow[Bits] => Flow[Bits],
                     fp16toFp32_func_block: Stream[Bits] => Stream[Bits],
                     fp32toFp16_func: Flow[Bits] => Flow[Bits],
                     fp32mul_func: (Flow[Bits], Flow[Bits]) => Flow[Bits],
                     fp32mul_func_block: (Stream[Bits], Stream[Bits]) => Stream[Bits],
                     fp32acc_func: Flow[Fragment[Bits]] => Flow[Fragment[Bits]],
                     fp32rsqrt_func: Flow[Bits] => Flow[Bits]
                   ) extends Component {

  val io = new Bundle {
    val allGatherOut = slave(Flow(util.AxiFrame(Bits(16 bits), userBit = 6)))
    val allReduceOut = slave(Flow(util.AxiFrame(Bits(16 bits), userBit = 6)))
    val lnScale = slave(Stream(Bits(16 bits)))
    val lnOut = master(Flow(util.AxiFrame(Bits(16 bits), userBit = 6)))
  }

  val status = new Bundle {
    val toLogitsGen = in Bool()
  }

  val toLogitsGenDly = if(numOfCore == 4) Delay(status.toLogitsGen, dim, init = False) else null
  val allReduceOut = FlowGate.keepTag(io.allReduceOut, List(lnInTagMap.last._1))
  val allGatherOut = FlowGate.keepTag(io.allGatherOut, lnInTagMap.dropRight(1).map(_._1))
  allReduceOut.tuser.removeDataAssignments()
  allReduceOut.tuser := lnInTagMap.last._2
  allGatherOut.tuser.removeDataAssignments()
  allGatherOut.tuser := lnInTagMap.head._2

  val (inputTagMap, _) = FlowMux(Vec(allReduceOut, allGatherOut))

  //  val (inputTagMap, inErr) = TagMap(Vec(io.allReduceOut, io.allGatherOut), lnInTagMap)

  val inputVld = RegNext(inputTagMap.valid, init = False)
  val inputData = RegNext(inputTagMap.tdata)
  val tag = if(numOfCore == 4)
    Mux(toLogitsGenDly & inputTagMap.tuser === attnLnTag, B(logitsLnTag), inputTagMap.tuser)
  else
    Mux(status.toLogitsGen & inputTagMap.tuser === attnLnTag, B(logitsLnTag), inputTagMap.tuser)

  val inCnt = UInt(log2Up(dim) bits).setAsReg().init(0)
  val inCntOvf = inCnt === dim - 1
  when(inputTagMap.valid) {
    inCnt := inCnt + 1
    when(inCntOvf) {
      inCnt.clearAll()
    }
  }

  val userStream = Stream(Bits(6 bits))
  val userStreamPipe = userStream.stage()
  userStream.valid := inCnt === 0 & inputTagMap.valid
  userStream.payload := tag

  val rmsNorm = new RMSNormFp32(
    dim, id, numOfCore,
    fp16toFp32_func, fp16toFp32_func_block, fp32toFp16_func,
    fp32mul_func, fp32mul_func_block, fp32acc_func, fp32rsqrt_func
  )

  val isAttnLn = userStreamPipe.payload === attnLnTag
  val isLmHeadLn = userStreamPipe.payload === logitsLnTag
  rmsNorm.isAttnLn := isAttnLn
  rmsNorm.isLmHeadLn := isLmHeadLn
  rmsNorm.io.toBeNorm.valid := inputVld
  rmsNorm.io.toBeNorm.payload := inputData
  rmsNorm.io.scale << io.lnScale

  val outCnt = UInt(log2Up(dim) bits).setAsReg().init(0)
  val outCntOvf = outCnt === Mux(isAttnLn, U(dim - 1), U(dim / numOfCore - 1))
  when(rmsNorm.io.normOut.valid) {
    outCnt := outCnt + 1
    when(outCntOvf) {
      outCnt.clearAll()
    }
  }

  val output = Flow(util.AxiFrame(Bits(16 bits), userBit = 6))
  output.valid := rmsNorm.io.normOut.valid
  output.tdata := rmsNorm.io.normOut.payload
  output.tuser := userStreamPipe.payload
  userStreamPipe.ready := output.valid & outCntOvf
  io.lnOut << output.m2sPipe

//  val mlpNormOutProbe = master(Flow(Bits(16 bits)))
//  mlpNormOutProbe.valid := rmsNorm.io.normOut.valid & outCnt === 2047 & (~isAttnLn & ~isLmHeadLn)
//  mlpNormOutProbe.payload := rmsNorm.io.normOut.payload
//
//  val attnNormOutProbe = master(Flow(Bits(16 bits)))
//  attnNormOutProbe.valid := rmsNorm.io.normOut.valid & outCnt === 2047 & (isAttnLn)
//  attnNormOutProbe.payload := rmsNorm.io.normOut.payload
}
