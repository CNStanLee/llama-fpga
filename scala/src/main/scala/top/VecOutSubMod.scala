package top

import adapter._
import spinal.core._
import spinal.lib._
import util._

import scala.language.postfixOps

class VecOutSubMod(
                    width: Int,
                    dim: Int,
                    head: Int,
                    layer: Int,
                    numOfCore: Int,
                    bankLen: Int,
                    fifoDepth: Int,
                    split: Int,
                    lnOutGateTag: List[Int],
                    vLocalTag: Int,
                    dotOutGateTag: List[Int],
                    ropeOutGateTag: Int,
                    serial2VecOutTag: List[Int],
                    busIn2VecOutTag: List[Int],
                    engine2VecOutTag: List[Int],

                    sqrtHeadDim: Int,
                    mul_func: (Flow[Bits], Flow[Bits]) => Flow[Bits]
                  ) extends Component {

  val serialWidth = width
  val parallelWidth = serialWidth * bankLen

  val io = new Bundle {
    val lnOut = slave(Flow(util.AxiFrame(Bits(serialWidth bits), userBit = 6)))
    val dotOut = slave(Flow(util.AxiFrame(Bits(serialWidth bits), userBit = 6)))
    val ropeOut = slave(Flow(util.AxiFrame(Bits(serialWidth bits), userBit = 6)))
    val busVecIn = slave(Flow(util.AxiFrame(Bits(parallelWidth bits), userBit = 6)))
    val engineVecIn = slave(Flow(util.AxiFrame(Bits(parallelWidth bits), userBit = 6)))
    val vLocal = master(Flow(util.AxiFrame(Bits(parallelWidth bits), userBit = 6)))
    val vecOut = master(Stream(Bits(parallelWidth bits)))
  }

  val vecBuf = new VecOutBuf(
    width = width,
    dim = dim,
    head = head,
    layer = layer,
    numOfCore = numOfCore,
    bankLen = bankLen,
    fifoDepth = fifoDepth,
    split = split
  )

  val status = vecBuf.status.toIo()

  val cond = Bool()
  val lnOut = FlowGate.keepTag(io.lnOut, lnOutGateTag)
  val dotOutPre = FlowGate.keepTag(io.dotOut, dotOutGateTag)
  val dotOut = dotOutPre.throwWhen(cond)
  val ropeOut = FlowGate(io.ropeOut, List(ropeOutGateTag))

  val prefillIn = Stream(Bool())
  prefillIn.valid := status.tokenIndexFlow.valid
  prefillIn.payload := status.tokenIndexFlow.payload === 0
  val prefillInLock = prefillIn.queue(64, forFMax = true)
  val prefill = prefillInLock.payload

  val layerCnt = UInt(log2Up(layer) bits).setAsReg().init(0)
  val lastLayer = Bool().setAsReg().init(False)
  val layerCntAbout2Ovf = layerCnt === layer - 2
  val enLayerCntInc = Bool()
  when(enLayerCntInc) {
    layerCnt := layerCnt + 1
    when(layerCntAbout2Ovf)(lastLayer.set())
    when(lastLayer) {
      layerCnt.clearAll()
      lastLayer.clear()
    }
  }
  val vCnt = UInt(log2Up(dim / numOfCore) bits).setAsReg().init(0)
  val vCntOvf = vCnt === dim / numOfCore - 1
  val vCntVld = Bool()
  when(vCntVld) {
    vCnt := vCnt + 1
    when(vCntOvf) {
      vCnt.clearAll()
    }
  }
  vCntVld := dotOutPre.valid
  enLayerCntInc := vCntVld & vCntOvf
  prefillInLock.ready := enLayerCntInc & lastLayer
  cond := prefill & lastLayer

  val sqrtD = Flow(Bits(width bits))
  sqrtD.payload := sqrtHeadDim
  sqrtD.valid.set()

  //  val ropeScale = div_func(ropeOut, sqrtD)
  //  val ropeScale = ropeOut
  val ropeScale = mul_func(ropeOut, sqrtD)

  val rope2s2p = Flow(AxiFrame(Bits(serialWidth bits), userBit = 6))
  rope2s2p.valid := ropeScale.valid
  rope2s2p.tdata := ropeScale.payload
  rope2s2p.tuser := ropeOutGateTag

  val (s2pInMux, s2pInErr) = FlowMux(Vec(lnOut, dotOut, rope2s2p))
  val s2pIn = s2pInMux.m2sPipe()
  val s2p = new Serial2Parallel(width, bankLen)
  s2p.io.input << s2pIn

  //  lnOut.valid.addAttribute("mark_debug", "true")
  //  allGatherOut.valid.addAttribute("mark_debug", "true")
  //  dotOut.valid.addAttribute("mark_debug", "true")
  //  s2pIn.valid.addAttribute("mark_debug", "true")
  //  s2p.io.output.valid.addAttribute("mark_debug", "true")

  //  s2p.io.input.valid.addAttribute("mark_debug", "true")
  //  s2p.io.output.valid.addAttribute("mark_debug", "true")

  //  val s2p2vLocalVld = Bool().setAsReg().init(False)
  //  s2p2vLocalVld.addAttribute("max_fanout", "100")
  //  s2p2vLocalVld := s2p.io.output.valid & s2p.io.output.tuser === vLocalTag
  //  val s2p2FifoVld =  Bool().setAsReg().init(False)
  //  s2p2FifoVld.addAttribute("max_fanout", "100")
  //  s2p2FifoVld := s2p.io.output.valid & serial2VecOutTag.map(s2p.io.output.tuser === _).reduce(_ || _)
  //  val s2pTag = RegNext(s2p.io.output.tuser)
  //  val s2pData = RegNext(s2p.io.output.tdata)

  val vLocalStream = Stream(util.AxiFrame(Bits(parallelWidth bits), userBit = 6))
  val vLocal = FlowGate.keepTag(s2p.io.output, List(vLocalTag))
//  io.vLocal << vLocalStream.m2sPipe()
//  vLocalStream.valid := vLocal.valid
//  vLocalStream.payload := vLocal.payload
//  io.vLocal.valid.addAttribute("max_fanout", "true")
  io.vLocal.valid := vLocal.valid
  io.vLocal.tdata := vLocal.tdata
  io.vLocal.tuser := vLocal.tuser

  //  io.vLocal.valid := s2p2vLocalVld
  //  io.vLocal.tdata := s2pData
  //  io.vLocal.tuser := s2pTag

  //  val s2pToFifo = Flow(util.AxiFrame(Bits(parallelWidth bits), userBit = 6))
  //  s2pToFifo.valid := s2p2FifoVld
  //  s2pToFifo.tdata := s2pData
  //  s2pToFifo.tuser := s2pTag

  val s2pToFifo = FlowGate.keepTag(s2p.io.output, serial2VecOutTag).m2sPipe
  val busVecInToFifo = FlowGate.keepTag(io.busVecIn, busIn2VecOutTag)
  val engineVecInToFifo = FlowGate.keepTag(io.engineVecIn, engine2VecOutTag).m2sPipe
  val (toBuf, toBufErr) = FlowMux(Vec(s2pToFifo, busVecInToFifo, engineVecInToFifo))

  vecBuf.io.input << toBuf.m2sPipe
  io.vecOut << vecBuf.io.output
}
