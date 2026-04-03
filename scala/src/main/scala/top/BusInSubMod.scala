package top

import adapter._
import cfgGen.LLaMA2_7B
import convert._
import spinal.core._
import spinal.lib._
import util._

import scala.language.postfixOps

class BusInSubMod(
                   busWidth: Int,
                   width: Int,
                   bankLen: Int,
                   numOfCore: Int,
                   dim: Int,
                   layer: Int,
                   split: Int,

                   mlpOutTag: Int,
                   kvInTag: List[Int],
                   toResTag: List[Int],
                   toConvertTag: List[Int],

                   wkvOutFifoDepth: Int,
                   vecP2sFifoDepth: Int,

                   convert_latency: Int,
                   int4_conv: Flow[Bits] => Flow[Bits],
                   int8_conv: Flow[Bits] => Flow[Bits]
                 ) extends Component {

  val serialWidth = width
  val parallelWidth = width * bankLen
  val vecInBanks = dim / numOfCore / bankLen

  val io = new Bundle {
    val bus = slave(Stream(Fragment(util.AxiFrame(Bits(busWidth bits), userBit = 6))))
    val vecIn = slave(Flow(util.AxiFrame(Bits(parallelWidth bits), userBit = 6)))
    val vLocal = slave(Stream(util.AxiFrame(Bits(parallelWidth bits), userBit = 6)))
    val zeroInt4 = slave(Stream(Bits(8 bits)))
    val zeroInt8 = slave(Stream(Bits(8 bits)))

    val wkv = master(Stream(Bits(parallelWidth bits)))
    val p2sOut = master(Flow(util.AxiFrame(Bits(serialWidth bits), userBit = 6)))
    val toResBuf = master(Flow(Bits(parallelWidth bits)))
    val directOut = master(Flow(util.AxiFrame(Bits(parallelWidth bits), userBit = 6)))
  }

  val status = new Bundle {
    val tokenNextHit = in Bool()
    val mlpNextHit = in Bool()
    val vNextHit = in Bool()
    val flushRes = in Bool()
  }

  io.bus.valid.addAttribute("mark_debug", "true")
  io.bus.ready.addAttribute("mark_debug", "true")
  io.bus.tuser.addAttribute("mark_debug", "true")

  val kvHit = kvInTag.map(_ === io.bus.tuser).reduce(_ || _)
  val wkvFire = io.bus.fire

  val startInfer = Bool().setAsReg().init(False)
  startInfer.setWhen(wkvFire)
  val haltDetected = Bool().setAsReg().init(False)
  val haltCnt = UInt(16 bits).setAsReg().init(0)
  val haltCntEn = Bool()
  val haltCntClr = Bool()
  when(haltCntEn) {
    haltCnt := haltCnt + 1
    when(haltCnt >= 1024) {
      haltDetected.set()
    }
  }
  when(haltCntClr) {
    haltCnt.clearAll()
  }
  haltCntEn := startInfer & ~wkvFire
  haltCntClr := wkvFire
  haltDetected.addAttribute("mark_debug", "true")

  val selExtTokenSqr = Bool().setAsReg().init(True)
  val selLocalMlpSqr = Bool().setAsReg().init(False)
  val selVLocal = Bool().setAsReg().init(False)
  val en = wkvFire & io.bus.last

  when(en) {
    when(status.tokenNextHit)(selExtTokenSqr := True)
    when(status.mlpNextHit)(selLocalMlpSqr := True)
    when(status.vNextHit)(selVLocal := True)
  }

  val selPrefillSqrDly = RegNext(selExtTokenSqr, init = False)
  val s2p = new Serial2Parallel(width = busWidth, bankLen = 4)
  s2p.io.input.valid := wkvFire & selExtTokenSqr
  s2p.io.input.tdata := io.bus.tdata
  s2p.io.input.tuser := io.bus.tuser

  val muxOut = Flow(util.AxiFrame(Bits(parallelWidth bits), userBit = 6))
  val vecInCnt = UInt(log2Up(vecInBanks) bits).setAsReg().init(0)
  val sqrInVld = muxOut.valid & (selLocalMlpSqr || selPrefillSqrDly)
  val vecInCntOvf = vecInCnt === vecInBanks - 1
  when(sqrInVld) {
    vecInCnt := vecInCnt + 1
    when(vecInCntOvf) {
      vecInCnt.clearAll()
      selLocalMlpSqr.clear()
    }
  }

  selExtTokenSqr.clearWhen(wkvFire & io.bus.last & selExtTokenSqr)

  when(io.vLocal.valid) {
    selVLocal.clear()
  }
  io.vLocal.ready := selVLocal
  selVLocal.addAttribute("max_fanout", "100")

  val vecInFifo = new LargeBankFifo(Bits(parallelWidth bits), 32, forFMax = true, split = split)
  val vecInVld = Bool().setAsReg().init(False)
  vecInVld.addAttribute("max_fanout", "100")
  vecInVld := io.vecIn.valid & io.vecIn.tuser === mlpOutTag
  vecInFifo.io.push.valid := vecInVld
  vecInFifo.io.push.payload := RegNext(io.vecIn.tdata)
  val vecIn = Flow(util.AxiFrame(Bits(parallelWidth bits), userBit = 6))
  vecIn.valid := vecInFifo.io.pop.fire
  vecIn.tdata := vecInFifo.io.pop.payload
  vecIn.tuser := mlpOutTag

  muxOut.valid := vecIn.valid
  muxOut.tdata := vecIn.tdata
  muxOut.tuser := vecIn.tuser
  when(selPrefillSqrDly) {
    muxOut.valid := s2p.io.output.valid
    muxOut.tdata := s2p.io.output.tdata
    muxOut.tuser := s2p.io.output.tuser
  }

  val preScaleFactor = UInt(5 bits)
  preScaleFactor := U(log2Up(dim) / 2 + 2)
  when(selPrefillSqrDly) {
    preScaleFactor := U(log2Up(dim) / 2)
  }

  val preScaleFlow = Flow(util.AxiFrame(Bits(parallelWidth bits), userBit = 6))
  preScaleFlow.valid := sqrInVld
  preScaleFlow.tdata := Fp16ScaleDown.vec(muxOut.tdata, preScaleFactor)
  preScaleFlow.tuser := muxOut.tuser

  val preScaleFlowPipe = preScaleFlow.m2sPipe
  io.directOut << preScaleFlowPipe

  val conv = new Int4Int8FP16Conv(bankLen, convert_latency, int8_conv)
//  conv.io.int4InVld := wkvFire & ~kvHit & ~selExtTokenSqr
//  conv.io.int8InVld := wkvFire & kvHit
  conv.io.selInt8 := kvHit
  conv.io.inputData.valid := wkvFire & ~selExtTokenSqr
  conv.io.inputData.payload := io.bus.tdata
  conv.io.zeroInt4 << io.zeroInt4
  conv.io.zeroInt8 << io.zeroInt8

  val fifo = new util.LargeBankFifo(Bits(parallelWidth bits), wkvOutFifoDepth, forFMax = true, split = split)
  fifo.io.pop >> io.wkv

  val select = UInt(2 bits)
  val selectPreDly = Delay(select, convert_latency - 1, init = U(0, 2 bits))
  val selectDly = RegNext(selectPreDly, init = U(0, 2 bits))
//  selectDly.addAttribute("mark_debug", "true")

  val pushVld = Vec(
    conv.io.output.valid,
    io.vLocal.valid,
    preScaleFlowPipe.valid
  )(selectDly)

  val convOutSlice = conv.io.output.payload.subdivideIn(8 slices)
  val vLocalSlice = io.vLocal.tdata.subdivideIn(8 slices)
  val preScaleSlice = preScaleFlowPipe.tdata.subdivideIn(8 slices)
  val selectCopy = Vec(UInt(2 bits), 8)
  val toFifo = Vec(Bits(width * bankLen / 8 bits), 8)
  for (i <- 0 until 8) {
    selectCopy(i).setAsReg().init(0)
    selectCopy(i) := selectPreDly
    selectCopy(i).addAttribute("keep", "true")
    toFifo(i) := Vec(convOutSlice(i), vLocalSlice(i), preScaleSlice(i))(selectCopy(i))
  }
  val pushPayload = toFifo.asBits

  //  val pushPayload = Vec(
  //    conv.io.output.payload,
  //    io.vLocal.tdata,
  //    preScaleFlowPipe.tdata
  //  )(selectDly)

  fifo.io.push.valid := RegNext(pushVld, init = False)
  fifo.io.push.payload := RegNext(pushPayload)

  val ready = RegNext(fifo.io.availability >= convert_latency + 3, False)
  io.bus.ready := Mux(selLocalMlpSqr || selVLocal, False, ready)

  select.clearAll()
  when(selVLocal)(select := 1)
  when(selExtTokenSqr || selLocalMlpSqr)(select := 2)

  val muxOutFlow = Flow(util.AxiFrame(NoData(), userBit = 6))
  val toResFlow = FlowGate(muxOutFlow, toResTag)
  val p2sInFlow = FlowGate.keepTag(muxOutFlow, toConvertTag)

  muxOutFlow.valid := muxOut.valid
  muxOutFlow.tuser := muxOut.tuser

  val tensorCnt = UInt(log2Up(vecInBanks) bits).setAsReg().init(0)
  val tensorOvf = tensorCnt === vecInBanks - 1
  val layerCnt = UInt(log2Up(layer) bits).setAsReg().init(0)
  val layerBound = UInt(log2Up(layer) bits).setAsReg().init(layer - 1)
  val layerOvf = layerCnt === layerBound
  val enInc = toResFlow.valid
  val prefillLayerCond = muxOut.tuser === toResTag(1)
  when(enInc) {
    tensorCnt := tensorCnt + 1
    when(tensorOvf) {
      tensorCnt := 0
      when(muxOut.tuser =/= toResTag.head) {
        layerBound := Mux(prefillLayerCond, U(layer - 2), U(layer - 1))
      }
      when(muxOut.tuser === toResTag.head) {
        layerCnt := layerCnt + 1
      }
      when(layerOvf) {
        layerCnt := 0
      }
    }
  }

  val payloadDly = RegNext(muxOut.tdata)
  val tagDly = RegNext(muxOut.tuser)

  io.toResBuf.valid := RegNext(toResFlow.valid & ~layerOvf, init = False)
  io.toResBuf.payload := payloadDly

  val p2sBuf = new StreamAxiFrameFifo(
    dataType = Bits(parallelWidth bits),
    depth = vecP2sFifoDepth,
    forFMax = true,
    userBit = 6,
    destBit = -1,
    largeBank = true,
    largeBankSplit = split
  )

  p2sBuf.io.input.valid := RegNext(p2sInFlow.valid, init = False)
  p2sBuf.io.input.tdata := payloadDly
  p2sBuf.io.input.tuser := tagDly

  val p2s = new Parallel2Serial(width = serialWidth, bankLen = bankLen)
  p2s.io.input << p2sBuf.io.output
  io.p2sOut << p2s.io.output.m2sPipe()

  vecInFifo.io.pop.ready := Delay(select === 2, convert_latency, init = False)
}
