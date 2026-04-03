package top

import adapter.{FlowGate, Parallel2Serial, Serial2Parallel}
import spinal.core._
import spinal.lib._
import util._

import scala.language.postfixOps

class BusInSubModNew(
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
    val vLocal = slave(Flow(util.AxiFrame(Bits(parallelWidth bits), userBit = 6)))
    val zeroInt4 = slave(Stream(Bits(8 bits)))
    val zeroInt8 = slave(Stream(Bits(8 bits)))

    val wkv = master(Stream(Bits(parallelWidth bits)))
    val p2sOut = master(Flow(Fragment(util.AxiFrame(Bits(serialWidth bits), userBit = 6))))
    val toResBuf = master(Flow(Bits(parallelWidth bits)))
    val directOut = master(Flow(util.AxiFrame(Bits(parallelWidth bits), userBit = 6)))
  }

  val status = new Bundle {
    val tokenNextHit = in Bool()
    val mlpNextHit = in Bool()
    val vNextHit = in Bool()
    val flushRes = in Bool()
    val logitsGen = in Bool()
  }

  val kvHit = kvInTag.map(_ === io.bus.tuser).reduce(_ || _)
  val wkvFire = io.bus.fire

  val selExtTokenSqr = Bool().setAsReg().init(True)
  val selVLocal = Bool().setAsReg().init(False)
  val en = wkvFire & io.bus.last

  when(en) {
    when(status.tokenNextHit)(selExtTokenSqr := True)
    when(status.vNextHit)(selVLocal := True)
  }
  selExtTokenSqr.clearWhen(wkvFire & io.bus.last & selExtTokenSqr)

  //  val vLocalLock = Flow(Bits(parallelWidth bits))
  //  vLocalLock.valid.setAsReg().init(False)
  //  vLocalLock.payload.setAsReg()
  //  when(io.vLocal.valid){
  //    vLocalLock.valid.set()
  //    vLocalLock.payload := io.vLocal.tdata
  //  }
  //  val vLocalFire = vLocalLock.valid & selVLocal
  //  vLocalLock.valid.clearWhen(vLocalFire)
  //  selVLocal.clearWhen(vLocalFire)

  val vecSplit = 8

  val vLocalVec = Vec(Stream(Bits(parallelWidth / vecSplit bits)), vecSplit)
  val vLocalPayload = io.vLocal.tdata.subdivideIn(vecSplit slices)
  vLocalVec.foreach(_.valid := io.vLocal.valid)
  (vLocalVec, vLocalPayload).zipped.foreach(_.payload := _)
  val vLocalLockVec = vLocalVec.map(_.m2sPipe())
  val selVLocalVec = Vec(Bool().setAsReg().init(False), 8)
  when(en) {
    when(status.vNextHit)(selVLocalVec.foreach(_.set()))
  }
  (vLocalLockVec, selVLocalVec).zipped.foreach(_.ready := _)
  (selVLocalVec, vLocalLockVec).zipped.foreach((s, t) => s.clearWhen(t.valid))

  val vLocal = Event
  vLocal.valid := io.vLocal.valid
  val vLocalLock = vLocal.m2sPipe()
  vLocalLock.ready := selVLocal
  selVLocal.clearWhen(vLocalLock.valid)

  vLocalLockVec.foreach(_.valid.addAttribute("keep", "true"))
  selVLocalVec.foreach(_.addAttribute("keep", "true"))
  vLocalLock.valid.addAttribute("max_fanout", 100)
  selVLocal.addAttribute("max_fanout", 100)

  val s2p = new Serial2Parallel(width = busWidth, bankLen = 4)
  s2p.io.input.valid := wkvFire & selExtTokenSqr
  s2p.io.input.tdata := io.bus.tdata
  s2p.io.input.tuser := io.bus.tuser

  val conv = new convert.Int4Int8FP16Conv(bankLen, convert_latency, int8_conv)
  conv.io.selInt8 := kvHit
  conv.io.inputData.valid := wkvFire & ~selExtTokenSqr
  conv.io.inputData.payload := io.bus.tdata
  conv.io.zeroInt4 << io.zeroInt4
  conv.io.zeroInt8 << io.zeroInt8

  val fifo = new util.LargeBankFifo(Bits(parallelWidth bits), wkvOutFifoDepth, forFMax = true, split = split)
  fifo.io.pop >> io.wkv

  val select = UInt(1 bits)
  val selectPreDly = Delay(select, convert_latency - 1, init = U(0, 1 bits))
  val selectDly = RegNext(selectPreDly, init = U(0, 1 bits))

  val pushVld = Vec(
    conv.io.output.valid,
    vLocalLock.fire
  )(selectDly)

  val convOutSlice = conv.io.output.payload.subdivideIn(vecSplit slices)
  val vLocalSlice = vLocalLockVec.map(_.payload)
  val selectCopy = Vec(UInt(1 bits), vecSplit)
  val toFifo = Vec(Bits(width * bankLen / vecSplit bits), vecSplit)
  for (i <- 0 until vecSplit) {
    selectCopy(i).setAsReg().init(0)
    selectCopy(i) := selectPreDly
    selectCopy(i).addAttribute("keep", "true")
    toFifo(i) := Vec(convOutSlice(i), vLocalSlice(i))(selectCopy(i))
  }
  val pushPayload = toFifo.asBits
  fifo.io.push.valid := RegNext(pushVld, init = False)
  fifo.io.push.payload := RegNext(pushPayload)

  select.clearAll()
  when(selVLocal)(select := 1)

  val ready = RegNext(fifo.io.availability >= convert_latency + 3, False)
  io.bus.ready := ready
  when(selVLocal)(io.bus.ready := False)
  when(selExtTokenSqr)(io.bus.ready := ~status.logitsGen)

  val muxOut = Flow(util.AxiFrame(Bits(parallelWidth bits), userBit = 6))
  val muxOutFlow = Flow(util.AxiFrame(NoData(), userBit = 6))
  val toResFlow = FlowGate(muxOutFlow, toResTag)
  val p2sInFlow = FlowGate.keepTag(muxOutFlow, toConvertTag)

  muxOutFlow.valid := muxOut.valid
  muxOutFlow.tuser := muxOut.tuser
  val payloadDly = RegNext(muxOut.tdata)
  val tagDly = RegNext(muxOut.tuser)

  val selPrefillSqrDly = RegNext(selExtTokenSqr, init = False)
  muxOut.valid := io.vecIn.valid & io.vecIn.tuser === mlpOutTag
  muxOut.tdata := io.vecIn.tdata
  muxOut.tuser := mlpOutTag
  when(selPrefillSqrDly) {
    muxOut.valid := s2p.io.output.valid
    muxOut.tdata := s2p.io.output.tdata
    muxOut.tuser := s2p.io.output.tuser
  }

  val tensorCnt = UInt(log2Up(vecInBanks) bits).setAsReg().init(0)
  val tensorOvf = tensorCnt === vecInBanks - 1
  val layerCnt = UInt(log2Up(layer) bits).setAsReg().init(0)
  val layerBound = UInt(log2Up(layer) bits).setAsReg().init(layer - 1)
  val layerOvf = layerCnt === layerBound
  val enInc = toResFlow.valid
  val prefillLayerCond = muxOut.tuser === 0
  when(enInc) {
    tensorCnt := tensorCnt + 1
    when(tensorOvf) {
      tensorCnt := 0
      when(muxOut.tuser =/= mlpOutTag) {
        layerBound := Mux(prefillLayerCond, U(layer - 2), U(layer - 1))
      }
      when(muxOut.tuser === mlpOutTag) {
        layerCnt := layerCnt + 1
      }
      when(layerOvf) {
        layerCnt := 0
      }
    }
  }

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

  io.toResBuf.valid := RegNext(toResFlow.valid & ~layerOvf, init = False)
  io.toResBuf.payload := payloadDly

  val vecInCnt = UInt(log2Up(vecInBanks) bits).setAsReg().init(0)
  val vecInCntOvf = vecInCnt === vecInBanks - 1
  when(muxOut.valid) {
    vecInCnt := vecInCnt + 1
    when(vecInCntOvf) {
      vecInCnt.clearAll()
    }
  }


  val directOutVld = Bool().setAsReg().init(False)
  directOutVld.addAttribute("max_fanout", 100)
  directOutVld := muxOut.valid & vecInCnt === 0
  io.directOut.valid := directOutVld
  io.directOut.tuser := 0
  io.directOut.tdata.clearAll()

  val p2sOutCnt = UInt(log2Up(dim / numOfCore) bits).setAsReg().init(0)
  val p2sOutCntOvf = p2sOutCnt === dim / numOfCore - 1
  when(p2s.io.output.valid) {
    p2sOutCnt := p2sOutCnt + 1
    when(p2sOutCntOvf) {
      p2sOutCnt.clearAll()
    }
  }

  val p2sOut = Flow(Fragment(util.AxiFrame(Bits(serialWidth bits), userBit = 6)))
  p2sOut.valid := p2s.io.output.valid
  p2sOut.fragment := p2s.io.output.payload
  p2sOut.last := p2sOutCntOvf
  io.p2sOut << p2sOut.m2sPipe()
}
