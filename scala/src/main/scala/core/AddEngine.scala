package core

import spinal.core._
import spinal.lib._
import util.Fp16ScaleUp

import scala.language.postfixOps

class AddEngine(
                 width: Int,
                 bankLen: Int,
                 maxFirstDim: Int,
                 mul_latency: Int,
                 add_latency: Int,
                 acc_latency: Int,
                 inLineRam: Boolean,
                 mul_func: (Flow[Bits], Flow[Bits]) => Flow[Bits],
                 add_func: (Flow[Bits], Flow[Bits]) => Flow[Bits],
                 acc_func: Flow[Fragment[Bits]] => Flow[Fragment[Bits]]
               ) extends Component {

  val serialBit = width
  val parallelBit = width * bankLen
  val minFirstDim = add_latency + 2
  val reduceLatency = log2Up(bankLen) * add_latency

  require(isPow2(minFirstDim))

  case class Config() extends Bundle {

    val data = Bits(32 bits)
    data.setPartialName("")

    def firstDim = data.drop(16).take(8).asUInt

    def secondDim = data.take(16).asUInt

    def isAxpy = data.takeHigh(8).lsb

    def enResAdd = data.takeHigh(8)(1)

    def tag = data.takeHigh(6)
  }

  val io = new Bundle {
    val mulRes = slave(Stream(Bits(parallelBit bits)))
    val resAdd = slave(Stream(Bits(parallelBit bits)))
    val postScale = slave(Stream(Bits(serialBit bits)))
    val vecOut = master(Flow(util.AxiFrame(Bits(parallelBit bits), userBit = 6)))
    val scalarOut = master(Flow(util.AxiFrame(Bits(serialBit bits), userBit = 6)))
    val cfg = slave(Stream(Config()))
    val postCfgTag = out Bits (6 bits)
  }

  noIoPrefix()
  util.AxiStreamSpecRenamer(io.mulRes)
  util.AxiStreamSpecRenamer(io.resAdd)
  util.AxiStreamSpecRenamer(io.postScale)
  util.AxiStreamSpecRenamer(io.vecOut)
  util.AxiStreamSpecRenamer(io.scalarOut)
  util.AxiStreamSpecRenamer(io.cfg)

  val tag = io.cfg.tag
  val tagReduceDly = Delay(tag, acc_latency + mul_latency, init = B(0, 6 bits))
  val tagAddDly = Delay(tag, add_latency, init = B(0, 6 bits))

  val ram = if (!inLineRam) new Bundle {
    val rdPort = master(util.MemRdPort(Bits(parallelBit bits), maxFirstDim))
    val wrPort = master(Flow(util.MemWrPort(Bits(parallelBit bits), maxFirstDim)))
  } else null

  val dotCond = ~io.cfg.isAxpy
  val fAxpyCond = io.cfg.isAxpy & io.cfg.firstDim =/= 0
  val pAxpyCond = io.cfg.isAxpy & io.cfg.firstDim === 0
  val ret = StreamDemuxOh(io.cfg, Seq(dotCond, fAxpyCond, pAxpyCond))

  val fifoCtrl = new util.StreamFifoCtrl(Bits(parallelBit bit), maxFirstDim, forFMax = true)

  if (!inLineRam) {
    fifoCtrl.rdPort <> ram.rdPort
    fifoCtrl.wrPort <> ram.wrPort
  }
  else {
    val mem = Mem(Bits(parallelBit bits), maxFirstDim)
    mem.addAttribute("ram_style", "distributed")
    mem.write(
      enable = fifoCtrl.wrPort.valid,
      address = fifoCtrl.wrPort.payload.address,
      data = fifoCtrl.wrPort.payload.data
    )
    fifoCtrl.rdPort.rsp := mem.readSync(
      enable = fifoCtrl.rdPort.cmd.valid,
      address = fifoCtrl.rdPort.cmd.payload
    )
  }

  val addLogic = new Area {
    val aSel = UInt(2 bits)
    val bSel = UInt(2 bits)

    val psumVld = Bool()
    val psumDlyVld = Bool()

    val mul = Vec(Bits(width bits), bankLen)
    val psum = Vec(Bits(width bits), bankLen)
    val psumDly = Vec(Bits(width bits), bankLen)
    val res = Vec(Bits(width bits), bankLen)
    val zero = Bits(width bits)
    zero.clearAll()

    (mul, io.mulRes.payload.subdivideIn(bankLen slices)).zipped.foreach(_ := _)
    (res, io.resAdd.payload.subdivideIn(bankLen slices)).zipped.foreach(_ := _)

    val a = Vec(Flow(Bits(width bits)), bankLen)
    val b = Vec(Flow(Bits(width bits)), bankLen)
    val c = Vec(Flow(Bits(width bits)), bankLen)

    a.foreach(_.valid.set())
    b.foreach(_.valid.set())

    (c, a, b).zipped.foreach((c, a, b) => c := add_func(a, b))

    for (i <- 0 until bankLen / 2 - 1) {
      a(i).payload := Vec(mul(i), c(2 * i + 1).payload, psumDly(i), zero)(aSel)
      b(i).payload := Vec(psum(i), c(2 * i + 2).payload, res(i), zero)(bSel)
    }

    val offset = bankLen / 2 - 1
    for (i <- 0 until bankLen / 2) {
      a(i + offset).payload := Vec(mul(i + offset), mul(i * 2 + 0), psumDly(i + offset), zero)(aSel)
      b(i + offset).payload := Vec(psum(i + offset), mul(i * 2 + 1), res(i + offset), zero)(bSel)
    }

    a.last.payload := Vec(mul.last, zero, psumDly.last, zero)(aSel)
    b.last.payload := Vec(psum.last, zero, res.last, zero)(bSel)

    val vecOut = Vec(Bits(width bits), bankLen)
    val scalarOut = Bits(serialBit bits)

    (vecOut, c).zipped.foreach(_ := _.payload)
    scalarOut := c.head.payload

    val popDly = RegNextWhen(fifoCtrl.io.pop.payload, fifoCtrl.io.pop.valid)
    (psumDly, popDly.subdivideIn(bankLen slices)).zipped.foreach(_ := _)
    (psum, fifoCtrl.io.pop.payload.subdivideIn(bankLen slices)).zipped.foreach(_ := _)
    fifoCtrl.io.push.payload := vecOut.asBits

    io.vecOut.tdata := vecOut.asBits
  }

  val dotCtrl = new Area {
    val cfg = ret(0)
    val cfgPayload = RegNext(cfg.payload)
    val cfgVld = Bool().setAsReg().init(False)
    val cfgVldNext = cfg.valid
    when(cfg.ready)(cfgVldNext.clear())
    cfgVld.addAttribute("max_fanout", "100")
    cfgVld := cfgVldNext

    val aSel = U(1, 2 bits)
    val bSel = U(1, 2 bits)

    val enMulResCnt = io.mulRes.fire & cfgVld
    val (mulResCnt, mulResCntOvf) = util.LoopsCntGen.wireOvf(
      List(cfgPayload.firstDim, cfgPayload.secondDim), enMulResCnt
    )

    val enAddOutCnt = Delay(enMulResCnt, reduceLatency, init = False)
    val addOutCntOvf = mulResCntOvf.map(ovf => Delay(ovf, reduceLatency, init = False))

    //    val flag = Bool().setAsReg().init(False)
    //    val flagClearCond = RegNext(enAddOutCnt & addOutCntOvf.reduce(_ & _), init = False)
    //    flag.setWhen(enMulResCnt & mulResCntOvf.reduce(_ & _))
    //    flag.clearWhen(flagClearCond)

    val flagClrCond = enMulResCnt & mulResCntOvf.reduce(_ & _)
    val flagSetCond = RegNext(enAddOutCnt & addOutCntOvf.reduce(_ & _), init = False)
    val flagInv = Bool().setAsReg().init(True)
    //    flagInv.clearWhen(flagClrCond)
    //    flagInv.setWhen(flagSetCond)
    val flagInvNext = Bool()
    flagInvNext := flagInv
    flagInv := flagInvNext
    when(flagClrCond)(flagInvNext.clear())
    when(flagSetCond)(flagInvNext.set())

    val accCnt = UInt(log2Up(maxFirstDim) bits).setAsReg().init(0)
    val accCntOvf = accCnt === cfgPayload.firstDim
    val accCntOvfDly = Delay(accCntOvf, reduceLatency + mul_latency, init = False)
    when(enMulResCnt) {
      accCnt := accCnt + 1
      when(accCntOvf) {
        accCnt.clearAll()
      }
    }

    val scaleFlow = Flow(Bits(width bits))
    val reduceFlow = Flow(Bits(width bits))
    val resFlow = mul_func(scaleFlow, reduceFlow)
    reduceFlow.valid := enAddOutCnt
    reduceFlow.payload := addLogic.scalarOut
    scaleFlow.valid := reduceFlow.valid
    scaleFlow.payload := io.postScale.payload
    io.postScale.ready := reduceFlow.valid

    val accIn = Flow(Fragment(Bits(width bits)))
    val accOut = acc_func(accIn)
    accIn.valid := resFlow.valid
    accIn.fragment := resFlow.payload
    accIn.last := accCntOvfDly

    io.scalarOut.valid := accOut.valid & accOut.last
    io.scalarOut.tdata := accOut.fragment
    io.scalarOut.tuser := tagReduceDly

    cfg.ready := enAddOutCnt & addOutCntOvf.reduce(_ & _)
  }

  io.postCfgTag := dotCtrl.cfgPayload.asBits.takeHigh(6)

  val fAxpyCtrl = new Area {
    val cfg = ret(1)
    val cfgPayload = RegNext(cfg.payload)
    val cfgVld = Bool().setAsReg().init(False)
    val cfgVldNext = cfg.valid
    when(cfg.ready)(cfgVldNext.clear())
    cfgVld.addAttribute("max_fanout", "100")
    cfgVld := cfgVldNext

    val psumPopEn = Bool()
    val psumPushEn = Bool()
    val aSel = UInt(2 bits)
    val bSel = UInt(2 bits)

    val enMulResCnt = io.mulRes.fire & cfgVld
    val (mulResCnt, mulResCntOvf) = util.LoopsCntGen.wireOvf(
      List(cfgPayload.firstDim, cfgPayload.secondDim), enMulResCnt
    )

    val mulResCntLastZero = Bool().setAsReg().init(True)
    val mulResCntLastNotZero = Bool().setAsReg().init(False)
    when(enMulResCnt) {
      when(mulResCntOvf.head) {
        mulResCntLastZero.clear()
        mulResCntLastNotZero.set()
        when(mulResCntOvf.last) {
          mulResCntLastZero.set()
          mulResCntLastNotZero.clear()
        }
      }
    }


    val enAddOutCnt = Delay(enMulResCnt, add_latency, init = False)
    val addOutCntOvf = mulResCntOvf.map(ovf => Delay(ovf, add_latency, init = False))

    io.resAdd.ready.clear()
    psumPopEn.clear()
    //    psumPushEn := enAddOutCnt & ~addOutCntOvf.last

    val psumPushEnReg = Delay(enMulResCnt & ~mulResCntOvf.last, add_latency, init = False)
    psumPushEn := psumPushEnReg

    aSel.clearAll()
    bSel := 3
    when(enMulResCnt & mulResCntLastZero & cfgPayload.enResAdd) {
      io.resAdd.ready.set()
      bSel := 2
    }
    when(enMulResCnt & mulResCntLastNotZero) {
      psumPopEn.set()
      bSel := 0
    }

    //    val psumVld = Bool()
    //    psumVld := fifoCtrl.io.pop.fire

    cfg.ready := enMulResCnt & mulResCntOvf.reduce(_ & _)
    val vecOutVld = enAddOutCnt & addOutCntOvf.last
  }

  //  val bypassCtrl = new Area {
  //    val cfg = ret(3)
  //    val cfgVld = Bool().setAsReg().init(False)
  //    val cfgVldNext = cfg.valid
  //    when(cfg.ready)(cfgVldNext.clear())
  //    cfgVld.addAttribute("max_fanout", "100")
  //    cfgVld := cfgVldNext
  //
  //    val psumPopEn = False
  //    val psumPushEn = False
  //    val aSel = UInt(2 bits)
  //    val bSel = UInt(2 bits)
  //    aSel.clearAll()
  //    bSel := 3
  //
  //    val inVld = io.mulRes.fire & cfgVld
  //    val outVld = Delay(inVld, add_latency, init = False)
  //
  //    cfg.ready := inVld
  //    val vecOutVld = outVld
  //  }

  val pAxpyCtrl = new Area {
    val cfg = ret(2)
    val cfgPayload = RegNext(cfg.payload)
    val cfgVld = Bool().setAsReg().init(False)
    val cfgVldNext = cfg.valid
    when(cfg.ready)(cfgVldNext.clear())
    cfgVld.addAttribute("max_fanout", "100")
    cfgVld := cfgVldNext

    val cycleReduce = Bool().setAsReg().init(False)
    val cycleReduceDly = Bool().setAsReg().init(False)

    val enMulResCnt = io.mulRes.fire & cfgVld
    val mulResCnt = UInt(16 bits).setAsReg().init(0)
    val mulResCntLow = mulResCnt.take(log2Up(minFirstDim)).asUInt
    val mulResCntHigh = mulResCnt.drop(log2Up(minFirstDim)).asUInt
    val mulResCntOvf = mulResCnt === cfgPayload.secondDim

    val mulResCntHighZero = Bool().setAsReg().init(True)
    val mulResCntHighNotZero = Bool().setAsReg().init(False)
    when(enMulResCnt) {
      when(mulResCntLow.andR) {
        mulResCntHighZero.clear()
        mulResCntHighNotZero.set()
      }
    }

    when(enMulResCnt) {
      mulResCnt := mulResCnt + 1
      when(mulResCntOvf) {
        mulResCnt.clearAll()
        mulResCntHighZero.set()
        mulResCntHighNotZero.clear()
        cycleReduce.set()
      }
    }

    val enMulResCntDly = Delay(enMulResCnt, add_latency, init = False)
    val enAddOutCnt = enMulResCntDly & ~cycleReduceDly
    val addOutCntOvf = Delay(mulResCntOvf, add_latency, init = False)
    when(enAddOutCnt & addOutCntOvf) {
      cycleReduceDly.set()
    }

    val reduceVldDly = Bool()
    val enCycleCnt = cycleReduceDly & reduceVldDly
    val cycleCnt = UInt(log2Up(minFirstDim) bits).setAsReg().init(0)
    val lastElem = cycleCnt === Min(U(minFirstDim - 2), cfgPayload.secondDim - 1)
    when(enCycleCnt) {
      cycleCnt := cycleCnt + 1
      when(lastElem) {
        cycleCnt.clearAll()
        cycleReduce.clear()
        cycleReduceDly.clear()
      }
    }

    val psumPushEn = Bool()
    psumPushEn := Mux(cycleReduceDly, enCycleCnt & ~(enCycleCnt & lastElem), enMulResCntDly)

    val psumPopEn = Bool()
    psumPopEn.clear()
    when(enMulResCnt & mulResCntHighNotZero)(psumPopEn.set())
    when(cycleReduce)(psumPopEn.set())

    val aSel = UInt(2 bits)
    aSel(0).clear()
    aSel(1) := cycleReduce
    //    aSel.clearAll()
    //    when(cycleReduce)(aSel := 2)

    val bSel = UInt(2 bits)
    val bSelBit = enMulResCnt & mulResCntHighZero
    bSel(0) := bSelBit
    bSel(1) := bSelBit
    //    bSel.clearAll()
    //    when(enMulResCnt & mulResCntHighZero)(bSel := 3)

    //    val flag = Bool().setAsReg().init(False)
    //    val flagClearCond = RegNext(enCycleCnt & lastElem, init = False)
    //    flag.setWhen(enMulResCnt & mulResCntOvf)
    //    flag.clearWhen(flagClearCond)

    val flagClrCond = enMulResCnt & mulResCntOvf
    val flagSetCond = RegNext(enCycleCnt & lastElem, init = False)
    val flagInv = Bool().setAsReg().init(True)
    //    flagInv.clearWhen(flagClrCond)
    //    flagInv.setWhen(flagSetCond)
    val flagInvNext = Bool()
    flagInvNext := flagInv
    flagInv := flagInvNext
    when(flagClrCond)(flagInvNext.clear())
    when(flagSetCond)(flagInvNext.set())

    val vldToggle = Bool().setAsReg().init(False)
    val reduceVld = vldToggle & fifoCtrl.io.pop.fire
    vldToggle.toggleWhen(cycleReduce & fifoCtrl.io.pop.fire)

    val psumVld = Bool()
    psumVld := fifoCtrl.io.pop.fire
    when(cycleReduce) {
      psumVld := reduceVld
    }
    cfg.ready := enCycleCnt & lastElem

    reduceVldDly := Delay(psumVld & reduceVld, add_latency, init = False)
    val vecOutVld = cfgVld & enCycleCnt & lastElem
  }

  io.vecOut.valid := fAxpyCtrl.vecOutVld | pAxpyCtrl.vecOutVld
  io.vecOut.tuser := MuxOH(
    Vec(fAxpyCtrl.vecOutVld, pAxpyCtrl.vecOutVld),
    Vec(tagAddDly, tag)
  )

  //  io.vecOut.valid := fAxpyCtrl.vecOutVld | pAxpyCtrl.vecOutVld | bypassCtrl.vecOutVld
  //  io.vecOut.tuser := MuxOH(
  //    Vec(fAxpyCtrl.vecOutVld, pAxpyCtrl.vecOutVld, bypassCtrl.vecOutVld),
  //    Vec(tagAddDly, tag, tagAddDly)
  //  )

  //  fifoCtrl.io.push.valid := MuxOH(
  //    Vec(fAxpyCtrl.cfgVld, pAxpyCtrl.cfgVld, bypassCtrl.cfgVld),
  //    Vec(fAxpyCtrl.psumPushEn, pAxpyCtrl.psumPushEn, bypassCtrl.psumPushEn)
  //  )
  //
  //  fifoCtrl.io.pop.ready := MuxOH(
  //    Vec(fAxpyCtrl.cfgVld, pAxpyCtrl.cfgVld, bypassCtrl.cfgVld),
  //    Vec(fAxpyCtrl.psumPopEn, pAxpyCtrl.psumPopEn, bypassCtrl.psumPopEn)
  //  )
  //
  //  addLogic.aSel := MuxOH(
  //    Vec(dotCtrl.cfgVld, fAxpyCtrl.cfgVld, pAxpyCtrl.cfgVld, bypassCtrl.cfgVld),
  //    Vec(dotCtrl.aSel, fAxpyCtrl.aSel, pAxpyCtrl.aSel, bypassCtrl.aSel)
  //  )
  //
  //  addLogic.bSel := MuxOH(
  //    Vec(dotCtrl.cfgVld, fAxpyCtrl.cfgVld, pAxpyCtrl.cfgVld, bypassCtrl.cfgVld),
  //    Vec(dotCtrl.bSel, fAxpyCtrl.bSel, pAxpyCtrl.bSel, bypassCtrl.bSel)
  //  )

  val selVldNextOneHot = pAxpyCtrl.cfgVldNext ## fAxpyCtrl.cfgVldNext ## dotCtrl.cfgVldNext
  val selVldNextUInt = OHToUInt(selVldNextOneHot)
  val selVldUIntReg = UInt(2 bits).setAsReg().init(0)
  selVldUIntReg := selVldNextUInt

  //  val selVldOneHot = pAxpyCtrl.cfgVld ## fAxpyCtrl.cfgVld ## dotCtrl.cfgVld
  //  val selVldUInt = OHToUInt(selVldOneHot)

  fifoCtrl.io.push.valid := Vec(False, fAxpyCtrl.psumPushEn, pAxpyCtrl.psumPushEn)(selVldUIntReg)
  fifoCtrl.io.pop.ready := Vec(False, fAxpyCtrl.psumPopEn, pAxpyCtrl.psumPopEn)(selVldUIntReg)
  addLogic.aSel := Vec(dotCtrl.aSel, fAxpyCtrl.aSel, pAxpyCtrl.aSel)(selVldUIntReg)
  addLogic.bSel := Vec(dotCtrl.bSel, fAxpyCtrl.bSel, pAxpyCtrl.bSel)(selVldUIntReg)
  //  io.mulRes.ready := Vec(~dotCtrl.flag, True, ~pAxpyCtrl.flag)(selVldUInt)

  //  io.mulRes.ready.clear()
  //  when(dotCtrl.cfgVld)(io.mulRes.ready := dotCtrl.flagInv)
  //  when(fAxpyCtrl.cfgVld)(io.mulRes.ready.set())
  //  when(pAxpyCtrl.cfgVld)(io.mulRes.ready := pAxpyCtrl.flagInv)

  val ready = Bool().setAsReg().init(False)
  val readyNext = Bool()
  ready := readyNext
  readyNext := False
  when(dotCtrl.cfgVldNext)(readyNext := dotCtrl.flagInvNext)
  when(fAxpyCtrl.cfgVldNext)(readyNext := True)
  when(pAxpyCtrl.cfgVldNext)(readyNext := pAxpyCtrl.flagInvNext)

  io.mulRes.ready := ready
}
