package top

import cfgGen.LLaMA2_7B
import spinal.core._
import spinal.lib._
import util.{LUT2, LUT3}

import scala.language.postfixOps

/*
val vecOut2ReuseTag = List(ATTN_LN_OUT, MLP_LN_OUT)
val vecOut2ThroughTag = List(ATTN_Q_ROTATE, ATTN_QKV_OUT, MLP_OUT, PREFILL_TOKEN)
 */

class VecOutBuf(
                 width: Int,
                 dim: Int,
                 head: Int,
                 layer: Int,
                 numOfCore: Int,
                 bankLen: Int,
                 fifoDepth: Int,
                 split: Int
               ) extends Component {

  val parallelWidth = width * bankLen
  val addrWidth = log2Up(fifoDepth / 2)

  object PushPackLen {
    //    val tokenIn = dim / bankLen / numOfCore - 1
    val tokenIn = 0
    val attnLnOut = dim / bankLen - 1
    val lgLnOut = dim / bankLen / numOfCore - 1
    val qRotateOut = 0
    val mlpLnOut = dim / bankLen / numOfCore - 1
  }

  object PopPackLen {
    //    val throughToLn = dim / bankLen / numOfCore - 1
    val throughToLn = 0
    val lnToLogits = dim / bankLen / numOfCore - 1
    val reuseToQ = dim / bankLen - 1
    val reuseToK = dim / bankLen - 1
    val reuseToV = dim / bankLen - 1
    val qToQK = 0
    val qkvToOutProj = dim / bankLen / numOfCore
    val reuseToMlp = dim / bankLen / numOfCore - 1
  }

  val io = new Bundle {
    val input = slave(Flow(util.AxiFrame(Bits(parallelWidth bits), userBit = 6)))
    val output = master(Stream(Bits(parallelWidth bits)))
  }

  //  io.output.valid.addAttribute("mark_debug", "true")
  //  io.output.ready.addAttribute("mark_debug", "true")

  val validDly = Bool().setAsReg().init(False)
  val dataDly = Bits(parallelWidth bits).setAsReg()
  validDly := io.input.valid
  dataDly := io.input.tdata

  val status = new Bundle {
    val tokenIndexFlow = slave(Flow(Bits(6 bits)))
    val enPredictor = in Bool()
  }

  val ptrPush = new Area {
    val enPushCnt = Bool()
    val pushPackLen = UInt(log2Up(fifoDepth / 2) bits).setAsReg().init(PushPackLen.tokenIn)
    val pushStartAt = UInt(log2Up(fifoDepth / 2) bits).setAsReg().init(0)
    val pushStartAtNext = UInt(log2Up(fifoDepth / 2) bits)
    val pushReuse = Bool().setAsReg().init(False)
    val pushReuseNext = Bool()
    val pushPtr = UInt(log2Up(fifoDepth / 2) bits).setAsReg().init(0)
    val pushNext = UInt(log2Up(fifoDepth / 2) bits)
    val pushPtrOvf = pushPtr === pushPackLen
    pushPtr := pushNext
    pushNext := pushPtr
    pushReuse := pushReuseNext
    pushReuseNext := pushReuse
    pushStartAt := pushStartAtNext
    pushStartAtNext := pushStartAt
    when(enPushCnt) {
      pushNext := pushPtr + 1
      when(pushPtrOvf) {
        pushNext := pushStartAtNext
      }
    }
  }

  val pushStatus = new Area {

    // decode: tokenIn -> lnOut -> [qRotateOut -> qkvOut] -> mlpLnOut
    // prefill first token: tokenIn -> lnOut -> [qkvOut] -> mlpLnOut
    // prefill last layer: tokenIn -> lnOut
    // logits: tokenIn -> lnOut

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

    val prefillIn = Stream(Bool())
    prefillIn.valid := status.tokenIndexFlow.valid
    prefillIn.payload := status.tokenIndexFlow.payload === 0
    val prefillInLock = prefillIn.queue(64, forFMax = true)
    prefillInLock.ready := enLayerCntInc & lastLayer

    val prefillFirstToken = Bool().setAsReg().init(True)
    val logitsGen = Bool().setAsReg().init(False)
    val prefill = prefillInLock.payload
    prefillFirstToken.clearWhen(prefillInLock.fire)
    logitsGen.setWhen(prefillInLock.fire & ~prefill)

    val tokenIn = Bool().setAsReg().init(True)
    val lnOut = Bool().setAsReg().init(False)
    val qRotateOut = Bool().setAsReg().init(False)
    val qkvOut = Bool().setAsReg().init(False)
    val mlpLnOut = Bool().setAsReg().init(False)

    val stateTrig = ptrPush.enPushCnt & ptrPush.pushPtrOvf

    val headCnt = UInt(log2Up(head / numOfCore) + 1 bits).setAsReg().init(0)
    val headCntOvf = headCnt === head / numOfCore - 1
    val headCntNext = UInt(log2Up(head / numOfCore) + 1 bits)
    val headCntNextInc = headCntNext + 1

    headCntNext := headCnt
    headCnt := headCntNext

    when(tokenIn & stateTrig) {
      tokenIn.clear()
      lnOut.set()
      ptrPush.pushReuseNext := Mux(logitsGen, False, True)
      ptrPush.pushPackLen := Mux(logitsGen, U(PushPackLen.lgLnOut), U(PushPackLen.attnLnOut)).resized
      ptrPush.pushStartAtNext := 0
    }

    when(lnOut & stateTrig) {
      lnOut.clear()
      when(prefill & lastLayer) {
        tokenIn.set()
        ptrPush.pushReuseNext.clear()
        ptrPush.pushPackLen := PushPackLen.tokenIn
      }.elsewhen(prefillFirstToken) {
        qkvOut.set()
        ptrPush.pushReuseNext.clear()
        ptrPush.pushPackLen := headCntNextInc.resized
        ptrPush.pushStartAtNext := headCntNextInc.resized
      }.elsewhen(logitsGen) {
        tokenIn.set()
        ptrPush.pushReuseNext.clear()
        ptrPush.pushPackLen := PushPackLen.tokenIn
      }.otherwise {
        qRotateOut.set()
        ptrPush.pushReuseNext.clear()
        ptrPush.pushPackLen := PushPackLen.qRotateOut
        ptrPush.pushStartAtNext := 0
      }
    }

    when(qRotateOut & stateTrig) {
      qRotateOut.clear()
      qkvOut.set()
      ptrPush.pushPackLen := headCntNextInc.resized
      ptrPush.pushStartAtNext := headCntNextInc.resized
    }

    when(qkvOut & stateTrig) {

      when(prefillFirstToken) {
        ptrPush.pushPackLen := headCntNextInc.resized
        ptrPush.pushStartAtNext := headCntNextInc.resized
      }.otherwise {
        qkvOut.clear()
        qRotateOut.set()
        ptrPush.pushPackLen := PushPackLen.qRotateOut
        ptrPush.pushStartAtNext.clearAll()
      }

      headCntNext := headCnt + 1
      when(headCntOvf) {
        headCntNext.clearAll()
        qkvOut.clear()
        qRotateOut.clear()
        mlpLnOut.set()
        ptrPush.pushReuseNext.set()
        ptrPush.pushStartAtNext.clearAll()
        ptrPush.pushPackLen := PushPackLen.mlpLnOut
      }
    }

    when(mlpLnOut & stateTrig) {
      mlpLnOut.clear()
      tokenIn.set()
      ptrPush.pushReuseNext.clear()
      ptrPush.pushPackLen := PushPackLen.tokenIn
    }

    enLayerCntInc := mlpLnOut & stateTrig
    when(prefill & lastLayer) {
      enLayerCntInc := lnOut & stateTrig
    }
    logitsGen.clearWhen(lnOut & stateTrig)
  }

  val ptrPop = new Area {
    val enPopCnt = Bool()
    val popPackLen = UInt(log2Up(fifoDepth / 2) bits).setAsReg().init(PopPackLen.throughToLn)
    val popStartAt = UInt(log2Up(fifoDepth / 2) bits).setAsReg().init(0)
    val popStartAtNext = UInt(log2Up(fifoDepth / 2) bits)
    val popPtr = UInt(log2Up(fifoDepth / 2) bits).setAsReg().init(0)
    val popReuse = Bool().setAsReg().init(False)
    val popReuseNext = Bool()
    val popNext = UInt(log2Up(fifoDepth / 2) bits)
    val popPtrOvf = popPtr === popPackLen
    popPtr := popNext
    popNext := popPtr
    popReuse := popReuseNext
    popReuseNext := popReuse
    popStartAt := popStartAtNext
    popStartAtNext := popStartAt
    when(enPopCnt) {
      popNext := popPtr + 1
      when(popPtrOvf) {
        popNext := popStartAtNext
      }
    }
  }

  //  ptrPush.pushPtr.addAttribute("mark_debug", "true")
  //  ptrPop.popPtr.addAttribute("mark_debug", "true")

  val popStatus = new Area {

    // decode: throughToLn -> [reuseToQ -> qToQK -> reuseToK -> reuseToV] -> qkvToOutProj -> reuseToPredU -> reuseToGate -> reuseToUp
    // prefill first token: throughToLn -> [reuseToK -> reuseToV] -> qkvToOutProj -> reuseToPredU -> reuseToGate -> reuseToUp
    // prefill last layer: throughToLn -> [reuseToK -> reuseToV]
    // logits: throughToLn -> lnToLogits

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

    val prefillIn = Stream(Bool())
    prefillIn.valid := status.tokenIndexFlow.valid
    prefillIn.payload := status.tokenIndexFlow.payload === 0
    val prefillInLock = prefillIn.queue(64, forFMax = true)
    prefillInLock.ready := enLayerCntInc & lastLayer

    val prefillFirstToken = Bool().setAsReg().init(True)
    val logitsGen = Bool().setAsReg().init(False)
    val prefill = prefillInLock.payload
    prefillFirstToken.clearWhen(prefillInLock.fire)
    logitsGen.setWhen(prefillInLock.fire & ~prefill)

    val throughToLn = Bool().setAsReg().init(True)
    val lnToLogits = Bool().setAsReg().init(False)
    val reuseToQ = Bool().setAsReg().init(False)
    val qToQK = Bool().setAsReg().init(False)
    val reuseToK = Bool().setAsReg().init(False)
    val reuseToV = Bool().setAsReg().init(False)
    val qkvToOutProj = Bool().setAsReg().init(False)
    val reuseToPredU = Bool().setAsReg().init(False)
    val reuseToGate = Bool().setAsReg().init(False)
    val reuseToUp = Bool().setAsReg().init(False)

    val stateTrig = ptrPop.enPopCnt & ptrPop.popPtrOvf
    val prefillCond = prefillFirstToken || prefill & lastLayer

    val headCnt = UInt(log2Up(head / numOfCore) + 1 bits).setAsReg().init(0)
    val headCntOvf = headCnt === head / numOfCore - 1
    val headCntNext = headCnt + 1

    when(throughToLn & stateTrig) {
      throughToLn.clear()
      when(logitsGen) {
        lnToLogits.set()
        ptrPop.popPackLen := PopPackLen.lnToLogits
        ptrPop.popReuseNext.clear()
      }.elsewhen(prefillCond) {
        reuseToK.set()
        ptrPop.popPackLen := PopPackLen.reuseToK
        ptrPop.popReuseNext.set()
      }.otherwise {
        reuseToQ.set()
        ptrPop.popPackLen := PopPackLen.reuseToQ
        ptrPop.popReuseNext.set()
      }
    }

    when(lnToLogits & stateTrig) {
      lnToLogits.clear()
      throughToLn.set()
      ptrPop.popReuseNext.clear()
      ptrPop.popPackLen := PopPackLen.throughToLn
    }

    when(reuseToQ & stateTrig) {
      reuseToQ.clear()
      reuseToK.set()
      ptrPop.popPackLen := PopPackLen.reuseToK
      ptrPop.popReuseNext.set()
    }

    when(reuseToK & stateTrig) {
      reuseToK.clear()
      when(prefillCond) {
        reuseToV.set()
        ptrPop.popPackLen := PopPackLen.reuseToV
        ptrPop.popReuseNext.set()
      }.otherwise {
        qToQK.set()
        ptrPop.popPackLen := PopPackLen.qToQK
        ptrPop.popReuseNext.clear()
      }
    }

    when(qToQK & stateTrig) {
      qToQK.clear()
      reuseToV.set()
      ptrPop.popPackLen := PopPackLen.reuseToV
      ptrPop.popReuseNext.set()
    }

    when(reuseToV & stateTrig) {
      reuseToV.clear()
      headCnt := headCntNext

      when(prefillCond) {
        reuseToK.set()
        ptrPop.popPackLen := PopPackLen.reuseToK
        ptrPop.popReuseNext.set()
      }.otherwise {
        reuseToQ.set()
        ptrPop.popPackLen := PopPackLen.reuseToQ
        ptrPop.popReuseNext.set()
      }

      when(headCntOvf) {
        headCnt.clearAll()
        reuseToQ.clear()
        reuseToK.clear()
        ptrPop.popReuseNext.clear()

        when(prefill & lastLayer) {
          throughToLn.set()
          ptrPop.popPackLen := PopPackLen.throughToLn
        }.otherwise {
          qkvToOutProj.set()
          ptrPop.popPackLen := PopPackLen.qkvToOutProj
          ptrPop.popStartAtNext := 1
        }
      }
    }

    when(qkvToOutProj & stateTrig) {
      qkvToOutProj.clear()
      when(status.enPredictor) {
        reuseToPredU.set()
      }.otherwise {
        reuseToGate.set()
      }
      ptrPop.popStartAtNext := 0
      ptrPop.popPackLen := PopPackLen.reuseToMlp
      ptrPop.popReuseNext.set()
    }

    when(reuseToPredU & stateTrig) {
      reuseToPredU.clear()
      reuseToGate.set()
    }

    when(reuseToGate & stateTrig) {
      reuseToGate.clear()
      reuseToUp.set()
    }

    when(reuseToUp & stateTrig) {
      reuseToUp.clear()
      throughToLn.set()
      ptrPop.popPackLen := PopPackLen.throughToLn
      ptrPop.popReuseNext.clear()
    }

    enLayerCntInc := reuseToUp & stateTrig
    when(prefill & lastLayer) {
      enLayerCntInc := reuseToV & stateTrig & headCntOvf
    }
    logitsGen.clearWhen(lnToLogits & stateTrig)
  }

  val ram = new Area {
    val ramSplit = split * 4
    val pushEn = Array.fill(ramSplit)(Bool().setAsReg().init(False))
    val pushPtrLow = Array.fill(ramSplit)(UInt(log2Up(fifoDepth / 2) bits).setAsReg().init(0))
    val pushPtrMsb = Array.fill(ramSplit)(Bool().setAsReg().init(False))
    val pushPtr = (pushPtrMsb, pushPtrLow).zipped.map((high, low) => high ## low)

    val popEn = Array.fill(ramSplit)(Bool())
    val popPtrLow = Array.fill(ramSplit)(UInt(log2Up(fifoDepth / 2) bits).setAsReg().init(0))
    val popPtrMsb = Array.fill(ramSplit)(Bool().setAsReg().init(False))
    val popPtr = (popPtrMsb, popPtrLow).zipped.map((high, low) => high ## low)

    val ramIn = dataDly.subdivideIn(ramSplit slices)
    val ramOut = Array.fill(ramSplit)(Bits(parallelWidth / ramSplit bits))
    val mem = Array.fill(ramSplit)(Mem(Bits(parallelWidth / ramSplit bits), fifoDepth))
    mem.foreach(_.addAttribute("ram_style", "distributed"))
    for (i <- 0 until ramSplit) {
      mem(i).write(address = pushPtr(i).asUInt, data = ramIn(i), enable = pushEn(i))
      ramOut(i) := mem(i).readSync(address = popPtr(i).asUInt, enable = popEn(i))
    }

    pushEn.foreach(_.addAttribute("keep", "true"))
    pushPtrMsb.foreach(_.addAttribute("keep", "true"))
    pushPtrLow.foreach(_.addAttribute("keep", "true"))
    popPtrMsb.foreach(_.addAttribute("keep", "true"))
    popPtrLow.foreach(_.addAttribute("keep", "true"))
  }

  val reusePushFinish = Bool().setAsReg().init(False)
  reusePushFinish.setWhen(pushStatus.stateTrig & (pushStatus.lnOut & ~pushStatus.logitsGen || pushStatus.mlpLnOut))
  reusePushFinish.clearWhen(popStatus.stateTrig & (popStatus.reuseToV & popStatus.headCntOvf || popStatus.reuseToUp))
  val popLtPush = ptrPop.popPtr < ptrPush.pushPtr

  val popPre = Event
  val popPrePipe = popPre.throwWhen(popStatus.throughToLn).m2sPipe()

  ram.pushEn.foreach(_ := io.input.valid)
  ram.pushPtrLow.foreach(_ := ptrPush.pushNext)
  ram.pushPtrMsb.foreach(_ := ptrPush.pushReuseNext)

  //  ram.popEn.foreach(_ := popPre.ready)
  ram.popPtrLow.foreach(_ := ptrPop.popNext)
  ram.popPtrMsb.foreach(_ := ptrPop.popReuseNext)

  io.output.arbitrationFrom(popPrePipe)
  io.output.payload := Vec(ram.ramOut).asBits

  ptrPush.enPushCnt := validDly
  ptrPop.enPopCnt := popPre.fire

  popPre.valid.clear()
  when(ptrPop.popReuse) {
    when(reusePushFinish) {
      popPre.valid.set()
    }.otherwise {
      popPre.valid := popLtPush
    }
  }.otherwise {
    when(popStatus.throughToLn) {
      when(pushStatus.lnOut) {
        popPre.valid.set()
      }.otherwise {
        popPre.valid := popLtPush
      }
    }
    when(popStatus.lnToLogits) {
      when(pushStatus.tokenIn) {
        popPre.valid.set()
      }.otherwise {
        popPre.valid := popLtPush
      }
    }
    when(popStatus.qToQK) {
      popPre.valid := pushStatus.qkvOut
    }
    when(popStatus.qkvToOutProj) {
      popPre.valid := pushStatus.mlpLnOut
    }
  }

  //  val lutInst = Array.fill(ram.popEn.length)(new LUT2(11))
  //  for (i <- ram.popEn.indices) {
  //    lutInst(i).I0 := popPrePipe.ready
  //    lutInst(i).I1 := popPrePipe.valid
  //    ram.popEn(i) := lutInst(i).O
  //    lutInst(i).addAttribute("keep_hierarchy", "yes")
  //  }

  val lutInst = Array.fill(ram.popEn.length)(new LUT3(251))
  for (i <- ram.popEn.indices) {
    lutInst(i).I0 := popPrePipe.ready
    lutInst(i).I1 := popPrePipe.valid
    lutInst(i).I2 := popStatus.throughToLn
    ram.popEn(i) := lutInst(i).O
    lutInst(i).addAttribute("keep_hierarchy", "yes")
  }

  //  val rdy = Vec(Bool(), ram.popEn.length)
  //  for (i <- ram.popEn.indices) {
  //    rdy(i).addAttribute("keep", "true")
  //    rdy(i).addAttribute("don_touch", "yes")
  //    rdy(i) := ~popPrePipe.valid || popPrePipe.ready
  //    ram.popEn(i) := rdy(i)
  //  }


}
