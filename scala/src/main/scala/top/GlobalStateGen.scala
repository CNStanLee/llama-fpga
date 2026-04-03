package top

import cfgGen.LLaMA2_7B
import spinal.core._
import spinal.lib._

import scala.language.postfixOps

class GlobalStateGen(
                      busInVecCnt: Int,
                      engineOutVecCnt: Int,
                      dotOutVecCnt: Int,
                      layer: Int,
                      head: Int,
                      numOfCore: Int,
                      maxToken: Int,
                      attnVParamTag: Int,
                      mlpDParamTag: Int,
                      lmHeadParamTag: Int,
                      tokenTag: (Int, Int, Int),
                      mlpTensorTag: Int,
                      vTensorTag: (Int, Int)
                    ) extends Component {

  val io = new Bundle {
    val busIn = slave(Flow(Fragment(Bits(6 bits))))
    val engineOut = slave(Flow(Bits(6 bits)))
    val dotOut = slave(Flow(Bits(6 bits)))
    val gtCnt = slave(Flow(Bits(16 bits)))
  }

  val status = new Bundle {
    val argmaxVld = in Bool()
    val endOfDecode = in Bool()

    val prefill = out Bool()
    val prefillFirstToken = out Bool()
    val lastHead = out Bool()
    val lastLayer = out Bool()
    val logitsGen = out Bool()

    val flushRes = out Bool()
    val tokenNextHit = out Bool()
    val mlpNextHit = out Bool()
    val vNextHit = out Bool()
    val token = out UInt (log2Up(maxToken) bits)
    val enPredictor = out Bool()
    val nextLayer = out Bool()
    val nonZeroCnt = out Bits (16 bits)
  }

  status.enPredictor.clear()

  val busInCnt = UInt(log2Up(busInVecCnt) bits).setAsReg().init(0)
  val busInCntOvf = busInCnt === busInVecCnt - 1
  val busInVld = Bool()
  when(busInVld) {
    busInCnt := busInCnt + 1
    when(busInCntOvf) {
      busInCnt.clearAll()
    }
  }

  val engineOutCnt = UInt(log2Up(engineOutVecCnt) bits).setAsReg().init(0)
  val engineOutCntOvf = engineOutCnt === engineOutVecCnt - 1
  val engineOutVld = Bool()
  when(engineOutVld) {
    engineOutCnt := engineOutCnt + 1
    when(engineOutCntOvf) {
      engineOutCnt.clearAll()
    }
  }

  val dotOutCnt = UInt(log2Up(dotOutVecCnt) bits).setAsReg().init(0)
  val dotOutCntOvf = dotOutCnt === dotOutVecCnt - 1
  val dotOutVld = Bool()
  when(dotOutVld) {
    dotOutCnt := dotOutCnt + 1
    when(dotOutCntOvf) {
      dotOutCnt.clearAll()
    }
  }

  val headCnt = UInt(log2Up(head / numOfCore) bits).setAsReg().init(0)
  val headCntOvf = headCnt === (head / numOfCore) - 1
  val lastHead = Bool().setAsReg().init(False)
  val headVld = Bool()
  when(headVld) {
    headCnt := headCnt + 1
    when(headCnt === (head / numOfCore) - 2)(lastHead.set())
    when(headCntOvf) {
      lastHead.clear()
      headCnt.clearAll()
    }
  }

  val prefillLastHit = io.busIn.fragment === tokenTag._2
  val prefillHit = io.busIn.fragment === tokenTag._1 || prefillLastHit
  val attnVHit = io.busIn.fragment === attnVParamTag
  val mlpDHit = io.busIn.fragment === mlpDParamTag
  val lmHeadHit = io.busIn.fragment === lmHeadParamTag

  val mlpHit = io.engineOut.payload === mlpTensorTag
  val vHit = io.dotOut.payload === vTensorTag._1 || io.dotOut.payload === vTensorTag._2

  busInVld := io.busIn.valid & prefillHit
  engineOutVld := io.engineOut.valid & mlpHit
  dotOutVld := io.dotOut.valid & vHit
  headVld := io.busIn.valid & io.busIn.last & attnVHit

  val prefillDone = busInVld & busInCntOvf
  val prefillLastDone = busInVld & busInCntOvf & prefillLastHit
  val mlpDone = engineOutVld & engineOutCntOvf
  val vDone = dotOutVld & dotOutCntOvf

  val prefill = Bool().setAsReg().init(True)
  val prefillFirstToken = Bool().setAsReg().init(True)
  val lastLayer = Bool().setAsReg().init(False)
  prefill.clearWhen(prefillLastDone).setWhen(status.endOfDecode)

  val enInc = Bool()
  val layerCnt = UInt(log2Up(layer) bits).setAsReg().init(0)
  val tokenCnt = UInt(log2Up(maxToken) bits).setAsReg().init(0)
  val layerOvf = layerCnt === layer - 1
  val beforeLastLayer = layerCnt === layer - 2

  when(enInc) {
    layerCnt := layerCnt + 1
    when(beforeLastLayer) {
      lastLayer.set()
    }
    when(layerOvf) {
      layerCnt.clearAll()
      lastLayer.clear()
      prefillFirstToken.clear()
      tokenCnt := tokenCnt + 1
      when(tokenCnt === maxToken - 1) {
        tokenCnt.clearAll()
      }
    }
  }

  enInc := mlpDone
  when(prefill & lastLayer) {
    enInc := vDone
  }

  val nonZeroCnt = Bits(16 bits).setAsReg().init(0xffff)
  when(io.gtCnt.valid)(nonZeroCnt := io.gtCnt.payload)
  when(enInc)(nonZeroCnt := 0xffff)

  val logitsGen = Bool().setAsReg().init(False)
  logitsGen.setWhen(~prefill & mlpDone & lastLayer).clearWhen(status.argmaxVld)

  val noAttn = prefillFirstToken || prefill & lastLayer
  val tokenNextHit = attnVHit & prefill & lastLayer & lastHead || lmHeadHit
  val vNextHit = attnVHit & ~noAttn
  val mlpNextHit = mlpDHit
  val flushRes = Mux(prefill, beforeLastLayer, lastLayer) & engineOutVld

  status.prefill := prefill
  status.prefillFirstToken := prefillFirstToken
  status.lastLayer := lastLayer
  status.lastHead := lastHead
  status.logitsGen := logitsGen

  status.token := tokenCnt
  status.flushRes := flushRes
  status.tokenNextHit := tokenNextHit
  status.vNextHit := vNextHit
  status.mlpNextHit := mlpNextHit
  status.nextLayer := enInc
  status.nonZeroCnt := nonZeroCnt

  tokenCnt.addAttribute("mark_debug", "true")
  layerCnt.addAttribute("mark_debug", "true")
  nonZeroCnt.addAttribute("mark_debug", "true")
}
