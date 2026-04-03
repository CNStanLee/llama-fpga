package cfgGen

import spinal.core._
import spinal.lib._

import scala.language.postfixOps

class GenCfg(
              dim: Int,
              mlpDim: Int,
              predDim: Int,
              vocabSize: Int,
              head: Int,
              layer: Int,
              bankLen: Int,
              numOfCore: Int,
              maxToken: Int
            ) extends Component {

  val headDim = dim / head

  val io = new Bundle {
    val cfg = master(Stream(Bits(32 bits)))
  }

  val status = new Bundle {
    //    val prefill = in Bool()
    val tokenIndexFlow = slave(Flow(Bits(6 bits)))
    val enPredictor = in Bool()
  }

  import LLaMA2_7B._

  val token = UInt(log2Up(maxToken) bits).setAsReg().init(0)
  val firstToken = token === 0

  val attnLnCfg = GenCfgBytes.constant(dim / bankLen / numOfCore, 1, tensor.ATTN_LN_SQR_OUT)
  val kNoAttnCfg = GenCfgBytes.constant(dim / bankLen, headDim, tensor.ATTN_K_NO_ATTN)
  val vNoAttnCfg = GenCfgBytes.constant(dim / bankLen, headDim, tensor.ATTN_V_NO_ATTN)
  val qCfg = GenCfgBytes.constant(dim / bankLen, headDim, tensor.ATTN_Q_OUT)
  val kCfg = GenCfgBytes.constant(dim / bankLen, headDim, tensor.ATTN_K_OUT)
  val vCfg = GenCfgBytes.constant(dim / bankLen, headDim, tensor.ATTN_V_OUT)
  val kCacheCfg = GenCfgBytes.variable(1, token, tensor.ATTN_QK_CACHE)
  val vCacheCfg = GenCfgBytes.variable(1, token + 1, tensor.ATTN_QKV_OUT, isAxpy = true)
  val attnOCfg = GenCfgBytes.constant(dim / bankLen / numOfCore, dim, tensor.ATTN_PROJ_OUT)
  val mlpPredUCfg = GenCfgBytes.constant(dim / bankLen / numOfCore, predDim, tensor.MLP_PRED_U_OUT)
  val mlpPredDCfg = GenCfgBytes.constant(mlpDim / bankLen / numOfCore, 1, tensor.MLP_PRED_D_OUT, isAxpy = true)
  val mlpGDenseCfg = GenCfgBytes.constant(dim / bankLen / numOfCore, mlpDim, tensor.MLP_G_PROJ_OUT)
  val mlpGSparseCfg = GenCfgBytes.constant(dim / bankLen / numOfCore, 1, tensor.MLP_G_PROJ_OUT)
  val mlpUCfg = GenCfgBytes.constant(dim / bankLen / numOfCore, 1, tensor.MLP_U_PROJ_OUT)
  val mlpDCfg = GenCfgBytes.constant(dim / bankLen / numOfCore, 1, tensor.MLP_OUT, isAxpy = true, resAdd = true)
  val logitsLnCfg = GenCfgBytes.constant(dim / bankLen / numOfCore, 1, tensor.LOGITS_LN_SQR_OUT)
  val logitsOutCfg = GenCfgBytes.constant(dim / bankLen / numOfCore, vocabSize, tensor.LOGITS_PROJ_OUT)

  val attnLn = Stream(Fragment(Bits(32 bits)))
  attnLn.valid.set()
  attnLn.fragment := attnLnCfg.asBits
  attnLn.last.set()

  val attnO = Stream(Fragment(Bits(32 bits)))
  attnO.valid.set()
  attnO.fragment := attnOCfg.asBits
  attnO.last.set()

  val attnKV = new Area {
    val vec = Vec(kNoAttnCfg, vNoAttnCfg)
    val cnt = UInt(log2Up(vec.length) bits).setAsReg().init(0)
    val cntOvf = cnt === vec.length - 1
    val cfg = Stream(Fragment(Bits(32 bits)))
    val cfgPipe = cfg.m2sPipe()
    cfg.valid.set()
    cfg.fragment := vec(cnt).asBits
    cfg.last := cntOvf
    when(cfg.fire) {
      cnt := cnt + 1
      when(cntOvf) {
        cnt.clearAll()
      }
    }
  }

  val attnQKV = new Area {
    val vec = Vec(qCfg, kCfg, kCacheCfg, vCfg, vCacheCfg)
    val cnt = UInt(log2Up(vec.length) bits).setAsReg().init(0)
    val cntOvf = cnt === vec.length - 1
    val cfg = Stream(Fragment(Bits(32 bits)))
    val cfgPipe = cfg.m2sPipe()
    cfg.valid.set()
    cfg.fragment := vec(cnt).asBits
    cfg.last := cntOvf
    when(cfg.fire) {
      cnt := cnt + 1
      when(cntOvf) {
        cnt.clearAll()
      }
    }
  }

  val mlpSparse = new Area {
    //    val vec = Vec(mlpPredUCfg, mlpPredDCfg, mlpGSparseCfg, mlpUCfg, mlpDCfg)
    //    val cnt = UInt(log2Up(vec.length) bits).setAsReg().init(0)
    //    val cntOvf = cnt === vec.length - 1
    //    val cfg = Stream(Fragment(Bits(32 bits)))
    //    val cfgPipe = cfg.m2sPipe()
    //    cfg.valid.set()
    //    cfg.fragment := vec(cnt).asBits
    //    cfg.last := cntOvf
    //    when(cfg.fire) {
    //      cnt := cnt + 1
    //      when(cntOvf) {
    //        cnt.clearAll()
    //      }
    //    }
    val cfgPipe = Stream(Fragment(Bits(32 bits)))
    cfgPipe.valid.clear()
    cfgPipe.payload.clearAll()
  }

  val mlpDense = new Area {
    val vec = Vec(mlpGDenseCfg, mlpUCfg, mlpDCfg)
    val cnt = UInt(log2Up(vec.length) bits).setAsReg().init(0)
    val cntOvf = cnt === vec.length - 1
    val cfg = Stream(Fragment(Bits(32 bits)))
    val cfgPipe = cfg.m2sPipe()
    cfg.valid.set()
    cfg.fragment := vec(cnt).asBits
    cfg.last := cntOvf
    when(cfg.fire) {
      cnt := cnt + 1
      when(cntOvf) {
        cnt.clearAll()
      }
    }
  }

  val logits = new Area {
    val vec = Vec(logitsLnCfg, logitsOutCfg)
    val cnt = UInt(log2Up(vec.length) bits).setAsReg().init(0)
    val cntOvf = cnt === vec.length - 1
    val cfg = Stream(Fragment(Bits(32 bits)))
    val cfgPipe = cfg.m2sPipe()
    cfg.valid.set()
    cfg.fragment := vec(cnt).asBits
    cfg.last := cntOvf
    when(cfg.fire) {
      cnt := cnt + 1
      when(cntOvf) {
        cnt.clearAll()
      }
    }
  }

  val mux = new StreamMux(Fragment(Bits(32 bits)), 7)
  mux.io.inputs(0) << attnLn
  mux.io.inputs(1) << attnKV.cfgPipe
  mux.io.inputs(2) << attnQKV.cfgPipe
  mux.io.inputs(3) << attnO
  mux.io.inputs(4) << mlpSparse.cfgPipe
  mux.io.inputs(5) << mlpDense.cfgPipe
  mux.io.inputs(6) << logits.cfgPipe

  val headCnt = UInt(log2Up(head / numOfCore) bits).setAsReg().init(0)
  val headCntOvf = headCnt === head / numOfCore - 1
  val enHeadCntInc = Bool()
  when(enHeadCntInc) {
    headCnt := headCnt + 1
    when(headCntOvf) {
      headCnt.clearAll()
    }
  }

  val layerCnt = UInt(log2Up(layer) bits).setAsReg().init(0)
  val layerCntOvf = layerCnt === layer - 1
  val enLayerCntInc = Bool()
  when(enLayerCntInc) {
    layerCnt := layerCnt + 1
    when(layerCntOvf) {
      layerCnt.clearAll()
    }
  }

  val enTokenCnt = enLayerCntInc & layerCntOvf
  when(enTokenCnt) {
    token := token + 1
  }

  val kvDone = attnKV.cfgPipe.fire & attnKV.cfgPipe.last
  val qkvDone = attnQKV.cfgPipe.fire & attnQKV.cfgPipe.last
  val sparseMlpDone = mlpSparse.cfgPipe.fire & mlpSparse.cfgPipe.last
  val denseMlpDone = mlpDense.cfgPipe.fire & mlpDense.cfgPipe.last
  val mlpDone = sparseMlpDone || denseMlpDone
  val logitsDone = logits.cfgPipe.fire & logits.cfgPipe.last

  val prefillIn = Stream(Bool())
  prefillIn.valid := status.tokenIndexFlow.valid
  prefillIn.payload := status.tokenIndexFlow.payload === 0

  val prefillLock = prefillIn.queue(64, forFMax = true)
  prefillLock.ready := enTokenCnt
  val prefill = prefillLock.payload

  enHeadCntInc := kvDone || qkvDone
  enLayerCntInc := mlpDone
  when(prefill & layerCntOvf) {
    enLayerCntInc := kvDone & headCntOvf
  }

  val select = UInt(log2Up(7) bits).setAsReg().init(0)
  val selectNext = UInt(log2Up(7) bits)
  selectNext := select
  select := selectNext
  mux.io.select := select

  when(select === 0 & attnLn.fire) {
    when(firstToken || prefill & layerCntOvf) {
      selectNext := 1
    }.otherwise {
      selectNext := 2
    }
  }
  when(select === 1 & kvDone & headCntOvf) {
    when(layerCntOvf) {
      selectNext := 0
    }.otherwise {
      selectNext := 3
    }
  }
  when(select === 2 & qkvDone & headCntOvf) {
    selectNext := 3
  }
  when(select === 3 & attnO.fire) {
    when(status.enPredictor) {
      selectNext := 4
    }.otherwise {
      selectNext := 5
    }
  }
  when(select === 4 & sparseMlpDone) {
    when(layerCntOvf) {
      selectNext := 6
    } otherwise {
      selectNext := 0
    }
  }
  when(select === 5 & denseMlpDone) {
    when(layerCntOvf) {
      selectNext := 6
    } otherwise {
      selectNext := 0
    }
  }
  when(select === 6 & logitsDone) {
    selectNext := 0
  }

  val cfg = Stream(Bits(32 bits))
  cfg.arbitrationFrom(mux.io.output)
  cfg.payload := mux.io.output.payload

  //  io.cfg.arbitrationFrom(mux.io.output)
  //  io.cfg.payload := mux.io.output.payload

  val throwCond = cfg.payload === attnLnCfg || cfg.payload === logitsLnCfg
  io.cfg << cfg.throwWhen(throwCond).m2sPipe()
}
