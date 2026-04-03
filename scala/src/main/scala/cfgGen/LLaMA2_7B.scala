package cfgGen

object LLaMA2_7B {

  object modelCfg {
    val layer = 32
    val head = 32
    val dim = 4096
    val mlpDim = 11008
    //    val mlpDim = 11264
    val predDim = 1024
    val vocabSize = 32000
    val headDim = dim / head
    val sqrtHeadDim = 0x2da8
  }

  object tinyModelCfg {
    val scaleFactor = 8
    val layer = 2
    val head = 32
    val dim = modelCfg.dim / scaleFactor
    val mlpDim = modelCfg.mlpDim / scaleFactor
    val predDim = modelCfg.predDim / scaleFactor
    val vocabSize = dim
    val headDim = dim / head
    //    val sqrtHeadDim = 0x35a8 // 32
    val sqrtHeadDim = 0x3400 // 64
    //    val sqrtHeadDim = 0x31a8 // 128
    //    val sqrtHeadDim = 0x2da8 // 512
  }

  object quant {
    val w = 4
    val kv = 8
    val s = 16
  }

  object param {
    val ATTN_LN_SCALE = 3
    val ATTN_W_Q = 4

    val ATTN_W_K = 5
    val ATTN_K_CACHE = 6
    val ATTN_W_V = 7
    val ATTN_V_CACHE = 8

    val ATTN_KV_SCALE = 9
    val ATTN_KV_ZERO = 10

    val ATTN_W_O = 11
    val MLP_LN_SCALE = 12
    val MLP_PRED_W_U = 13
    val MLP_PRED_W_D = 14
    val MLP_W_G = 15
    val MLP_W_U = 16
    val MLP_W_D = 17

    val OUT_LN_SCALE = 18
    val LM_HEAD_W = 19
  }

  object tensor {

    val PREFILL_TOKEN = 0
    val PREFILL_LAST_TOKEN = 1
    val DECODE_TOKEN = 2

    val ATTN_LN_SQR_OUT = 3
    val ATTN_LN_OUT = 4

    val ATTN_Q_OUT = 5
    val ATTN_K_OUT = 6
    val ATTN_V_OUT = 7
    val ATTN_Q_ROTATE = 8
    val ATTN_K_ROTATE = 9
    val ATTN_K_QUANT = 10
    val ATTN_V_QUANT = 11
    val ATTN_QK_LOCAL = 12
    val ATTN_QK_CACHE = 13
    val ATTN_SOFTMAX_OUT = 14
    val ATTN_QKV_OUT = 15
    val ATTN_PROJ_OUT = 16

    val ATTN_K_NO_ATTN = 17
    val ATTN_K_NO_ATTN_ROTATE = 18

    val ATTN_V_NO_ATTN = 19

    val MLP_LN_SQR_OUT = 20
    val MLP_LN_OUT = 21
    val MLP_G_PROJ_OUT = 22
    val MLP_G_FILTER = 23
    val MLP_U_PROJ_OUT = 24
    val MLP_UG = 25
    val MLP_UG_FILTER = 26
    val MLP_PRED_U_OUT = 27
    val MLP_PRED_D_OUT = 28
    val MLP_PRED_U_FILTER = 29
    val MLP_PRED_D_FILTER = 30
    val MLP_OUT = 31

    val LOGITS_LN_SQR_OUT = 32
    val LOGITS_LN_OUT = 34
    val LOGITS_PROJ_OUT = 35
    val END_TOKEN = 2
  }

  object tag {

    import tensor._
    import param._

    val kvInTag = List(ATTN_K_CACHE, ATTN_V_CACHE)

    val extTokenTag = (PREFILL_TOKEN, PREFILL_LAST_TOKEN, DECODE_TOKEN)
    val logitsTag = (LOGITS_PROJ_OUT, END_TOKEN)
    val resAddTag = ATTN_PROJ_OUT
    val vLocalTag = ATTN_V_OUT
    val normFilterTag = ATTN_PROJ_OUT
    val resOut2NodeTag = ATTN_PROJ_OUT
    val p2sOut2NodeTag = List(MLP_OUT, PREFILL_TOKEN, PREFILL_LAST_TOKEN, DECODE_TOKEN)
    val index2NodeTag = MLP_PRED_D_FILTER
    val index2UgTag = (MLP_PRED_D_FILTER, MLP_G_FILTER, MLP_UG_FILTER)

    val qkMulTag = (ATTN_Q_ROTATE, ATTN_K_ROTATE, ATTN_QK_LOCAL)
    val ugMulTag = (MLP_U_PROJ_OUT, MLP_G_PROJ_OUT, MLP_UG)
    val axpyTensorInTag = (MLP_UG, ATTN_SOFTMAX_OUT, MLP_PRED_U_FILTER)
    val axpyParamInTag = List(ATTN_V_CACHE, MLP_PRED_W_D, MLP_W_D)
    val vTag = (ATTN_V_OUT, ATTN_V_NO_ATTN)

    val vecIn2ResTag = List(MLP_OUT, PREFILL_TOKEN, PREFILL_LAST_TOKEN, DECODE_TOKEN)
    val vecIn2ScalarTag = List(MLP_OUT, MLP_PRED_D_OUT, PREFILL_TOKEN, PREFILL_LAST_TOKEN, DECODE_TOKEN)

    val serial2VecOutTag = List(ATTN_LN_OUT, MLP_LN_OUT, LOGITS_LN_OUT, ATTN_Q_ROTATE, ATTN_V_NO_ATTN)
    val busIn2VecOutTag = List(MLP_OUT, PREFILL_TOKEN, PREFILL_LAST_TOKEN, DECODE_TOKEN)
    val engine2VecOutTag = List(ATTN_QKV_OUT)

    val lnOut2VecTag = List(MLP_LN_OUT, LOGITS_LN_OUT, ATTN_LN_OUT)
    val dotOut2VecTag = List(ATTN_V_OUT, ATTN_V_NO_ATTN)
    val rope2VecTag = ATTN_Q_ROTATE
    val dotOut2NodeTag = List(
      MLP_PRED_U_OUT,
      MLP_U_PROJ_OUT,
      MLP_G_PROJ_OUT,
      LOGITS_LN_SQR_OUT,
      LOGITS_PROJ_OUT
    )
    val cfgInsertTag = (
      MLP_PRED_D_OUT,
      MLP_G_PROJ_OUT,
      MLP_U_PROJ_OUT,
      MLP_OUT
    )

    val lnScaleTag = List(ATTN_LN_SCALE, MLP_LN_SCALE, OUT_LN_SCALE)
    val denseDotParamTag = List(ATTN_W_Q, ATTN_W_K, ATTN_W_V, ATTN_W_O, MLP_PRED_W_U, LM_HEAD_W)
    val denseDotTensorTag = List(ATTN_Q_OUT, ATTN_K_OUT, ATTN_V_OUT, ATTN_PROJ_OUT, ATTN_K_NO_ATTN, ATTN_V_NO_ATTN, MLP_PRED_U_OUT, LOGITS_PROJ_OUT)
    val dotSqrTensorTag = List(ATTN_LN_SQR_OUT, LOGITS_LN_SQR_OUT)

    val axiLnBusTag = List(ATTN_LN_SCALE, MLP_LN_SCALE, OUT_LN_SCALE)
    val axiDenseBusTag = List(ATTN_W_Q, ATTN_W_K, ATTN_W_V, ATTN_W_O, MLP_PRED_W_U, LM_HEAD_W)
    val axiKvBusTag = List(ATTN_K_CACHE, ATTN_V_CACHE)

    val axiDenseCfgTag = List(ATTN_Q_OUT, ATTN_K_OUT, ATTN_V_OUT, ATTN_PROJ_OUT, ATTN_K_NO_ATTN, ATTN_V_NO_ATTN, MLP_PRED_U_OUT, LOGITS_PROJ_OUT)
    val axiSparseCfgTag = List(MLP_PRED_D_OUT, MLP_G_PROJ_OUT, MLP_U_PROJ_OUT, MLP_OUT)
    val axiLnCfgTag = List(ATTN_LN_SQR_OUT, LOGITS_LN_SQR_OUT)
    val axiKvCfgTag = (ATTN_QK_CACHE, ATTN_QKV_OUT)
    val axiMlpGTag = (MLP_W_G, MLP_G_PROJ_OUT)

    def axiSparseDotBusTagMap(busWidth: Int, numOfCore: Int, dim: Int) = {

      val quantLen = busWidth / 4
      val packWidth = 16 + 4
      val packCnt = busWidth / packWidth
      val dimPerCore = dim / numOfCore
      val sparseTagMap = List(
        //        (MLP_W_G, (dimPerCore / quantLen + packCnt - 1) / packCnt, dimPerCore / quantLen),
        (MLP_W_U, (dimPerCore / quantLen + packCnt - 1) / packCnt, dimPerCore / quantLen)
      )
      sparseTagMap
    }

    def axiSparseAxpyBusTagMap(busWidth: Int, numOfCore: Int, dim: Int, mlpDim: Int) = {

      val quantGroup = busWidth / 4
      val packWidth = 16 + 4
      val packCnt = busWidth / packWidth
      val dimPerCore = dim / numOfCore
      val mlpDimPerCore = mlpDim / numOfCore
      val sparseTagMap = List(
        //        (MLP_PRED_W_D, (mlpDimPerCore / quantGroup + packCnt - 1) / packCnt, mlpDimPerCore / quantGroup),
        (MLP_W_D, (dimPerCore / quantGroup + packCnt - 1) / packCnt, dimPerCore / quantGroup)
      )
      sparseTagMap
    }

  }

  object tagMap {

    import tensor._

    val ropeTagMap = List(
      (ATTN_Q_OUT, ATTN_Q_ROTATE),
      (ATTN_K_OUT, ATTN_K_ROTATE),
      (ATTN_K_NO_ATTN, ATTN_K_NO_ATTN_ROTATE)
    )

    val quantTagMap = List(
      (ATTN_K_ROTATE, ATTN_K_QUANT),
      (ATTN_K_NO_ATTN_ROTATE, ATTN_K_QUANT),
      (ATTN_V_OUT, ATTN_V_QUANT),
      (ATTN_V_NO_ATTN, ATTN_V_QUANT)
    )

    val softmaxTag = (ATTN_QK_LOCAL, ATTN_QK_CACHE, ATTN_SOFTMAX_OUT)

    val lnInTagMap = List(
      (PREFILL_TOKEN, ATTN_LN_OUT),
      (PREFILL_LAST_TOKEN, ATTN_LN_OUT),
      (DECODE_TOKEN, ATTN_LN_OUT),
      (MLP_OUT, ATTN_LN_OUT),
      (ATTN_PROJ_OUT, MLP_LN_OUT)
    )

    val zeroFilterTagMap = List(
      (MLP_PRED_U_OUT, MLP_PRED_U_FILTER),
      (MLP_PRED_D_OUT, MLP_PRED_D_FILTER)
    )
  }
}
