package schedule

object LLMCfg {

  val core = 2
  val axiBus = 512

  val dim = 4096
  val head = 32
  val layer = 32
  val mlpDim = 11008
  val predDim = 1024
  val maxToken = 1024
  val group = 128

  object quant {
    val w = 4
    val kv = 8
    val s = 16
  }

  object tfBTTPerCore {

    val headPerCore = head / core

    val attnLn = dim / core * quant.s / 8

    val wAttnPerHead = dim * dim / head * quant.w / 8
    val sAttnPerHead = dim * dim / head / group * quant.s / 8
    val zAttnPerHead = dim * dim / head / group * quant.w / 8
    val bytePerHead = wAttnPerHead + sAttnPerHead + zAttnPerHead

    val kvPerToken = dim / head * quant.kv / 8
    val kvCachePerHead = kvPerToken * maxToken

    val numOfKvSzPerPack = axiBus / quant.kv
    val szKvCachePerPack = numOfKvSzPerPack * (quant.s + quant.kv) / 8
    val szKvCachePerHead = (quant.s + quant.kv) * maxToken / 8

    val wOutProj = dim * dim / core * quant.w / 8
    val sOutProj = dim * dim / core / group * quant.s / 8
    val zOutProj = dim * dim / core / group * quant.w / 8
    val byteOutProj = wOutProj + sOutProj + zOutProj

    val mlpLn = dim / core * quant.s / 8

    val wPredUp = dim * predDim / core * quant.w / 8
    val sPredUp = dim * predDim / core / group * quant.s / 8
    val zPredUp = dim * predDim / core / group * quant.w / 8
    val bytePredUp = wPredUp + sPredUp + zPredUp

    val wPredDownPerCol = mlpDim / core * quant.w / 8
    val szPredDownPerColBits = mlpDim / core / group * (quant.s + quant.w)
    val szPredDownPerColBeat = (szPredDownPerColBits + axiBus - 1) / axiBus
    val bytePredDownPerCol = wPredDownPerCol + szPredDownPerColBeat * axiBus / 8
    val bytePredDown = bytePredDownPerCol * predDim

    val wUGPerRow = 2 * dim / core * quant.w / 8
    val szUGPerRowBits = 2 * dim / core / group * (quant.s + quant.w)
    val szUGPerRowBeat = (szUGPerRowBits + axiBus - 1) / axiBus
    val byteUGProjPerRow = wUGPerRow + szUGPerRowBeat * axiBus / 8
    val byteUGProj = byteUGProjPerRow * mlpDim

    val wDPerCol = dim / core * quant.w / 8
    val szDPerColBits = dim / core / group * (quant.s + quant.w)
    val szDPerColBeat = (szDPerColBits + axiBus - 1) / axiBus
    val byteDProjPerCol = wDPerCol + szDPerColBeat * axiBus / 8
    val byteDProj = byteDProjPerCol * mlpDim

    val bytePerLayer =
      attnLn +
        bytePerHead * headPerCore * 3 +
        kvCachePerHead * headPerCore * 2 +
        szKvCachePerHead * headPerCore * 2 +
        byteOutProj +
        mlpLn +
        bytePredUp +
        bytePredDown +
        byteUGProj +
        byteDProj
  }

  object tfOsPerCore {

    import tfBTTPerCore._

    val attnLnStartAt = 0

    val wqStartAt = attnLnStartAt + attnLn
    val wkStartAt = wqStartAt + bytePerHead * headPerCore
    val wvStartAt = wkStartAt + bytePerHead * headPerCore

    val kCacheSzStartAt = wvStartAt + bytePerHead * headPerCore
    val kCacheStartAt = kCacheSzStartAt + szKvCachePerHead * headPerCore

    val vCacheSzStartAt = kCacheStartAt + kvCachePerHead * headPerCore
    val vCacheStartAt = vCacheSzStartAt + szKvCachePerHead * headPerCore

    val outProjStartAt = vCacheStartAt + kvCachePerHead * headPerCore

    val mlpLnStartAt = outProjStartAt + byteOutProj

    val predUpStartAt = mlpLnStartAt + mlpLn
    val predDownStartAt = predUpStartAt + bytePredUp
    val ugProjStartAt = predDownStartAt + bytePredDown
    val dProjStartAt = ugProjStartAt + byteUGProj

    val nextLayerStartAt = dProjStartAt + byteDProj
  }

  object state {
    val num = 14

    val attnLn = 0
    val wq = 1
    val wk = 2
    val kCacheSz = 3
    val kCache = 4
    val wv = 5
    val vCacheSz = 6
    val vCache = 7
    val wo = 8
    val mlpLn = 9
    val wPredU = 10
    val wPredD = 11
    val wug = 12
    val wd = 13
  }
}
