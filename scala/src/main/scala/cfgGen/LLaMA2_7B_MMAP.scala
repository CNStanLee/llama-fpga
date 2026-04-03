package cfgGen

class LLaMA2_7B_MMAP(
                      val dim: BigInt,
                      val mlpDim: BigInt,
                      val predDim: BigInt,
                      val layer: BigInt,
                      val head: BigInt,
                      val vocabSize: BigInt,
                      val numOfCore: BigInt,
                      val busWidth: BigInt,
                      val maxToken: BigInt
                    ) {

  val bankLen = busWidth / 4

  val vocabTable_addr = 0
  val vocabTable_len = vocabSize * dim * 2 / numOfCore

  val afterTokenizer_addr = vocabTable_addr + vocabTable_len

  // layer begin ---------------------
  val attnLnScale_addr = 0
  val attnLnScale_len = GenMemCmdLen.lnScale(dim, numOfCore)

  // head begin ---------------------
  val attnQ_addr = 0
  val attnQ_len = GenMemCmdLen.attnDot(dim / head, dim, bankLen)

  val attnK_addr = attnQ_addr + attnQ_len
  val attnK_len = GenMemCmdLen.attnDot(dim / head, dim, bankLen)

  val attnV_addr = attnK_addr + attnK_len
  val attnV_len = GenMemCmdLen.attnDot(dim / head, dim, bankLen)

  val attnKCache_addr = attnV_addr + attnV_len
  val attnKCache_len = dim / head * maxToken
  val attnKScaleZero_addr = attnKCache_addr + attnKCache_len
  val attnKScaleZero_len = 4 * maxToken

  val attnVCache_addr = attnKScaleZero_addr + attnKScaleZero_len
  val attnVCache_len = dim / head * maxToken
  val attnVScaleZero_addr = attnVCache_addr + attnVCache_len
  val attnVScaleZero_len = 4 * maxToken

  // head end ---------------------

  val attnQKV_addr = attnLnScale_addr + attnLnScale_len
  val attnQKV_len = attnVScaleZero_addr + attnVScaleZero_len

  val attnO_addr = attnQKV_addr + attnQKV_len * head / numOfCore
  val attnO_len = GenMemCmdLen.splitDot(dim, dim, numOfCore, bankLen)

  val mlpLnScale_addr = attnO_addr + attnO_len
  val mlpLnScale_len = GenMemCmdLen.lnScale(dim, numOfCore)

  // sparse
  val mlpPredU_addr = mlpLnScale_addr + mlpLnScale_len
  val mlpPredU_len = GenMemCmdLen.splitDot(predDim, dim, numOfCore, bankLen)

  val mlpPredD_addr = mlpPredU_addr + mlpPredU_len
  val mlpPredD_len = GenMemCmdLen.splitSparseAxpy(mlpDim, numOfCore, bankLen, busWidth)
  val mlpPredD_totalLen = mlpPredD_len * predDim

  val mlpSparseG_addr = mlpPredD_addr + mlpPredD_totalLen
  val mlpSparseG_len = GenMemCmdLen.splitSparseDot(dim, numOfCore, bankLen, busWidth)
  val mlpSparseG_totalLen = mlpSparseG_len * mlpDim

  val mlpSparseU_addr = mlpSparseG_addr + mlpSparseG_totalLen
  val mlpSparseU_len = GenMemCmdLen.splitSparseDot(dim, numOfCore, bankLen, busWidth)
  val mlpSparseU_totalLen = mlpSparseU_len * mlpDim

  val mlpSparseD_addr = mlpSparseU_addr + mlpSparseU_totalLen
  val mlpSparseD_len = GenMemCmdLen.splitSparseAxpy(dim, numOfCore, bankLen, busWidth)
  val mlpSparseD_totalLen = mlpSparseD_len * mlpDim
  // sparse layer end ---------------------

  val mlpSparseTotal_len = mlpPredU_len + mlpPredD_totalLen + mlpSparseG_totalLen + mlpSparseU_totalLen + mlpSparseD_totalLen

  val sparseLayer_len = mlpSparseD_addr + mlpSparseD_totalLen
  val lgSparseLnScale_addr = afterTokenizer_addr + sparseLayer_len * layer
  val lgSparseLnScale_len = GenMemCmdLen.lnScale(dim, numOfCore)

  val lgSparseHead_addr = lgSparseLnScale_addr + lgSparseLnScale_len
  val lgSparseHead_len = GenMemCmdLen.splitDot(vocabSize, dim, numOfCore, bankLen)
  val sparseTotalMem = lgSparseHead_addr + lgSparseHead_len
  val sparseWhereToSplit = afterTokenizer_addr + sparseLayer_len * layer / 2

  // dense
  val mlpDenseG_addr = mlpLnScale_addr + mlpLnScale_len
  val mlpDenseG_len = GenMemCmdLen.splitDot(mlpDim, dim, numOfCore, bankLen)

  val mlpDenseU_addr = mlpDenseG_addr + mlpDenseG_len
  val mlpDenseU_len = GenMemCmdLen.splitSparseDot(dim, numOfCore, bankLen, busWidth)
  val mlpDenseU_totalLen = mlpDenseU_len * mlpDim

  val mlpDenseD_addr = mlpDenseU_addr + mlpDenseU_totalLen
  val mlpDenseD_len = GenMemCmdLen.splitSparseAxpy(dim, numOfCore, bankLen, busWidth)
  val mlpDenseD_totalLen = mlpDenseD_len * mlpDim

  println("mlpDenseU_len", mlpDenseU_len)
  println("mlpDenseD_len", mlpDenseD_len)
  // dense layer end ---------------------

  val mlpDenseTotal_len = mlpDenseG_len + mlpDenseU_totalLen + mlpDenseD_totalLen

  val denseLayer_len = mlpDenseD_addr + mlpDenseD_totalLen
  val lgDenseLnScale_addr = afterTokenizer_addr + denseLayer_len * layer
  val lgDenseLnScale_len = GenMemCmdLen.lnScale(dim, numOfCore)

  val lgDenseHead_addr = lgDenseLnScale_addr + lgDenseLnScale_len
  val lgDenseHead_len = GenMemCmdLen.splitDot(vocabSize, dim, numOfCore, bankLen)
  val denseTotalMem = lgDenseHead_addr + lgDenseHead_len
  val denseWhereToSplit = afterTokenizer_addr + denseLayer_len * layer / 2
  println(denseTotalMem, denseWhereToSplit)

  val kvCacheSize = (attnKCache_len + attnVCache_len + attnKScaleZero_len + attnVScaleZero_len) * head
  val denseWeightSize = attnLnScale_len + (attnQ_len + attnK_len + attnV_len) * head + attnO_len + mlpLnScale_len + mlpDenseG_len + mlpDenseU_totalLen + mlpDenseD_totalLen
  val lgSize = lgSparseLnScale_len + lgDenseHead_len
  val singleDecodeSize = dim * 2 + denseWeightSize * layer + lgSize
  val addUp = vocabTable_len + lgSize + denseWeightSize * layer + kvCacheSize * layer

  def getKAddr(base: Int, l: Int, h: Int, t: Int) = {
    val hMod = h % (head / numOfCore)
    val tokenOs = t * dim / head
    val headOs = hMod * attnQKV_len
    val layerOs = l * denseLayer_len
    val addr = base + afterTokenizer_addr + (layerOs + attnQKV_addr) + (headOs + attnKCache_addr) + tokenOs
    addr
  }

  def getKAddrSplitHalf(base: (Int, Int), l: Int, h: Int, t: Int) = {
    val hMod = h % (head / numOfCore)
    val tokenOs = t * dim / head
    val headOs = hMod * attnQKV_len
    val layerOs = l % (layer / 2) * denseLayer_len
    val baseAddr = if (l >= layer / 2) BigInt(base._2) else (BigInt(base._1) + afterTokenizer_addr)
    val addr = baseAddr + (layerOs + attnQKV_addr) + (headOs + attnKCache_addr) + tokenOs
    addr
  }

  def getVAddrSplitHalf(base: (Int, Int), l: Int, h: Int, t: Int) = {
    val hMod = h % (head / numOfCore)
    val tokenOs = t * dim / head
    val headOs = hMod * attnQKV_len
    val layerOs = l % (layer / 2) * denseLayer_len
    val baseAddr = if (l >= layer / 2) BigInt(base._2) else (BigInt(base._1) + afterTokenizer_addr)
    val addr = baseAddr + (layerOs + attnQKV_addr) + (headOs + attnVCache_addr) + tokenOs
    addr
  }

  def getVAddr(base: Int, l: Int, h: Int, t: Int) = {
    val hMod = h % (head / numOfCore)
    val tokenOs = t * dim / head
    val headOs = hMod * attnQKV_len
    val layerOs = l * denseLayer_len
    val addr = base + afterTokenizer_addr + (layerOs + attnQKV_addr) + (headOs + attnVCache_addr) + tokenOs
    addr
  }

  def getKSzAddr(base: Int, l: Int, h: Int, t: Int, busWidth: Int) = {
    val packCnt = busWidth / 32
    val hMod = h % (head / numOfCore)
    val tokenOs = t / packCnt * dim / head
    val headOs = hMod * attnQKV_len
    val layerOs = l * denseLayer_len
    val addr = base + afterTokenizer_addr + (layerOs + attnQKV_addr) + (headOs + attnKScaleZero_addr) + tokenOs
    addr
  }
}

object MMAP_Test extends App {

  import LLaMA2_7B._

  val numOfCore = 1
  val busWidth = 512

  val mmap = new LLaMA2_7B_MMAP(
    dim = modelCfg.dim,
    mlpDim = modelCfg.mlpDim,
    predDim = modelCfg.predDim,
    layer = modelCfg.layer,
    head = modelCfg.head,
    vocabSize = modelCfg.vocabSize,
    numOfCore = numOfCore,
    busWidth = busWidth,
    maxToken = 1024
  )

  for (i <- 0 until 32) {
    val ret = mmap.getKAddrSplitHalf((0x00, 0x28000), i, 0, 0)
    //    val ret = mmap.getKAddr(0x28000, i, 0, 0)
    //    for (t <- 0 until 4) {
    //      val retOs = ret + mmap.attnKCache_len / 4 * t
    //      println(i, t, retOs, retOs.toInt.toHexString)
    //    }
    println(i, ret, ret.toInt.toHexString)
  }

  //  for (i <- 0 until 32) {
  //    val ret = mmap.getKAddr(0, i, 16, 0)
  //    println(i, ret, ret.toInt.toHexString)
  //  }

  // print hex


  //  println("afterTokenizer_addr", mmap.afterTokenizer_addr)
  //  println("attnQKV_addr", mmap.attnQKV_addr)
  //  println("attnQ_addr", mmap.attnQ_addr)
  //  println("attnK_addr", mmap.attnK_addr)
  //  println("attnV_addr", mmap.attnV_addr)
  //  println("attnKCache_addr", mmap.attnKCache_addr)
  //  println("attnKScaleZero_addr", mmap.attnKScaleZero_addr)
  //  println("attnVCache_addr", mmap.attnVCache_addr)
  //  println("attnVScaleZero_addr", mmap.attnVScaleZero_addr)
  //  println("attnO_addr", mmap.attnO_addr)
  //  println()
  //  println("mlpLnScale_addr", mmap.mlpLnScale_addr)
  //  println("mlpDenseG_addr", mmap.mlpDenseG_addr)
  //  println("mlpDenseU_addr", mmap.mlpDenseU_addr)
  //  println("mlpDenseD_addr", mmap.mlpDenseD_addr)
  //  println("denseLayer_len", mmap.denseLayer_len)
  //  println("lgDenseLnScale_addr", mmap.lgDenseLnScale_addr)
  //  println("lgDenseHead_addr", mmap.lgDenseHead_addr)
  //  println("denseTotalMem", mmap.denseTotalMem)
  //  println()
  //  println("mlpPredU_addr", mmap.mlpPredU_addr)
  //  println("mlpPredD_addr", mmap.mlpPredD_addr)
  //  println("mlpSparseG_addr", mmap.mlpSparseG_addr)
  //  println("mlpSparseU_addr", mmap.mlpSparseU_addr)
  //  println("mlpSparseD_addr", mmap.mlpSparseD_addr)
  //  println("sparseLayer_len", mmap.sparseLayer_len)
  //  println("lgSparseLnScale_addr", mmap.lgSparseLnScale_addr)
  //  println("lgSparseHead_addr", mmap.lgSparseHead_addr)
  //  println("sparseTotalMem", mmap.sparseTotalMem)
  //  println()
  //  println(mmap.attnQKV_len)
}
