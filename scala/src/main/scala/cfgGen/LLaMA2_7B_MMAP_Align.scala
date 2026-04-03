package cfgGen

import scala.collection.mutable.ArrayBuffer

class LLaMA2_7B_MMAP_Align(
                            val dim: BigInt,
                            val mlpDim: BigInt,
                            val predDim: BigInt,
                            val layer: BigInt,
                            val head: BigInt,
                            val vocabSize: BigInt,
                            val numOfCore: BigInt,
                            val busWidth: BigInt,
                            val maxToken: BigInt,
                            val pageSize: Int
                          ) {

  def roundToPageSize(input: BigInt) = {
    val ret = (input + pageSize - 1) / pageSize * pageSize
    ret
  }

  val bankLen = busWidth / 4

  val vocabTable_addr = 0
  val vocabTable_len = vocabSize * dim * 2 / numOfCore

  val afterTokenizer_addr = vocabTable_addr + vocabTable_len

  // layer begin ---------------------
  val attnLnScale_addr = 0
  val attnLnScale_len = GenMemCmdLen.lnScaleFull(dim)

  // head begin ---------------------
  val attnQ_addr = 0
  val attnQ_len = GenMemCmdLen.attnDot(dim / head, dim, bankLen)

  val attnK_addr = attnQ_addr + roundToPageSize(attnQ_len)
  val attnK_len = GenMemCmdLen.attnDot(dim / head, dim, bankLen)

  val attnV_addr = attnK_addr + roundToPageSize(attnK_len)
  val attnV_len = GenMemCmdLen.attnDot(dim / head, dim, bankLen)

  val attnKCache_addr = attnV_addr + roundToPageSize(attnV_len)
  val attnKCache_len = dim / head * maxToken
  val attnVCache_addr = attnKCache_addr + roundToPageSize(attnKCache_len)
  val attnVCache_len = dim / head * maxToken

  val attnKScaleZero_addr = attnVCache_addr + roundToPageSize(attnVCache_len)
  val attnKScaleZero_len = 4 * maxToken
  val attnVScaleZero_addr = attnKScaleZero_addr + attnKScaleZero_len
  val attnVScaleZero_len = 4 * maxToken
  // head end ---------------------

  val attnQKV_addr = attnLnScale_addr + roundToPageSize(attnLnScale_len)
  val attnQKV_len = attnVScaleZero_addr + attnVScaleZero_len

  val attnO_addr = attnQKV_addr + attnQKV_len * head / numOfCore
  val attnO_len = GenMemCmdLen.splitDot(dim, dim, numOfCore, bankLen)

  val mlpLnScale_addr = attnO_addr + roundToPageSize(attnO_len)
  val mlpLnScale_len = GenMemCmdLen.lnScale(dim, numOfCore)


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
  val sparseLayer_len = mlpSparseD_addr + mlpSparseD_totalLen
  val lgSparseLnScale_addr = afterTokenizer_addr + sparseLayer_len * layer
  val lgSparseLnScale_len = GenMemCmdLen.lnScale(dim, numOfCore)
  val lgSparseHead_addr = lgSparseLnScale_addr + lgSparseLnScale_len
  val lgSparseHead_len = GenMemCmdLen.splitDot(vocabSize, dim, numOfCore, bankLen)


  val mlpDenseG_addr = mlpLnScale_addr + roundToPageSize(mlpLnScale_len)
  val mlpDenseG_len = GenMemCmdLen.splitDot(mlpDim, dim, numOfCore, bankLen)

  val mlpDenseU_addr = mlpDenseG_addr + roundToPageSize(mlpDenseG_len)
  val mlpDenseU_len = GenMemCmdLen.splitSparseDot(dim, numOfCore, bankLen, busWidth)
  val mlpDenseU_totalLen = mlpDenseU_len * mlpDim

  val mlpDenseD_addr = mlpDenseU_addr + roundToPageSize(mlpDenseU_totalLen)
  val mlpDenseD_len = GenMemCmdLen.splitSparseAxpy(dim, numOfCore, bankLen, busWidth)
  val mlpDenseD_totalLen = mlpDenseD_len * mlpDim

  val denseLayer_len = mlpDenseD_addr + roundToPageSize(mlpDenseD_totalLen)
  val lgDenseLnScale_addr = afterTokenizer_addr + denseLayer_len * layer
  val lgDenseLnScale_len = GenMemCmdLen.lnScale(dim, numOfCore)

  val lgDenseHead_addr = lgDenseLnScale_addr + roundToPageSize(lgDenseLnScale_len)
  val lgDenseHead_len = GenMemCmdLen.splitDot(vocabSize, dim, numOfCore, bankLen)
  val denseTotalMem = lgDenseHead_addr + roundToPageSize(lgDenseHead_len)
  val denseFirstHalf = afterTokenizer_addr + denseLayer_len * layer / 2
  val denseSecondHalf = denseLayer_len * layer / 2 + roundToPageSize(lgDenseLnScale_len) + roundToPageSize(lgDenseHead_len)
  val denseWhereToSplit = afterTokenizer_addr + denseLayer_len * layer / 2

  if (numOfCore == 1) {
    println(denseTotalMem, denseWhereToSplit)
    println(denseFirstHalf, denseSecondHalf, denseFirstHalf + denseSecondHalf)
  }

  println(denseTotalMem)

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

object MMAP_Test_Align extends App {

  import LLaMA2_7B._

  val numOfCore = 4
  val busWidth = 512

  val mmap = new LLaMA2_7B_MMAP_Align(
    dim = modelCfg.dim,
    mlpDim = modelCfg.mlpDim,
    predDim = modelCfg.predDim,
    layer = modelCfg.layer,
    head = modelCfg.head,
    vocabSize = modelCfg.vocabSize,
    numOfCore = numOfCore,
    busWidth = busWidth,
    maxToken = 1024,
    pageSize = 8192
  )

  for (i <- 0 until 32) {
    //    val ret = mmap.getVAddrSplitHalf((0x00, 0x36000), i, 0, 0)
    //    val ret = mmap.getKAddr(0, i, 0, 0)
    //    for (t <- 0 until 4) {
    //      val retOs = ret + mmap.attnKCache_len / 4 * t
    //      println(i, t, retOs, retOs.toInt.toHexString)
    //    }
    //    println(i, ret, ret.toInt.toHexString)
    //    print("0x")
    //    print(ret.toInt.toHexString)
    //    print(" ")
  }

  for (i <- 0 until 32) {
    val ret = mmap.getVAddr(0, i, 0, 0)
    val hexString = ret.toInt.toHexString
    val hexPad = if (hexString.length == 7) "0" + hexString else hexString
    print("0x4" + hexPad + " ")
  }

  // print hex


  println("afterTokenizer_len", mmap.vocabTable_len)
  println("attnQKV_len", mmap.attnQKV_len)
  println("attnQ_len", mmap.attnQ_len)
  println("attnK_len", mmap.attnK_len)
  println("attnV_len", mmap.attnV_len)
  println("attnKCache_len", mmap.attnKCache_len)
  println("attnKScaleZero_len", mmap.attnKScaleZero_len)
  println("attnVCache_len", mmap.attnVCache_len)
  println("attnVScaleZero_len", mmap.attnVScaleZero_len)
  println("attnO_len", mmap.attnO_len)
  println()
  println("mlpLnScale_len", mmap.mlpLnScale_len)
  println("mlpDenseG_len", mmap.mlpDenseG_len)
  println("mlpDenseU_len", mmap.mlpDenseU_len)
  println("mlpDenseD_len", mmap.mlpDenseD_len)
  println("denseLayer_len", mmap.denseLayer_len)
  println("lgDenseLnScale_len", mmap.lgDenseLnScale_len)
  println("lgDenseHead_len", mmap.lgDenseHead_len)
  println("denseTotalMem", mmap.denseTotalMem)
  println()
  println("denseWhereToSplit", mmap.denseWhereToSplit)
}
