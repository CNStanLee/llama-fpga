package cfgGen

import breeze.linalg.NumericOps
import spinal.core._
import spinal.lib._

import scala.language.postfixOps

object GenMemCmdLen {

  val maxLenBits = 23

  def tokenIn(dim: BigInt, numOfCore: BigInt) = {
    val ret = dim / numOfCore * 16 / 8
    ret
  }

  def lnScale(dim: BigInt, numOfCore: BigInt) = {
    val ret = dim / numOfCore * 16 / 8
    ret
  }

  def lnScaleFull(dim:BigInt) = {
    val ret = dim * 16 / 8
    ret
  }

  def attnDot(headDim: BigInt, inDim: BigInt, bankLen: BigInt) = {
    val weightBytes = headDim * inDim * 4 / 8
    val scaleBytes = headDim * inDim / bankLen * 16 / 8
    val zeroBytes = headDim * inDim / bankLen * 4 / 8
    val ret = weightBytes + scaleBytes + zeroBytes
    ret
  }

  def splitDot(outDim: BigInt, inDim: BigInt, numOfCore: BigInt, bankLen: BigInt) = {
    val weightBytes = outDim * inDim / numOfCore * 4 / 8
    val scaleBytes = outDim * inDim / numOfCore / bankLen * 16 / 8
    val zeroBytes = outDim * inDim / numOfCore / bankLen * 4 / 8
    val ret = weightBytes + scaleBytes + zeroBytes
    ret
  }

  def splitSparseDot(inDim: BigInt, numOfCore: BigInt, bankLen: BigInt, busWidth: BigInt) = {
    val splitInDim = inDim / numOfCore
    val numOfPackPerRow = splitInDim / bankLen
    val packPerBeat = busWidth / 20
    val numOfBeat = (numOfPackPerRow + packPerBeat - 1) / packPerBeat
    val weightBytes = splitInDim * 4 / 8
    val packBytes = numOfBeat * busWidth / 8
    val ret = weightBytes + packBytes
    ret
  }

  def splitSparseAxpy(outDim: BigInt, numOfCore: BigInt, bankLen: BigInt, busWidth: BigInt) = {
    val splitOutDim = outDim / numOfCore
    val numOfPackPerCol = splitOutDim / bankLen
    val packPerBeat = busWidth / 20
    val numOfBeat = (numOfPackPerCol + packPerBeat - 1) / packPerBeat
    val weightBytes = splitOutDim * 4 / 8
    val packBytes = numOfBeat * busWidth / 8
    val ret = weightBytes + packBytes
    ret
  }

  def kvCache(headDim: BigInt, token: UInt) = {
    val bytesPerHead = headDim * 8 / 8
    val bytesToTransfer = token ## B(0, log2Up(bytesPerHead) bits)
    val ret = bytesToTransfer.resize(maxLenBits bits)
    B(ret, maxLenBits bits)
  }

  def kvScaleZero(token: UInt, busWidth: BigInt) = {
    val packPerBeat = busWidth / 32
    val tokenHigh = token.drop(log2Up(packPerBeat))
    val bytesToTransfer = tokenHigh ## B(0, log2Up(busWidth / 8) bits)
    val ret = bytesToTransfer.resize(maxLenBits bits)
    B(ret, maxLenBits bits)
  }
}
