package top

import c2c.ReduceFilter
import norm.{RMSLayerNorm, SquareSum}
import spinal.core._
import spinal.lib._

import scala.language.postfixOps

class NormSubMod(
                  id: Int,
                  width: Int,
                  dim: Int,
                  numOfCore: Int,

                  filterTag: Int,
                  lnInTagMap: List[(Int, Int)],
                  lnSqrTagMap: List[(Int, Int)],
                  sqrInTagMap: List[(Int, Int)],
                  logitsLnTag: Int,

                  mulLatency: Int,
                  mul_func: (Flow[Bits], Flow[Bits]) => Flow[Bits],
                  mul_func_block: (Stream[Bits], Stream[Bits]) => Stream[Bits],
                  acc_func: Flow[Fragment[Bits]] => Flow[Fragment[Bits]],
                  rsqrt_func: Flow[Bits] => Flow[Bits]
                ) extends Component {

  val io = new Bundle {
    val p2sOut = slave(Flow(util.AxiFrame(Bits(width bits), userBit = 6)))
    val allReduceOut = slave(Flow(util.AxiFrame(Bits(width bits), userBit = 6)))
    val lnScale = slave(Stream(Bits(width bits)))
    val lnOut = master(Flow(Fragment(util.AxiFrame(Bits(width bits), userBit = 6))))
  }

  val status = new Bundle {
    val logitsGen = in Bool()
  }

  noIoPrefix()
  util.AxiStreamSpecRenamer(io.lnOut)

  val allReduceOut = Flow(util.AxiFrame(Bits(width bits), userBit = 6))
  allReduceOut.valid := io.allReduceOut.valid
  allReduceOut.tdata := util.Fp16ScaleDown(io.allReduceOut.tdata, 2)
  allReduceOut.tuser := io.allReduceOut.tuser

  val p2sOut = Flow(util.AxiFrame(Bits(width bits), userBit = 6))
  val p2sScale = UInt(5 bits)
  p2sScale := U(2)
  when(p2sOut.tuser === 0 || p2sOut.tuser === 1 || p2sOut.tuser === 2)(p2sScale.clearAll())
  p2sOut.valid := io.p2sOut.valid
  p2sOut.tdata := util.Fp16ScaleDown(io.p2sOut.tdata, p2sScale)
  p2sOut.tuser := io.p2sOut.tuser

  val sqrSum = new SquareSum(
    width = width,
    dim = dim,
    numOfPort = 1,
    tagMap = sqrInTagMap,
    mul_func = mul_func,
    acc_func = acc_func
  )

  val filter = new ReduceFilter(
    id = id,
    dim = dim,
    numOfCore = numOfCore,
    width = width,
    tag = filterTag
  )

  val ln = new RMSLayerNorm(
    width = width,
    dim = dim,
    numOfCore = numOfCore,
    numOfInPort = 2,
    numOfSqrPort = 2,
    inputTagMap = lnInTagMap,
    sqrSumTagMap = lnSqrTagMap,
    mulLatency = mulLatency,
    mul_func = mul_func,
    mul_func_block = mul_func_block,
    rsqrt_func = rsqrt_func
  )

  ln.io.input(0) << p2sOut
  ln.io.input(1) << filter.io.output

  ln.io.squareSum(0) << sqrSum.io.sqrSum
  ln.io.squareSum(1) << io.allReduceOut

  ln.io.scale << io.lnScale

  sqrSum.io.input(0) << allReduceOut
  filter.io.input << allReduceOut

  val lnOutPipe = ln.io.output.m2sPipe
  io.lnOut.valid := lnOutPipe.valid
  io.lnOut.tdata := lnOutPipe.tdata
  io.lnOut.tuser := Mux(status.logitsGen, B(logitsLnTag), lnOutPipe.tuser)
  io.lnOut.last := lnOutPipe.last
}
