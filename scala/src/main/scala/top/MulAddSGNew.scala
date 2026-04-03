package top

import core.{Fp32AccEngine, VecNto1}
import spinal.core._
import spinal.lib._

import scala.language.postfixOps

class MulAddSGNew(
                   numOfCore: Int,
                   split: Int,
                   width: Int,
                   bankLen: Int,
                   dotMaxFirstDim: Int,
                   axpyMaxFirstDim: Int,
                   mul_latency: Int,
                   add_latency: Int,
                   acc_latency: Int,
                   add_func: (Flow[Bits], Flow[Bits]) => Flow[Bits],
                   acc_func: Flow[Fragment[Bits]] => Flow[Fragment[Bits]],
                   mul_func_nonblock: (Flow[Bits], Flow[Bits]) => Flow[Bits],
                   mul_func_block: (Stream[Bits], Stream[Bits]) => Stream[Bits],

                   toFp32_func: Flow[Bits] => Flow[Bits],
                   toFp32_func_block: Stream[Bits] => Stream[Bits],
                   toFp16_func: Flow[Bits] => Flow[Bits],
                   fp32Mul_func: (Flow[Bits], Flow[Bits]) => Flow[Bits],
                   fp32Add_func: (Flow[Bits], Flow[Bits]) => Flow[Bits],
                   fp32Acc_func: Flow[Fragment[Bits]] => Flow[Fragment[Bits]],
                   toFp32_latency: Int,
                   toFp16_latency: Int,
                   fp32Mul_latency: Int,
                   fp32Add_latency: Int,
                   fp32Acc_latency: Int
                 ) extends Component {

  require(isPow2(split))

  val serialBit = width
  val parallelBit = width * bankLen

  val io = new Bundle {
    val wkvIn = slave(Stream(Bits(parallelBit bits)))
    val dotIn = slave(Stream(Bits(parallelBit bits)))
    val resAdd = slave(Stream(Bits(parallelBit bits)))

    val axpyIn = slave(Stream(Bits(serialBit bits)))
    val preScale = slave(Stream(Bits(serialBit bits)))
    val postScale = slave(Stream(Bits(32 bits)))

    val vecOut = master(Flow(util.AxiFrame(Bits(parallelBit bits), userBit = 6)))
    val scalarOut = master(Flow(util.AxiFrame(Bits(serialBit bits), userBit = 6)))
    val cfg = slave(Stream(Bits(32 bits)))
    val preCfgTag = out Bits (6 bits)
    val postCfgTag = out Bits (6 bits)
  }

  noIoPrefix()
  util.AxiStreamSpecRenamer(io.wkvIn)
  util.AxiStreamSpecRenamer(io.dotIn)
  util.AxiStreamSpecRenamer(io.axpyIn)
  util.AxiStreamSpecRenamer(io.preScale)
  util.AxiStreamSpecRenamer(io.postScale)
  util.AxiStreamSpecRenamer(io.resAdd)
  util.AxiStreamSpecRenamer(io.vecOut)
  util.AxiStreamSpecRenamer(io.scalarOut)
  util.AxiStreamSpecRenamer(io.cfg)

  val banks = Array.fill(split)(new MulAddEngineNew(
    width, bankLen / split, dotMaxFirstDim, axpyMaxFirstDim,
    mul_latency, add_latency, acc_latency,
    add_func, acc_func, mul_func_nonblock, mul_func_block
  ))

  val wkvInSplit = io.wkvIn.payload.subdivideIn(split slices)
  val dotInSplit = io.dotIn.payload.subdivideIn(split slices)
  val resAddSplit = io.resAdd.payload.subdivideIn(split slices)

  io.preCfgTag := banks.head.io.preCfgTag
  //  io.postCfgTag := banks.head.io.postCfgTag

  //  val secondDim = Bits(16 bits)
  //  secondDim := banks.head.io.secondDim
  //  secondDim.addAttribute("mark_debug", "true")

  banks.zipWithIndex.foreach { case (bank, i) =>
    bank.io.wkvIn.payload := wkvInSplit(i)
    bank.io.dotIn.payload := dotInSplit(i)
    bank.io.resAdd.payload := resAddSplit(i)
  }

  banks.foreach { bank =>
    bank.io.wkvIn.valid := io.wkvIn.valid
    bank.io.dotIn.valid := io.dotIn.valid
    bank.io.axpyIn.valid := io.axpyIn.valid
    bank.io.preScale.valid := io.preScale.valid
    //    bank.io.postScale.valid := io.postScale.valid
    bank.io.resAdd.valid := io.resAdd.valid
    bank.io.cfg.valid := io.cfg.valid

    bank.io.axpyIn.payload := io.axpyIn.payload
    bank.io.preScale.payload := io.preScale.payload
    //    bank.io.postScale.payload := io.postScale.payload
    bank.io.cfg.payload := io.cfg.payload
  }

  io.wkvIn.ready := banks.head.io.wkvIn.ready
  io.dotIn.ready := banks.head.io.dotIn.ready
  io.axpyIn.ready := banks.head.io.axpyIn.ready
  io.preScale.ready := banks.head.io.preScale.ready
  //  io.postScale.ready := banks.head.io.postScale.ready
  io.resAdd.ready := banks.head.io.resAdd.ready
  io.cfg.ready := banks.head.io.cfg.ready

  val fp32Acc = new Fp32AccEngine(
    banks = split,
    toFp32_func = toFp32_func,
    toFp32_func_block = toFp32_func_block,
    toFp16_func = toFp16_func,
    fp32Mul_func = fp32Mul_func,
    fp32Add_func = fp32Add_func,
    fp32Acc_func = fp32Acc_func,
    toFp32_latency = toFp32_latency,
    toFp16_latency = toFp16_latency,
    fp32Mul_latency = fp32Mul_latency,
    fp32Add_latency = fp32Add_latency,
    fp32Acc_latency = fp32Acc_latency
  )

  (fp32Acc.io.inputs, banks).zipped.foreach(_ << _.io.scalarOut)
  io.postScale >> fp32Acc.io.postScale
  io.scalarOut << fp32Acc.io.output
  io.postCfgTag := Delay(banks.head.io.postCfgTag, fp32Add_latency * log2Up(split) + toFp32_latency)

  //  val scalarFlow = Vec(Flow(Bits(serialBit bits)), split)
  //  scalarFlow.zipWithIndex.foreach { case (flow, i) =>
  //    flow.valid := banks(i).io.scalarOut.valid
  //    flow.payload := banks(i).io.scalarOut.tdata
  //  }
  //
  //  val reduceLatency = add_latency * log2Up(split)
  //  val tuserDly = Delay(banks.head.io.scalarOut.tuser, reduceLatency + mul_latency + acc_latency, init = B"000000")
  //  val lastDly = Delay(banks.head.io.scalarOut.last, reduceLatency + mul_latency, init = False)
  //  io.postCfgTag := Delay(banks.head.io.postCfgTag, reduceLatency)
  //
  //  val reduce = new VecNto1(serialBit, split, add_func)
  //  (reduce.io.inputs, scalarFlow).zipped.foreach(_ << _)
  //
  //  val scaleFlow = Flow(Bits(width bits))
  //  val reduceFlow = Flow(Bits(width bits))
  //  val resFlow = mul_func_nonblock(scaleFlow, reduceFlow)
  //  reduceFlow.valid := reduce.io.output.valid
  //  reduceFlow.payload := reduce.io.output.payload
  //  scaleFlow.valid := reduceFlow.valid
  //  scaleFlow.payload := io.postScale.payload
  //  io.postScale.ready := reduceFlow.valid
  //
  //  val accIn = Flow(Fragment(Bits(width bits)))
  //  val accOut = acc_func(accIn)
  //  accIn.valid := resFlow.valid
  //  accIn.fragment := resFlow.payload
  //  accIn.last := lastDly
  //
  //  io.scalarOut.valid := accOut.valid & accOut.last
  //  io.scalarOut.tdata := accOut.fragment
  //  io.scalarOut.tuser := tuserDly

  val singleCore = (numOfCore == 1) generate new Area {
    val vld = banks.head.io.vecOut.valid
    val tdata = Vec(banks.map(_.io.vecOut.tdata)).asBits
    val tuser = banks.head.io.vecOut.tuser

    //    val vldDly = Delay(vld, 32, init = False)
    //    val tdataDly = Delay(tdata, 32)
    //    val tuserDly = Delay(tuser, 32)

    val vldDly = RegNext(vld, init = False)
    val tdataDly = RegNext(tdata)
    val tuserDly = RegNext(tuser)

    io.vecOut.valid := vldDly
    io.vecOut.tdata := tdataDly
    io.vecOut.tuser := tuserDly

    //    io.vecOut.valid := vld
    //    io.vecOut.tdata := tdata
    //    io.vecOut.tuser := tuser
  }

  val multiCore = (numOfCore > 1) generate new Area {
    io.vecOut.valid := RegNext(banks.head.io.vecOut.valid, init = False)
    io.vecOut.tdata := RegNext(Vec(banks.map(_.io.vecOut.tdata)).asBits)
    io.vecOut.tuser := RegNext(banks.head.io.vecOut.tuser)
  }

}
