package c2c

import adapter.FlowGate
import spinal.core._
import spinal.lib._

import scala.language.postfixOps

class AllReduce(
                 numOfCore: Int,
                 dataWidth: Int,
                 listOfTag: List[Int],
                 acc_func: Flow[Fragment[Bits]] => Flow[Fragment[Bits]]
               ) extends Component {

  require(isPow2(numOfCore))

  val idWidth = log2Up(numOfCore)

  val io = new Bundle {
    val input = slave(Flow(util.AxiFrame(Bits(dataWidth bits), userBit = 6, destBit = idWidth)))
    val output = master(Flow(util.AxiFrame(Bits(dataWidth bits), userBit = 6)))
  }
  noIoPrefix()
  util.AxiStreamSpecRenamer(io.output)

  val input = Flow(util.AxiFrame(Bits(dataWidth bits), userBit = 6))
  val cond = listOfTag.map(t => io.input.tuser === B(t)).reduce(_ || _)
  input.valid := io.input.valid & cond
  input.tdata := io.input.tdata
  input.tuser := io.input.tuser

  val singleCore = numOfCore == 1 generate new Area {
    io.output.valid := input.valid
    io.output.tdata := io.input.tdata
    io.output.tuser := io.input.tuser
  }

  val multiCore = numOfCore > 1 generate new Area {

    val tuser = Stream(Bits(6 bits))
    val tuserQueue = tuser.queue(32, forFMax = true)

    val idCnt = UInt(idWidth bits).setAsReg().init(0)
    val idOvf = idCnt === numOfCore - 1
    when(input.valid) {
      idCnt := idCnt + 1
    }

    tuser.valid := input.valid & idOvf
    tuser.payload := io.input.tuser
    tuserQueue.ready := io.output.valid

    val accIn = Flow(Fragment(Bits(dataWidth bits)))
    accIn.valid := input.valid
    accIn.fragment := input.tdata
    accIn.last := idOvf

    val accOut = acc_func(accIn)
    io.output.valid := accOut.valid & accOut.last
    io.output.tdata := accOut.payload
    io.output.tuser := tuserQueue.payload
  }
}

object AllReduce extends App {
  SpinalVerilog(new AllReduce(4, 16, List(1, 2), util.fp16acc16.acc))
}