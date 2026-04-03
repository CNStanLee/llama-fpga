package norm

import adapter.{FlowGate, TagMap}
import spinal.core._
import spinal.lib._
import util.Fp16ScaleDown

import scala.language.postfixOps

class SquareSum(
                 width: Int,
                 dim: Int,
                 numOfPort:Int,
                 tagMap: List[(Int,Int)],
                 mul_func: (Flow[Bits], Flow[Bits]) => Flow[Bits],
                 acc_func: Flow[Fragment[Bits]] => Flow[Fragment[Bits]]
               ) extends Component {

  val io = new Bundle {
    val input = Vec(slave(Flow(util.AxiFrame(Bits(width bits), userBit = 6))), numOfPort)
    val sqrSum = master(Flow(util.AxiFrame(Bits(width bits), userBit = 6)))
  }

  noIoPrefix()
  util.AxiStreamSpecRenamer(io.sqrSum)

  val (inTgM,inErr) = TagMap(io.input, tagMap)
  val input = inTgM.m2sPipe()

  val inCnt = UInt(log2Up(dim) bits).setAsReg().init(0)
  val inCntOvf = inCnt === dim - 1
  val inCntZero = inCnt === 0
  val usrStream = Stream(Bits(6 bits))
  val usrStreamPipe = usrStream.stage()
  usrStream.valid := inCntZero & input.valid
  usrStream.payload := input.tuser
  when(input.valid) {
    inCnt := inCnt + 1
    when(inCntOvf) {
      inCnt.clearAll()
    }
  }

  val shiftedNum = log2Up(dim) / 2
  val inFlow = input.translateWith(Fp16ScaleDown(input.tdata, shiftedNum))
  val sqr = mul_func(inFlow, inFlow)
  val sqrCnt = UInt(16 bits).setAsReg().init(0)
  val sqrCntOvf = sqrCnt === dim - 1
  when(sqr.valid) {
    sqrCnt := sqrCnt + 1
    when(sqrCntOvf) {
      sqrCnt.clearAll()
    }
  }

  val accIn = Flow(Fragment(Bits(width bits)))
  accIn.valid := sqr.valid
  accIn.last := sqrCntOvf
  accIn.fragment := sqr.payload
  val accOut = acc_func(accIn)

  io.sqrSum.valid := accOut.valid & accOut.last
  io.sqrSum.tdata := accOut.payload
  io.sqrSum.tuser := usrStreamPipe.payload

  usrStreamPipe.ready := accOut.valid & accOut.last
}

object SquareSum extends App {
  SpinalVerilog(new SquareSum(16, 4096,2, List((1, 2)), util.fp16mul6.mul, util.fp16acc16.acc))
}