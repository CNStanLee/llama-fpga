package attn

import adapter.FlowGate
import spinal.core._
import spinal.lib._

import scala.language.postfixOps

class QKMul(
             width: Int,
             dim: Int,
             qkTag: (Int, Int, Int),
             mul_func: (Flow[Bits], Flow[Bits]) => Flow[Bits],
             acc_func: Flow[Fragment[Bits]] => Flow[Fragment[Bits]],
             sqrtHeadDim: Int
             //             div_func: (Flow[Bits], Flow[Bits]) => Flow[Bits]
           ) extends Component {

  val io = new Bundle {
    val input = slave(Flow(util.AxiFrame(Bits(width bits), userBit = 6)))
    val output = master(Flow(util.AxiFrame(Bits(width bits), userBit = 6)))
  }

  val qIn = FlowGate(io.input, List(qkTag._1))

  val inCnt = UInt(log2Up(128) bits).setAsReg().init(0)
  val headCnt = UInt(5 bits).setAsReg().init(0)
  when(qIn.valid) {
    inCnt := inCnt + 1
    when(inCnt === 127) {
      inCnt.clearAll()
      headCnt := headCnt + 1
    }
  }
  val qProbe = master(Flow(Bits(width bits)))
  qProbe.valid := qIn.valid & inCnt === 0 & headCnt === 0
  qProbe.payload := qIn.payload

  util.AxiStreamSpecRenamer(qIn)

  val kInput = FlowGate(io.input, List(qkTag._2))

  val sqrtD = Flow(Bits(width bits))
  sqrtD.payload := sqrtHeadDim
  sqrtD.valid.set()

  //  val qInput = div_func(qIn, sqrtD)
  val qInput = mul_func(qIn, sqrtD)
  //  val qInput = qIn

  //  val ram = Mem(Bits(width bits), dim)
  //  val wrAddr = UInt(log2Up(dim) bits).setAsReg().init(0)
  //  val wrAddrOvf = wrAddr === dim - 1
  //  ram.write(address = wrAddr, data = qInput.payload, enable = qInput.valid)
  //  when(qInput.valid) {
  //    wrAddr := wrAddr + 1
  //    when(wrAddrOvf) {
  //      wrAddr.clearAll()
  //    }
  //  }
  //
  //  val rdEvent = Event
  //  val rdAddr = UInt(log2Up(dim) bits).setAsReg().init(0)
  //  val rdAddrOvf = rdAddr === dim - 1
  //  val rdOut = Stream(Bits(width bits))
  //  rdEvent.valid.setAsReg().init(False)
  //  rdEvent.valid.setWhen(qInput.valid & wrAddrOvf)
  //  rdEvent.valid.clearWhen(rdEvent.fire & rdAddrOvf)
  //  rdOut.arbitrationFrom(rdEvent.m2sPipe())
  //  rdOut.payload := ram.readSync(rdAddr, rdEvent.ready)
  //  when(rdEvent.fire) {
  //    rdAddr := rdAddr + 1
  //    when(rdAddrOvf) {
  //      rdAddr.clearAll()
  //    }
  //  }

  val fifo = new StreamFifo(Bits(width bits), dim, forFMax = true)
  fifo.io.push.valid := qInput.valid
  fifo.io.push.payload := qInput.payload

  val qFlow = Flow(Bits(width bits))
  val kFlow = Flow(Bits(width bits))
  val qk = mul_func(qFlow, kFlow)
  qFlow.valid := kFlow.valid
  fifo.io.pop.ready := kFlow.valid
  qFlow.payload := fifo.io.pop.payload
  kFlow.payload := kInput.payload
  kFlow.valid := kInput.valid

  val accIn = Flow(Fragment(Bits(width bits)))
  val accInCnt = UInt(log2Up(dim) bits).setAsReg().init(0)
  val accInCntOvf = accInCnt === dim - 1
  accIn.valid := qk.valid
  accIn.fragment := qk.payload
  accIn.last := accInCntOvf
  when(qk.valid) {
    accInCnt := accInCnt + 1
    when(accInCntOvf) {
      accInCnt.clearAll()
    }
  }

  val accOut = acc_func(accIn)
  io.output.valid := accOut.valid & accOut.last
  io.output.tdata := accOut.fragment
  io.output.tuser := qkTag._3
}
