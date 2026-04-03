package residual

import adapter.FlowGate
import spinal.core._
import spinal.lib._

import scala.language.postfixOps

class SerialResAdd(
                    id: Int,
                    dim: Int,
                    numOfCore: Int,
                    width: Int,
                    addLatency: Int,
                    resAddTag: Int,
                    add_func: (Flow[Bits], Flow[Bits]) => Flow[Bits]
                  ) extends Component {

  val idWidth = log2Up(numOfCore)

  val partialDim = dim / numOfCore

  val io = new Bundle {
    val dotOut = slave(Flow(util.AxiFrame(Bits(width bits), userBit = 6)))
    val output = master(Flow(Fragment(util.AxiFrame(Bits(width bits), userBit = 6))))
    val fromAllReduce = if (numOfCore != 1) slave(Flow(util.AxiFrame(Bits(width bits), userBit = 6))) else null

    val fromResBuf = slave(Stream(Bits(width bits)))
    val toResBuf = master(Flow(Bits(width bits)))
  }

  val fromResBuf = io.fromResBuf.queue(32, latency = 2, forFMax = true)
  fromResBuf.valid.addAttribute("max_fanout", 100)

  val input = FlowGate(io.dotOut, List(resAddTag))
  val tuser = Delay(io.dotOut.tuser, addLatency)

  val singleCore = numOfCore == 1 generate new Area {
    val tobeAdded = Flow(Bits(width bits))
    val result = add_func(input, tobeAdded)

    tobeAdded.valid := input.valid
    tobeAdded.payload := fromResBuf.payload
    fromResBuf.ready := input.valid
    io.output.valid := result.valid
    io.output.tdata := result.payload
    io.output.tuser := tuser
    io.output.last.set()

    io.toResBuf.valid := result.valid
    io.toResBuf.payload := result.payload
  }

  val multiCore = numOfCore > 1 generate new Area {
    val idCnt = UInt(idWidth bits).setAsReg().init(0)
    val bankCnt = UInt(16 bits).setAsReg().init(0)
    val idOvf = idCnt === numOfCore - 1
    val bankOvf = bankCnt === partialDim - 1
    when(input.valid) {
      bankCnt := bankCnt + 1
      when(bankOvf) {
        bankCnt.clearAll()
        idCnt := idCnt + 1
        when(idOvf) {
          idCnt.clearAll()
        }
      }
    }

    //    val last = input.valid & bankOvf & idOvf

    val tobeAdded = Flow(Bits(width bits))
    val result = add_func(input, tobeAdded)
    io.output.valid := result.valid
    io.output.tdata := result.payload
    io.output.tuser := tuser
    io.output.last.set()
    //    io.output.last := Delay(last, addLatency, init = False)

    val flag = idCnt === id
    tobeAdded.valid := input.valid
    tobeAdded.payload := Mux(flag, fromResBuf.payload, B(0))
    fromResBuf.ready := input.valid & flag
  }

  val multiCore2Res = numOfCore > 1 generate new Area {

    val allReduce = FlowGate(io.fromAllReduce, List(resAddTag))

    val idCnt = UInt(idWidth bits).setAsReg().init(0)
    val bankCnt = UInt(16 bits).setAsReg().init(0)
    val idOvf = idCnt === numOfCore - 1
    val bankOvf = bankCnt === partialDim - 1
    when(allReduce.valid) {
      bankCnt := bankCnt + 1
      when(bankOvf) {
        bankCnt.clearAll()
        idCnt := idCnt + 1
        when(idOvf) {
          idCnt.clearAll()
        }
      }
    }

    val flag = idCnt === id
    val toResBuf = Flow(Bits(width bits))
    toResBuf.valid.setAsReg().init(False)
    toResBuf.valid.addAttribute("max_fanout", 100)
    toResBuf.valid := allReduce.valid & flag
    toResBuf.payload := RegNext(allReduce.payload)

    io.toResBuf.valid := toResBuf.valid
    io.toResBuf.payload := toResBuf.payload

    val resAddProbe = master(Flow(Bits(16 bits)))
    resAddProbe.valid := allReduce.valid & bankCnt === 0 & flag
    resAddProbe.payload := allReduce.payload
  }
}

object SerialResAdd extends App {
  SpinalVerilog(
    new SerialResAdd(
      0,
      4096,
      4,
      16,
      1,
      0,
      util.fp16mul6.mul
    )
  )
}
