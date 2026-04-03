package c2c

import adapter.{FlowGate, TagMap}
import spinal.core._
import spinal.lib._

import scala.language.postfixOps

class ReduceFilter(
                    id: Int,
                    dim: Int,
                    width: Int,
                    numOfCore: Int,
                    tag: Int
                  ) extends Component {

  val idWidth = log2Up(numOfCore)

  val partialDim = dim / numOfCore

  val io = new Bundle {
    val input = slave(Flow(util.AxiFrame(Bits(width bits), userBit = 6)))
    val output = master(Flow(util.AxiFrame(Bits(width bits), userBit = 6)))
  }

  val input = FlowGate(io.input, List(tag))

  val singleCore = numOfCore == 1 generate new Area {
    io.output.valid := input.valid
    io.output.tdata := input.payload
    io.output.tuser := tag
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

    io.output.valid := input.valid & idCnt === id
    io.output.tdata := input.payload
    io.output.tuser := tag
  }
}

object ReduceFilter extends App {
  SpinalVerilog(new ReduceFilter(0, 4096, 4, 4,1))
}