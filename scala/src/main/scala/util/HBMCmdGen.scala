package util

import spinal.core._
import spinal.lib._

import scala.language.postfixOps

class HBMCmdGen(
                 HBMAddrWidth: Int = 33,
                 HBMDataWidth: Int = 256
               ) extends Component {

  val maxLen = 16
  val io = new Bundle {
    val localCmd = slave(Stream(Bits(72 bits)))
    val hbmCmd = master(Stream(util.Linked(UInt(HBMAddrWidth bits), UInt(log2Up(maxLen) bits))))
  }

  val bytesPerCycle = HBMDataWidth / 8
  val bytesPerBurst = bytesPerCycle * maxLen

  val baseAddr = io.localCmd.payload.dropLow(32).takeLow(32).asUInt
  val bytes2Transfer = io.localCmd.payload.takeLow(23)
  val numOfCycles = bytes2Transfer.dropLow(log2Up(bytesPerCycle))
  val numOfHbmCmd = numOfCycles.dropLow(4).asUInt
  val cyclesLeft = numOfCycles.takeLow(4).asUInt

  val cmdCnt = UInt(numOfHbmCmd.getWidth bits).setAsReg().init(0)
  val cmdCntOvf = cmdCnt === Mux(cyclesLeft === 0, numOfHbmCmd - 1, numOfHbmCmd)
  when(io.hbmCmd.fire) {
    cmdCnt := cmdCnt + 1
    when(cmdCntOvf) {
      cmdCnt.clearAll()
    }
  }

  val offsetAddr = (cmdCnt ## B(0, log2Up(bytesPerBurst) bits)).asUInt
  val hbmAddr = baseAddr + offsetAddr

  io.hbmCmd.valid := io.localCmd.valid
  io.hbmCmd.A := hbmAddr.resize(HBMAddrWidth)
  io.hbmCmd.B := Mux(cmdCntOvf, cyclesLeft - 1, U(maxLen - 1))
  io.localCmd.ready := io.hbmCmd.ready & cmdCntOvf
}

object HBMCmdGen extends App {
  SpinalVerilog(new HBMCmdGen())
}
