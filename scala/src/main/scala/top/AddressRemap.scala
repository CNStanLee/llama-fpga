package top

import spinal.core._
import spinal.lib._

import scala.language.postfixOps

class AddressRemap(firstBankBase: BigInt, secondBankBase: BigInt, inAddrWidth: Int, outAddrWidth: Int, splitFrom: BigInt) extends Component {

  val io = new Bundle {
    val input = slave(Stream(Bits(inAddrWidth + 40 bits)))
    val output = master(Stream(Bits(outAddrWidth + 40 bits)))
  }

  val low32 = io.input.payload.takeLow(32)
  val high8 = io.input.payload.takeHigh(8)
  val addr32 = io.input.payload.drop(32).takeLow(inAddrWidth).asUInt

  val newFirstAddr40 = addr32.resize(outAddrWidth) + firstBankBase
  val newSecondAddr40 = addr32.resize(outAddrWidth) - splitFrom + secondBankBase
  val isSecondBank = addr32 >= splitFrom
  val newAddr40 = Mux(isSecondBank, newSecondAddr40, newFirstAddr40)

  val newCmd = high8 ## newAddr40 ## low32
  io.output << io.input.translateWith(newCmd).m2sPipe()
}
