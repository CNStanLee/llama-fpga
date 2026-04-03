package rope

import spinal.core._
import spinal.lib._

import scala.language.postfixOps

class RoPERotate[T <: Data](dataType: HardType[T], maxDim: Int) extends Component {

  val io = new Bundle {
    val input = slave(Flow(dataType()))
    val output = master(Flow(util.Linked(dataType(), dataType())))
  }

  val cfg = new Bundle {
    val dim = in UInt (log2Up(maxDim) bits)
  }

  val halfDim = cfg.dim.drop(1).asUInt
  val inCnt = UInt(log2Up(maxDim) - 1 bits).setAsReg().init(0)
  val inCntOvf = inCnt === halfDim
  val inputSecondHalf = Bool().setAsReg().init(False)
  when(io.input.valid) {
    inCnt := inCnt + 1
    when(inCntOvf) {
      inCnt.clearAll()
      inputSecondHalf := ~inputSecondHalf
    }
  }

  val rotateFifo = new StreamFifo(dataType(), maxDim / 2)
  val rotateFifoPop = rotateFifo.io.pop.m2sPipe()
  rotateFifo.io.push.valid := io.input.valid & ~inputSecondHalf
  rotateFifo.io.push.payload := io.input.payload

  val bypassFifo = new StreamFifo(dataType(), maxDim / 2)
  val bypassFifoPop = bypassFifo.io.pop.m2sPipe()
  bypassFifo.io.push.valid := io.input.valid
  bypassFifo.io.push.payload := io.input.payload

  val rotateOut = Flow(dataType())
  val rotateOutCnt = UInt(log2Up(maxDim) - 1 bits).setAsReg().init(0)
  val rotateOutCntOvf = rotateOutCnt === halfDim
  val rotateSecondHalf = Bool().setAsReg().init(False)
  when(rotateOut.valid) {
    rotateOutCnt := rotateOutCnt + 1
    when(rotateOutCntOvf) {
      rotateOutCnt.clearAll()
      rotateSecondHalf := ~rotateSecondHalf
    }
  }

  val in2Rotate = Flow(dataType())
  val r = io.input.payload.asBits
  in2Rotate.valid := io.input.valid & inputSecondHalf
  in2Rotate.payload.assignFromBits(~r.msb ## r.dropHigh(1))

  rotateOut := Mux(rotateSecondHalf, rotateFifoPop.toFlow, in2Rotate)
  rotateFifoPop.ready.removeAssignments()
  rotateFifoPop.ready := rotateSecondHalf

  io.output.valid := rotateOut.valid
  io.output.A := bypassFifoPop.payload
  io.output.B := rotateOut.payload
  bypassFifoPop.ready := rotateOut.valid
}

object RoPERotate extends App {
  SpinalVerilog(new RoPERotate(Bits(16 bits), 128))
}