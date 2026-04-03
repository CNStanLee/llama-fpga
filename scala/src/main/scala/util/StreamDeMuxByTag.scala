package util

import spinal.core._
import spinal.lib._

import scala.language.postfixOps

class StreamDeMuxByTag[T <: Data](dataType: HardType[T], numOfPorts: Int, userBit:Int) extends Component {

  val io = new Bundle{
    val input = slave(Stream(AxiFrame(dataType(), userBit)))
    val outputs = Vec(master(Stream(dataType())), numOfPorts)
  }

  val deMux = new StreamDemux(dataType(), numOfPorts)
  deMux.io.input.arbitrationFrom(io.input)
  deMux.io.input.payload := io.input.tdata
  deMux.io.select := io.input.tuser.asUInt.resized
  for(i <- 0 until numOfPorts){
    io.outputs(i) << deMux.io.outputs(i)
  }
}
