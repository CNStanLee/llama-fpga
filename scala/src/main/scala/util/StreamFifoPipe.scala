package util

import spinal.core._
import spinal.lib._

import scala.language.postfixOps

class StreamFifoPipe[T <: Data](val dataType: HardType[T],
                            val depth: Int,
                            val withAsyncRead : Boolean = false,
                            val withBypass : Boolean = false,
                            val allowExtraMsb : Boolean = true,
                            val forFMax : Boolean = false,
                            val useVec : Boolean = false) extends Component {
  require(depth >= 0)

  if(withBypass) require(withAsyncRead)
  if(useVec) require (withAsyncRead)

  val io = new Bundle with StreamFifoInterface[T]{
    val push = slave Stream (dataType)
    val pop = master Stream (dataType)
    val flush = in Bool() default(False)
    val occupancy    = out UInt (log2Up(depth + 1) bits)
    val availability = out UInt (log2Up(depth + 1) bits)
    override def pushOccupancy = occupancy
    override def popOccupancy = occupancy
  }

  val popOut = Stream (dataType)
  io.pop << popOut.m2sPipe()
  
  class CounterUpDownFmax(states : BigInt, init : BigInt) extends Area{
    val incr, decr = Bool()
    val value = Reg(UInt(log2Up(states) bits)) init(init)
    val plusOne = KeepAttribute(value + 1)
    val minusOne = KeepAttribute(value - 1)
    when(incr =/= decr){
      value := incr.mux(plusOne, minusOne)
    }
    when(io.flush) { value := init }
  }

  val withExtraMsb = allowExtraMsb && isPow2(depth)
  val bypass = (depth == 0) generate new Area {
    io.push >> popOut
    io.occupancy := 0
    io.availability := 0
  }
  val oneStage = (depth == 1) generate new Area {
    val doFlush = CombInit(io.flush)
    val buffer = io.push.m2sPipe(flush = doFlush)
    popOut << buffer
    io.occupancy := U(buffer.valid)
    io.availability := U(!buffer.valid)

    if(withBypass){
      when(!buffer.valid){
        popOut.valid := io.push.valid
        popOut.payload := io.push.payload
        doFlush setWhen(popOut.ready)
      }
    }
  }
  val logic = (depth > 1) generate new Area {
    val vec = useVec generate Vec(Reg(dataType), depth)
    val ram = !useVec generate Mem(dataType, depth)

    val ptr = new Area{
      val doPush, doPop = Bool()
      val full, empty = Bool()
      val push = Reg(UInt(log2Up(depth) + withExtraMsb.toInt bits)) init(0)
      val pop  = Reg(UInt(log2Up(depth) + withExtraMsb.toInt bits)) init(0)
      val occupancy = cloneOf(io.occupancy)
      val popOnIo = cloneOf(pop) // Used to track the global occupancy of the fifo (the extra buffer of !withAsyncRead)
      val wentUp = RegNextWhen(doPush, doPush =/= doPop) init(False) clearWhen (io.flush)

      val arb = new Area {
        val area = !forFMax generate {
          withExtraMsb match {
            case true => { //as we have extra MSB, we don't need the "wentUp"
              full := (push ^ popOnIo ^ depth) === 0
              empty := push === pop
            }
            case false => {
              full := push === popOnIo && wentUp
              empty := push === pop && !wentUp
            }
          }
        }

        val fmax = forFMax generate new Area {
          val counterWidth = log2Up(depth) + 1
          val emptyTracker = new CounterUpDownFmax(1 << counterWidth, 1 << (counterWidth - 1)) {
            incr := doPop
            decr := doPush
            empty := value.msb
          }

          val fullTracker = new CounterUpDownFmax(1 << counterWidth, (1 << (counterWidth - 1)) - depth) {
            incr := io.push.fire
            decr := popOut.fire
            full := value.msb
          }
        }
      }


      when(doPush){
        push := push + 1
        if(!isPow2(depth)) when(push === depth - 1){ push := 0 }
      }
      when(doPop){
        pop := pop + 1
        if(!isPow2(depth)) when(pop === depth - 1){ pop := 0 }
      }

      when(io.flush){
        push := 0
        pop := 0
      }


      val forPow2 = (withExtraMsb && !forFMax) generate new Area{
        occupancy := push - popOnIo  //if no extra msb, could be U(full ## (push - popOnIo))
      }

      val notPow2 = (!withExtraMsb && !forFMax) generate new Area{
        val counter = Reg(UInt(log2Up(depth + 1) bits)) init(0)
        counter := counter + U(io.push.fire) - U(popOut.fire)
        occupancy := counter

        when(io.flush) { counter := 0 }
      }
      val fmax = forFMax generate new CounterUpDownFmax(depth + 1, 0){
        incr := io.push.fire
        decr := popOut.fire
        occupancy := value
      }
    }

    val push = new Area {
      io.push.ready := !ptr.full
      ptr.doPush := io.push.fire
      val onRam = !useVec generate new Area {
        val write = ram.writePort()
        write.valid := io.push.fire
        write.address := ptr.push.resized
        write.data := io.push.payload
      }
      val onVec = useVec generate new Area {
        when(io.push.fire){
          vec.write(ptr.push.resized, io.push.payload)
        }
      }
    }

    val pop = new Area{
      val addressGen = Stream(UInt(log2Up(depth) bits))
      addressGen.valid := !ptr.empty
      addressGen.payload := ptr.pop.resized
      ptr.doPop := addressGen.fire

      val sync = !withAsyncRead generate new Area{
        assert(!useVec)
        val readArbitation = addressGen.m2sPipe(flush = io.flush)
        val readPort = ram.readSyncPort
        readPort.cmd := addressGen.toFlowFire
        popOut << readArbitation.translateWith(readPort.rsp)

        val popReg = RegNextWhen(ptr.pop, readArbitation.fire) init(0)
        ptr.popOnIo := popReg
        when(io.flush){ popReg := 0 }
      }

      val async = withAsyncRead generate new Area{
        val readed = useVec match {
          case true => vec.read(addressGen.payload)
          case false => ram.readAsync(addressGen.payload)
        }
        popOut << addressGen.translateWith(readed)
        ptr.popOnIo := ptr.pop

        if(withBypass){
          when(ptr.empty){
            popOut.valid := io.push.valid
            popOut.payload := io.push.payload
            ptr.doPush clearWhen(popOut.ready)
          }
        }
      }
    }

    io.occupancy := ptr.occupancy
    if(!forFMax) io.availability := depth - ptr.occupancy
    val fmaxAvail = forFMax generate new CounterUpDownFmax(depth + 1, depth){
      incr := popOut.fire
      decr := io.push.fire
      io.availability := value
    }
  }
}