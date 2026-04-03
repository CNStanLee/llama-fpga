package util

import spinal.core._
import spinal.lib._

import scala.collection.mutable.ArrayBuffer
import scala.language.postfixOps

object fp16add8 {

  val ipName = this.getClass.getSimpleName.stripSuffix("$")
  val latency = ipName.stripPrefix("fp16").filter(_.isDigit).toInt

  class ipFlowIO extends XilinxFloatIPFlowIO(
    ipName = ipName, latency = latency,
    numOfOperand = 2, inputWidth = 16, outputWidth = 16, isAcc = false
  )

  class ipFlowIOSim extends XilinxFloatIPFlowIOSim(
    ipName = ipName, latency = latency,
    numOfOperand = 2, inputWidth = 16, outputWidth = 16, isAcc = false
  )

  def add_sim(a: Flow[Bits], b: Flow[Bits]) = new Composite(a, "add") {
    val ip = new ipFlowIOSim()
    ip.io.a << a
    ip.io.b << b
  }.ip.io.r

  def add(a: Flow[Bits], b: Flow[Bits]) = new Composite(a, "add") {
    val ip = new ipFlowIO()
    ip.io.a << a
    ip.io.b << b
  }.ip.io.r

  def add(a: Bits, b: Bits, vld: Bool) = new Composite(a, "add") {
    val ip = new ipFlowIO()
    ip.io.a.valid := vld
    ip.io.a.payload := a
    ip.io.b.valid := vld
    ip.io.b.payload := b
  }.ip.io.r
}

object fp16add6 {

  val ipName = this.getClass.getSimpleName.stripSuffix("$")
  val latency = ipName.stripPrefix("fp16").filter(_.isDigit).toInt

  class ipFlowIO extends XilinxFloatIPFlowIO(
    ipName = ipName, latency = latency,
    numOfOperand = 2, inputWidth = 16, outputWidth = 16, isAcc = false
  )

  class ipFlowIOSim extends XilinxFloatIPFlowIOSim(
    ipName = ipName, latency = latency,
    numOfOperand = 2, inputWidth = 16, outputWidth = 16, isAcc = false
  )

  def add_sim(a: Flow[Bits], b: Flow[Bits]) = new Composite(a, "add") {
    val ip = new ipFlowIOSim()
    ip.io.a << a
    ip.io.b << b
  }.ip.io.r

  def add(a: Flow[Bits], b: Flow[Bits]) = new Composite(a, "add") {
    val ip = new ipFlowIO()
    ip.io.a << a
    ip.io.b << b
  }.ip.io.r

  def add(a: Bits, b: Bits, vld: Bool) = new Composite(a, "add") {
    val ip = new ipFlowIO()
    ip.io.a.valid := vld
    ip.io.a.payload := a
    ip.io.b.valid := vld
    ip.io.b.payload := b
  }.ip.io.r
}

object fp16mul6 {

  val ipName = this.getClass.getSimpleName.stripSuffix("$")
  val latency = ipName.stripPrefix("fp16").filter(_.isDigit).toInt

  class ipFlowIO extends XilinxFloatIPFlowIO(
    ipName = ipName, latency = latency,
    numOfOperand = 2, inputWidth = 16, outputWidth = 16, isAcc = false
  )

  class ipFlowIOSim extends XilinxFloatIPFlowIOSim(
    ipName = ipName, latency = latency,
    numOfOperand = 2, inputWidth = 16, outputWidth = 16, isAcc = false
  )

  def mul_sim(a: Flow[Bits], b: Flow[Bits]) = new Composite(a, "mul") {
    val ip = new ipFlowIOSim()
    ip.io.a << a
    ip.io.b << b
  }.ip.io.r

  def mul(a: Flow[Bits], b: Flow[Bits]) = new Composite(a, "mul") {
    val ip = new ipFlowIO()
    ip.io.a << a
    ip.io.b << b
  }.ip.io.r

  def mul(a: Bits, b: Bits, vld: Bool) = new Composite(a, "mul") {
    val ip = new ipFlowIO()
    ip.io.a.valid := vld
    ip.io.a.payload := a
    ip.io.b.valid := vld
    ip.io.b.payload := b
  }.ip.io.r
}

object fp16sub8 {

  val ipName = this.getClass.getSimpleName.stripSuffix("$")
  val latency = ipName.stripPrefix("fp16").filter(_.isDigit).toInt

  class ipFlowIO extends XilinxFloatIPFlowIO(
    ipName = ipName, latency = latency,
    numOfOperand = 2, inputWidth = 16, outputWidth = 16, isAcc = false
  )

  class ipFlowIOSim extends XilinxFloatIPFlowIOSim(
    ipName = ipName, latency = latency,
    numOfOperand = 2, inputWidth = 16, outputWidth = 16, isAcc = false
  )

  def sub_sim(a: Flow[Bits], b: Flow[Bits]) = new Composite(a, "sub") {
    val ip = new ipFlowIOSim()
    ip.io.a << a
    ip.io.b << b
  }.ip.io.r

  def sub(a: Flow[Bits], b: Flow[Bits]) = new Composite(a, "sub") {
    val ip = new ipFlowIO()
    ip.io.a << a
    ip.io.b << b
  }.ip.io.r

  def sub(a: Bits, b: Bits, vld: Bool) = new Composite(a, "sub") {
    val ip = new ipFlowIO()
    ip.io.a.valid := vld
    ip.io.a.payload := a
    ip.io.b.valid := vld
    ip.io.b.payload := b
  }.ip.io.r
}

object fp16div12 {

  val ipName = this.getClass.getSimpleName.stripSuffix("$")
  val latency = ipName.stripPrefix("fp16").filter(_.isDigit).toInt

  class ipFlowIO extends XilinxFloatIPFlowIO(
    ipName = ipName, latency = latency,
    numOfOperand = 2, inputWidth = 16, outputWidth = 16, isAcc = false
  )

  class ipFlowIOSim extends XilinxFloatIPFlowIOSim(
    ipName = ipName, latency = latency,
    numOfOperand = 2, inputWidth = 16, outputWidth = 16, isAcc = false
  )

  def div_sim(a: Flow[Bits], b: Flow[Bits]) = new Composite(a, "div") {
    val ip = new ipFlowIOSim()
    ip.io.a << a
    ip.io.b << b
  }.ip.io.r

  def div(a: Flow[Bits], b: Flow[Bits]) = new Composite(a, "div") {
    val ip = new ipFlowIO()
    ip.io.a << a
    ip.io.b << b
  }.ip.io.r

  def div(a: Bits, b: Bits, vld: Bool) = new Composite(a, "div") {
    val ip = new ipFlowIO()
    ip.io.a.valid := vld
    ip.io.a.payload := a
    ip.io.b.valid := vld
    ip.io.b.payload := b
  }.ip.io.r
}

object fp16rev8 {

  val ipName = this.getClass.getSimpleName.stripSuffix("$")
  val latency = ipName.stripPrefix("fp16").filter(_.isDigit).toInt

  class ipFlowIO extends XilinxFloatIPFlowIO(
    ipName = ipName, latency = latency,
    numOfOperand = 1, inputWidth = 16, outputWidth = 16, isAcc = false
  )

  class ipFlowIOSim extends XilinxFloatIPFlowIOSim(
    ipName = ipName, latency = latency,
    numOfOperand = 1, inputWidth = 16, outputWidth = 16, isAcc = false
  )

  def rev_sim(a: Flow[Bits]) = new Composite(a, "rev") {
    val ip = new ipFlowIOSim()
    ip.io.a << a
  }.ip.io.r

  def rev(a: Flow[Bits]) = new Composite(a, "rev") {
    val ip = new ipFlowIO()
    ip.io.a << a
  }.ip.io.r

  def rev(a: Bits, vld: Bool) = new Composite(a, "rev") {
    val ip = new ipFlowIO()
    ip.io.a.valid := vld
    ip.io.a.payload := a
  }.ip.io.r
}

object fp16ex12 {

  val ipName = this.getClass.getSimpleName.stripSuffix("$")
  val latency = ipName.stripPrefix("fp16").filter(_.isDigit).toInt

  class ipFlowIO extends XilinxFloatIPFlowIO(
    ipName = ipName, latency = latency,
    numOfOperand = 1, inputWidth = 16, outputWidth = 16, isAcc = false
  )

  class ipFlowIOSim extends XilinxFloatIPFlowIOSim(
    ipName = ipName, latency = latency,
    numOfOperand = 1, inputWidth = 16, outputWidth = 16, isAcc = false
  )

  def exp_sim(a: Flow[Bits]) = new Composite(a, "exp") {
    val ip = new ipFlowIOSim()
    ip.io.a << a
  }.ip.io.r

  def exp(a: Flow[Bits]) = new Composite(a, "exp") {
    val ip = new ipFlowIO()
    ip.io.a << a
  }.ip.io.r

  def exp(a: Bits, vld: Bool) = new Composite(a, "exp") {
    val ip = new ipFlowIO()
    ip.io.a.valid := vld
    ip.io.a.payload := a
  }.ip.io.r
}

object fp32exp20 {

  val ipName = this.getClass.getSimpleName.stripSuffix("$")
  val latency = ipName.stripPrefix("fp32").filter(_.isDigit).toInt

  class ipFlowIO extends XilinxFloatIPFlowIO(
    ipName = ipName, latency = latency,
    numOfOperand = 1, inputWidth = 32, outputWidth = 32, isAcc = false
  )

  class ipFlowIOSim extends XilinxFloatIPFlowIOSim(
    ipName = ipName, latency = latency,
    numOfOperand = 1, inputWidth = 32, outputWidth = 32, isAcc = false
  )

  def exp_sim(a: Flow[Bits]) = new Composite(a, "exp") {
    val ip = new ipFlowIOSim()
    ip.io.a << a
  }.ip.io.r

  def exp(a: Flow[Bits]) = new Composite(a, "exp") {
    val ip = new ipFlowIO()
    ip.io.a << a
  }.ip.io.r

  def exp(a: Bits, vld: Bool) = new Composite(a, "exp") {
    val ip = new ipFlowIO()
    ip.io.a.valid := vld
    ip.io.a.payload := a
  }.ip.io.r
}

object fp16lt0 {

  val ipName = this.getClass.getSimpleName.stripSuffix("$")
  val latency = ipName.stripPrefix("fp16").filter(_.isDigit).toInt

  class ipFlowIO extends XilinxFloatIPFlowIO(
    ipName = ipName, latency = latency,
    numOfOperand = 2, inputWidth = 16, outputWidth = 8, isAcc = false
  )

  def lt_async_sim(a: Flow[Bits], b: Flow[Bits]) = new Composite(a, "lt_async") {
    val ret = Flow(Bool())
    ret.valid := a.valid & b.valid
    ret.payload := False
  }.ret

  def lt_async(a: Flow[Bits], b: Flow[Bits]) = new Composite(a, "lt_async") {
    val ip = new ipFlowIO()
    ip.io.a << a
    ip.io.b << b
    val ret = Flow(Bool())
    ret.valid := ip.io.r.valid
    ret.payload := ip.io.r.payload(0)
  }.ret

  def lt_async(a: Bits, b: Bits, vld: Bool) = new Composite(a, "lt_async") {
    val ip = new ipFlowIO()
    ip.io.a.valid := vld
    ip.io.a.payload := a
    ip.io.b.valid := vld
    ip.io.b.payload := b
    val ret = Flow(Bool())
    ret.valid := ip.io.r.valid
    ret.payload := ip.io.r.payload(0)
  }.ret

  def lt_async(a: Bits, b: Bits) = new Composite(a, "lt_async") {
    val ip = new ipFlowIO()
    ip.io.a.valid.set()
    ip.io.a.payload := a
    ip.io.b.valid.set()
    ip.io.b.payload := b
    val ret = Flow(Bool())
    ret.valid := ip.io.r.valid
    ret.payload := ip.io.r.payload(0)
  }.ret.payload
}

object fp32lt0 {

  val ipName = this.getClass.getSimpleName.stripSuffix("$")
  val latency = ipName.stripPrefix("fp32").filter(_.isDigit).toInt

  class ipFlowIO extends XilinxFloatIPFlowIO(
    ipName = ipName, latency = latency,
    numOfOperand = 2, inputWidth = 32, outputWidth = 8, isAcc = false
  )

  def lt_async(a: Flow[Bits], b: Flow[Bits]) = new Composite(a, "lt_async") {
    val ip = new ipFlowIO()
    ip.io.a << a
    ip.io.b << b
    val ret = Flow(Bool())
    ret.valid := ip.io.r.valid
    ret.payload := ip.io.r.payload(0)
  }.ret

  def lt_async(a: Bits, b: Bits, vld: Bool) = new Composite(a, "lt_async") {
    val ip = new ipFlowIO()
    ip.io.a.valid := vld
    ip.io.a.payload := a
    ip.io.b.valid := vld
    ip.io.b.payload := b
    val ret = Flow(Bool())
    ret.valid := ip.io.r.valid
    ret.payload := ip.io.r.payload(0)
  }.ret

  def lt_async(a: Bits, b: Bits) = new Composite(a, "lt_async") {
    val ip = new ipFlowIO()
    ip.io.a.valid.set()
    ip.io.a.payload := a
    ip.io.b.valid.set()
    ip.io.b.payload := b
    val ret = Flow(Bool())
    ret.valid := ip.io.r.valid
    ret.payload := ip.io.r.payload(0)
  }.ret.payload
}

object fp16acc16 {

  val ipName = this.getClass.getSimpleName.stripSuffix("$")
  val latency = ipName.stripPrefix("fp16").filter(_.isDigit).toInt

  class ipFlowIO extends XilinxFloatIPFlowIO(
    ipName = ipName, latency = latency,
    numOfOperand = 1, inputWidth = 16, outputWidth = 16, isAcc = true
  )

  class ipFlowIOSim extends XilinxFloatIPFlowIOSim(
    ipName = ipName, latency = latency,
    numOfOperand = 1, inputWidth = 16, outputWidth = 16, isAcc = true
  )

  def acc_sim(a: Flow[Fragment[Bits]]) = new Composite(a, "acc") {
    val ip = new ipFlowIOSim()
    ip.io.accIn << a
  }.ip.io.accOut

  def acc(a: Flow[Fragment[Bits]]) = new Composite(a, "acc") {
    val ip = new ipFlowIO()
    ip.io.accIn << a
  }.ip.io.accOut
}

object fp32acc22 {

  val ipName = this.getClass.getSimpleName.stripSuffix("$")
  val latency = ipName.stripPrefix("fp32").filter(_.isDigit).toInt

  class ipFlowIO extends XilinxFloatIPFlowIO(
    ipName = ipName, latency = latency,
    numOfOperand = 1, inputWidth = 32, outputWidth = 32, isAcc = true
  )

  class ipFlowIOSim extends XilinxFloatIPFlowIOSim(
    ipName = ipName, latency = latency,
    numOfOperand = 1, inputWidth = 32, outputWidth = 32, isAcc = true
  )

  def acc_sim(a: Flow[Fragment[Bits]]) = new Composite(a, "acc") {
    val ip = new ipFlowIOSim()
    ip.io.accIn << a
  }.ip.io.accOut

  def acc(a: Flow[Fragment[Bits]]) = new Composite(a, "acc") {
    val ip = new ipFlowIO()
    ip.io.accIn << a
  }.ip.io.accOut
}

object fp16rsqrt4 {

  val ipName = this.getClass.getSimpleName.stripSuffix("$")
  val latency = ipName.stripPrefix("fp16").filter(_.isDigit).toInt

  class ipFlowIO extends XilinxFloatIPFlowIO(
    ipName = ipName, latency = latency,
    numOfOperand = 1, inputWidth = 16, outputWidth = 16, isAcc = false
  )

  class ipFlowIOSim extends XilinxFloatIPFlowIOSim(
    ipName = ipName, latency = latency,
    numOfOperand = 1, inputWidth = 16, outputWidth = 16, isAcc = false
  )

  def rsqrt_sim(a: Flow[Bits]) = new Composite(a, "rsqrt") {
    val ip = new ipFlowIOSim()
    ip.io.a << a
  }.ip.io.r

  def rsqrt(a: Flow[Bits]) = new Composite(a, "rsqrt") {
    val ip = new ipFlowIO()
    ip.io.a << a
  }.ip.io.r

  def rsqrt(a: Bits, vld: Bool) = new Composite(a, "rsqrt") {
    val ip = new ipFlowIO()
    ip.io.a.valid := vld
    ip.io.a.payload := a
  }.ip.io.r
}

object fp32rsqrt32 {

  val ipName = this.getClass.getSimpleName.stripSuffix("$")
  val latency = ipName.stripPrefix("fp32").filter(_.isDigit).toInt

  class ipFlowIO extends XilinxFloatIPFlowIO(
    ipName = ipName, latency = latency,
    numOfOperand = 1, inputWidth = 32, outputWidth = 32, isAcc = false
  )

  class ipFlowIOSim extends XilinxFloatIPFlowIOSim(
    ipName = ipName, latency = latency,
    numOfOperand = 1, inputWidth = 32, outputWidth = 32, isAcc = false
  )

  def rsqrt_sim(a: Flow[Bits]) = new Composite(a, "rsqrt") {
    val ip = new ipFlowIOSim()
    ip.io.a << a
  }.ip.io.r

  def rsqrt(a: Flow[Bits]) = new Composite(a, "rsqrt") {
    val ip = new ipFlowIO()
    ip.io.a << a
  }.ip.io.r

  def rsqrt(a: Bits, vld: Bool) = new Composite(a, "rsqrt") {
    val ip = new ipFlowIO()
    ip.io.a.valid := vld
    ip.io.a.payload := a
  }.ip.io.r
}

object fp16int5d4 {

  val ipName = this.getClass.getSimpleName.stripSuffix("$")
  val latency = 4

  class ipFlowIO extends XilinxFloatIPFlowIO(
    ipName = ipName, latency = latency,
    numOfOperand = 1, inputWidth = 8, outputWidth = 16, isAcc = false
  )

  class ipFlowIOSim extends XilinxFloatIPFlowIOSim(
    ipName = ipName, latency = latency,
    numOfOperand = 1, inputWidth = 8, outputWidth = 16, isAcc = false
  )

  def from_sim(a: Flow[Bits]) = new Composite(a, "from") {
    val ip = new ipFlowIOSim()
    ip.io.a << a
  }.ip.io.r

  def from(a: Flow[Bits]) = new Composite(a, "from") {
    val ip = new ipFlowIO()
    ip.io.a << a
  }.ip.io.r

  def from(a: Bits, vld: Bool) = new Composite(a, "from") {
    val ip = new ipFlowIO()
    ip.io.a.valid := vld
    ip.io.a.payload := a
  }.ip.io.r
}

object fp16int9d4 {

  val ipName = this.getClass.getSimpleName.stripSuffix("$")
  val latency = 4

  class ipFlowIO extends XilinxFloatIPFlowIO(
    ipName = ipName, latency = latency,
    numOfOperand = 1, inputWidth = 16, outputWidth = 16, isAcc = false
  )

  class ipFlowIOSim extends XilinxFloatIPFlowIOSim(
    ipName = ipName, latency = latency,
    numOfOperand = 1, inputWidth = 16, outputWidth = 16, isAcc = false
  )

  def from_sim(a: Flow[Bits]) = new Composite(a, "from") {
    val ip = new ipFlowIOSim()
    ip.io.a << a
  }.ip.io.r

  def from(a: Flow[Bits]) = new Composite(a, "from") {
    val ip = new ipFlowIO()
    ip.io.a << a
  }.ip.io.r

  def from(a: Bits, vld: Bool) = new Composite(a, "from") {
    val ip = new ipFlowIO()
    ip.io.a.valid := vld
    ip.io.a.payload := a
  }.ip.io.r
}

object fp16toint9d4 {

  val ipName = this.getClass.getSimpleName.stripSuffix("$")
  val latency = 4

  class ipFlowIO extends XilinxFloatIPFlowIO(
    ipName = ipName, latency = latency,
    numOfOperand = 1, inputWidth = 16, outputWidth = 16, isAcc = false
  )

  class ipFlowIOSim extends XilinxFloatIPFlowIOSim(
    ipName = ipName, latency = latency,
    numOfOperand = 1, inputWidth = 16, outputWidth = 16, isAcc = false
  )

  def to_sim(a: Flow[Bits]) = new Composite(a, "to") {
    val ip = new ipFlowIOSim()
    ip.io.a << a
  }.ip.io.r

  def to(a: Flow[Bits]) = new Composite(a, "to") {
    val ip = new ipFlowIO()
    ip.io.a << a
  }.ip.io.r

  def to(a: Bits, vld: Bool) = new Composite(a, "to") {
    val ip = new ipFlowIO()
    ip.io.a.valid := vld
    ip.io.a.payload := a
  }.ip.io.r
}

object fp32mul8 {

  val ipName = this.getClass.getSimpleName.stripSuffix("$")
  val latency = 8

  class ipFlowIO extends XilinxFloatIPFlowIO(
    ipName = ipName, latency = latency,
    numOfOperand = 2, inputWidth = 32, outputWidth = 32, isAcc = false
  )

  class ipFlowIOSim extends XilinxFloatIPFlowIOSim(
    ipName = ipName, latency = latency,
    numOfOperand = 2, inputWidth = 32, outputWidth = 32, isAcc = false
  )

  def mul_sim(a: Flow[Bits], b: Flow[Bits]) = new Composite(a, "mul") {
    val ip = new ipFlowIOSim()
    ip.io.a << a
    ip.io.b << b
  }.ip.io.r

  def mul(a: Flow[Bits], b: Flow[Bits]) = new Composite(a, "mul") {
    val ip = new ipFlowIO()
    ip.io.a << a
    ip.io.b << b
  }.ip.io.r

  def mul(a: Bits, b: Bits, vld: Bool) = new Composite(a, "mul") {
    val ip = new ipFlowIO()
    ip.io.a.valid := vld
    ip.io.a.payload := a
    ip.io.b.valid := vld
    ip.io.b.payload := b
  }.ip.io.r

}

object fp32div28 {

  val ipName = this.getClass.getSimpleName.stripSuffix("$")
  val latency = 28

  class ipFlowIO extends XilinxFloatIPFlowIO(
    ipName = ipName, latency = latency,
    numOfOperand = 2, inputWidth = 32, outputWidth = 32, isAcc = false
  )

  class ipFlowIOSim extends XilinxFloatIPFlowIOSim(
    ipName = ipName, latency = latency,
    numOfOperand = 2, inputWidth = 32, outputWidth = 32, isAcc = false
  )

  def div_sim(a: Flow[Bits], b: Flow[Bits]) = new Composite(a, "div") {
    val ip = new ipFlowIOSim()
    ip.io.a << a
    ip.io.b << b
  }.ip.io.r

  def div(a: Flow[Bits], b: Flow[Bits]) = new Composite(a, "div") {
    val ip = new ipFlowIO()
    ip.io.a << a
    ip.io.b << b
  }.ip.io.r

  def div(a: Bits, b: Bits, vld: Bool) = new Composite(a, "div") {
    val ip = new ipFlowIO()
    ip.io.a.valid := vld
    ip.io.a.payload := a
    ip.io.b.valid := vld
    ip.io.b.payload := b
  }.ip.io.r

}

object fp32mul8s {

  val ipName = this.getClass.getSimpleName.stripSuffix("$")
  val latency = 8

  class ipFlowIO extends XilinxFloatIPStreamIO(
    ipName = ipName, latency = latency,
    numOfOperand = 2, inputWidth = 32, outputWidth = 32, isAcc = false
  )

  def mul(a: Stream[Bits], b: Stream[Bits]) = new Composite(a, "mul") {
    val ip = new ipFlowIO()
    ip.io.a << a
    ip.io.b << b
  }.ip.io.r

  def mul(a: Bits, b: Bits, vld: Bool) = new Composite(a, "mul") {
    val ip = new ipFlowIO()
    ip.io.a.valid := vld
    ip.io.a.payload := a
    ip.io.b.valid := vld
    ip.io.b.payload := b
  }.ip.io.r

}

object fp32add11 {

  val ipName = this.getClass.getSimpleName.stripSuffix("$")
  val latency = 11

  class ipFlowIO extends XilinxFloatIPFlowIO(
    ipName = ipName, latency = latency,
    numOfOperand = 2, inputWidth = 32, outputWidth = 32, isAcc = false
  )

  class ipFlowIOSim extends XilinxFloatIPFlowIOSim(
    ipName = ipName, latency = latency,
    numOfOperand = 2, inputWidth = 32, outputWidth = 32, isAcc = false
  )

  def add_sim(a: Flow[Bits], b: Flow[Bits]) = new Composite(a, "add") {
    val ip = new ipFlowIOSim()
    ip.io.a << a
    ip.io.b << b
  }.ip.io.r

  def add(a: Flow[Bits], b: Flow[Bits]) = new Composite(a, "add") {
    val ip = new ipFlowIO()
    ip.io.a << a
    ip.io.b << b
  }.ip.io.r

  def add(a: Bits, b: Bits, vld: Bool) = new Composite(a, "add") {
    val ip = new ipFlowIO()
    ip.io.a.valid := vld
    ip.io.a.payload := a
    ip.io.b.valid := vld
    ip.io.b.payload := b
  }.ip.io.r

}

object fp32sub11 {

  val ipName = this.getClass.getSimpleName.stripSuffix("$")
  val latency = 11

  class ipFlowIO extends XilinxFloatIPFlowIO(
    ipName = ipName, latency = latency,
    numOfOperand = 2, inputWidth = 32, outputWidth = 32, isAcc = false
  )

  class ipFlowIOSim extends XilinxFloatIPFlowIOSim(
    ipName = ipName, latency = latency,
    numOfOperand = 2, inputWidth = 32, outputWidth = 32, isAcc = false
  )

  def sub_sim(a: Flow[Bits], b: Flow[Bits]) = new Composite(a, "sub") {
    val ip = new ipFlowIOSim()
    ip.io.a << a
    ip.io.b << b
  }.ip.io.r

  def sub(a: Flow[Bits], b: Flow[Bits]) = new Composite(a, "sub") {
    val ip = new ipFlowIO()
    ip.io.a << a
    ip.io.b << b
  }.ip.io.r

  def sub(a: Bits, b: Bits, vld: Bool) = new Composite(a, "sub") {
    val ip = new ipFlowIO()
    ip.io.a.valid := vld
    ip.io.a.payload := a
    ip.io.b.valid := vld
    ip.io.b.payload := b
  }.ip.io.r

}

object fp32toint32d6 {

  val ipName = this.getClass.getSimpleName.stripSuffix("$")
  val latency = 6

  class ipFlowIO extends XilinxFloatIPFlowIO(
    ipName = ipName, latency = latency,
    numOfOperand = 1, inputWidth = 32, outputWidth = 32, isAcc = false
  )

  class ipFlowIOSim extends XilinxFloatIPFlowIOSim(
    ipName = ipName, latency = latency,
    numOfOperand = 1, inputWidth = 32, outputWidth = 32, isAcc = false
  )

  def to_sim(a: Flow[Bits]) = new Composite(a, "to") {
    val ip = new ipFlowIOSim()
    ip.io.a << a
  }.ip.io.r

  def to(a: Flow[Bits]) = new Composite(a, "to") {
    val ip = new ipFlowIO()
    ip.io.a << a
  }.ip.io.r

  def to(a: Bits, vld: Bool) = new Composite(a, "to") {
    val ip = new ipFlowIO()
    ip.io.a.valid := vld
    ip.io.a.payload := a
  }.ip.io.r
}

object fp32toFp16 {

  val ipName = this.getClass.getSimpleName.stripSuffix("$")
  val latency = 3

  class ipFlowIO extends XilinxFloatIPFlowIO(
    ipName = ipName, latency = latency,
    numOfOperand = 1, inputWidth = 32, outputWidth = 16, isAcc = false
  )

  class ipFlowIOSim extends XilinxFloatIPFlowIOSim(
    ipName = ipName, latency = latency,
    numOfOperand = 1, inputWidth = 32, outputWidth = 16, isAcc = false
  )

  def to_sim(a: Flow[Bits]) = new Composite(a, "to") {
    val ip = new ipFlowIOSim()
    ip.io.a << a
  }.ip.io.r

  def to(a: Flow[Bits]) = new Composite(a, "to") {
    val ip = new ipFlowIO()
    ip.io.a << a
  }.ip.io.r

  def to(a: Bits, vld: Bool) = new Composite(a, "to") {
    val ip = new ipFlowIO()
    ip.io.a.valid := vld
    ip.io.a.payload := a
  }.ip.io.r
}

object fp16toFp32 {

  val ipName = this.getClass.getSimpleName.stripSuffix("$")
  val latency = 2

  class ipFlowIO extends XilinxFloatIPFlowIO(
    ipName = ipName, latency = latency,
    numOfOperand = 1, inputWidth = 16, outputWidth = 32, isAcc = false
  )

  class ipFlowIOSim extends XilinxFloatIPFlowIOSim(
    ipName = ipName, latency = latency,
    numOfOperand = 1, inputWidth = 16, outputWidth = 32, isAcc = false
  )

  def to_sim(a: Flow[Bits]) = new Composite(a, "to") {
    val ip = new ipFlowIOSim()
    ip.io.a << a
  }.ip.io.r

  def to(a: Flow[Bits]) = new Composite(a, "to") {
    val ip = new ipFlowIO()
    ip.io.a << a
  }.ip.io.r

  def to(a: Bits, vld: Bool) = new Composite(a, "to") {
    val ip = new ipFlowIO()
    ip.io.a.valid := vld
    ip.io.a.payload := a
  }.ip.io.r
}

object fp16mulFp32 {
  def apply(fp16: Flow[Bits], fp32: Flow[Bits]) = {
    val convert = fp16toFp32.to(fp16)
    val fp32Pipe = Flow(Bits(32 bits))
    fp32Pipe.valid := Delay(fp32.valid, fp16toFp32.latency, init = False)
    fp32Pipe.payload := Delay(fp32.payload, fp16toFp32.latency)
    fp32mul8.mul(convert, fp32Pipe)
  }
}

object fp32add2Fp16 {
  def apply(a: Flow[Bits], b: Flow[Bits]) = {
    val res = fp32add11.add(a, b)
    val conv = fp32toFp16.to(res)
    conv
  }
}

object fp16toFp32s {

  val ipName = this.getClass.getSimpleName.stripSuffix("$")
  val latency = 3

  class ipFlowIO extends XilinxFloatIPStreamIO(
    ipName = ipName, latency = latency,
    numOfOperand = 1, inputWidth = 16, outputWidth = 32, isAcc = false
  )

  def to(a: Stream[Bits]) = new Composite(a, "to") {
    val ip = new ipFlowIO()
    ip.io.a << a
  }.ip.io.r

  def to(a: Bits, vld: Bool) = new Composite(a, "to") {
    val ip = new ipFlowIO()
    ip.io.a.valid := vld
    ip.io.a.payload := a
  }.ip.io.r
}

object fp32int16d4 {

  val ipName = this.getClass.getSimpleName.stripSuffix("$")
  val latency = 4

  class ipFlowIO extends XilinxFloatIPFlowIO(
    ipName = ipName, latency = latency,
    numOfOperand = 1, inputWidth = 16, outputWidth = 32, isAcc = false
  )

  class ipFlowIOSim extends XilinxFloatIPFlowIOSim(
    ipName = ipName, latency = latency,
    numOfOperand = 1, inputWidth = 16, outputWidth = 32, isAcc = false
  )

  def from_sim(a: Flow[Bits]) = new Composite(a, "from") {
    val ip = new ipFlowIOSim()
    ip.io.a << a
  }.ip.io.r

  def from(a: Flow[Bits]) = new Composite(a, "from") {
    val ip = new ipFlowIO()
    ip.io.a << a
  }.ip.io.r

}

object fp32int16d6 {

  val ipName = this.getClass.getSimpleName.stripSuffix("$")
  val latency = 6

  class ipFlowIO extends XilinxFloatIPStreamIO(
    ipName = ipName, latency = latency,
    numOfOperand = 1, inputWidth = 16, outputWidth = 32, isAcc = false
  )

  class ipFlowIOSim extends XilinxFloatIPStreamIO(
    ipName = ipName, latency = latency,
    numOfOperand = 1, inputWidth = 16, outputWidth = 32, isAcc = false
  )

  def from_sim(a: Stream[Bits]) = new Composite(a, "from") {
    val ip = new ipFlowIOSim()
    ip.io.a << a
  }.ip.io.r

  def from(a: Stream[Bits]) = new Composite(a, "from") {
    val ip = new ipFlowIO()
    ip.io.a << a
  }.ip.io.r

}

object fp16mul7s {
  val ipName = this.getClass.getSimpleName.stripSuffix("$")
  val latency = 7

  class ipFlowIO extends XilinxFloatIPStreamIO(
    ipName = ipName, latency = latency,
    numOfOperand = 2, inputWidth = 16, outputWidth = 16, isAcc = false
  )

  def mul(a: Stream[Bits], b: Stream[Bits]) = new Composite(a, "mul") {
    val ip = new ipFlowIO()
    ip.io.a << a
    ip.io.b << b
  }.ip.io.r
}

object fp32fix48d6 {
  val ipName = this.getClass.getSimpleName.stripSuffix("$")
  val latency = 6

  class ipFlowIO extends XilinxFloatIPFlowIO(
    ipName = ipName, latency = latency,
    numOfOperand = 1, inputWidth = 48, outputWidth = 32, isAcc = false
  )

  class ipFlowIOSim extends XilinxFloatIPFlowIOSim(
    ipName = ipName, latency = latency,
    numOfOperand = 1, inputWidth = 48, outputWidth = 32, isAcc = false
  )

  def from_sim(a: Flow[Bits]) = new Composite(a, "from") {
    val ip = new ipFlowIOSim()
    ip.io.a << a
  }.ip.io.r

  def from(a: Flow[Bits]) = new Composite(a, "from") {
    val ip = new ipFlowIO()
    ip.io.a << a
  }.ip.io.r
}

object fp32toFix24d6 {
  val ipName = this.getClass.getSimpleName.stripSuffix("$")
  val latency = 6

  class ipFlowIO extends XilinxFloatIPFlowIO(
    ipName = ipName, latency = latency,
    numOfOperand = 1, inputWidth = 32, outputWidth = 24, isAcc = false
  )

  class ipFlowIOSim extends XilinxFloatIPFlowIOSim(
    ipName = ipName, latency = latency,
    numOfOperand = 1, inputWidth = 32, outputWidth = 24, isAcc = false
  )

  def to_sim(a: Flow[Bits]) = new Composite(a, "to") {
    val ip = new ipFlowIOSim()
    ip.io.a << a
  }.ip.io.r

  def to(a: Flow[Bits]) = new Composite(a, "to") {
    val ip = new ipFlowIO()
    ip.io.a << a
  }.ip.io.r
}

object fp16_ip_path {
  var ip_base = "./fp16ip"
  val ip_name = List(
    fp16add8.ipName,
    fp16sub8.ipName,
    fp16mul6.ipName,
    fp16div12.ipName,
    fp16ex12.ipName,
    fp16lt0.ipName,
    fp16acc16.ipName
  )
  val paths = ip_name.map(ip_base + _ + "/").to[ArrayBuffer]
}
