package util

import spinal.core._
import spinal.lib._

import scala.language.postfixOps

case class Linked[T1 <: Data, T2 <: Data](typeA: HardType[T1], typeB: HardType[T2]) extends Bundle {
  val A = typeA()
  val B = typeB()
}


case class Linked3[T1 <: Data, T2 <: Data, T3 <: Data](typeA: HardType[T1], typeB: HardType[T2], typeC: HardType[T3]) extends Bundle {
  val A = typeA()
  val B = typeB()
  val C = typeC()
}

case class Linked4[T1 <: Data, T2 <: Data, T3 <: Data, T4 <: Data](typeA: HardType[T1], typeB: HardType[T2], typeC: HardType[T3], typeD: HardType[T4]) extends Bundle {
  val A = typeA()
  val B = typeB()
  val C = typeC()
  val D = typeD()
}

case class Linked5[T1 <: Data, T2 <: Data, T3 <: Data, T4 <: Data, T5 <: Data](typeA: HardType[T1], typeB: HardType[T2], typeC: HardType[T3], typeD: HardType[T4], typeE: HardType[T5]) extends Bundle {
  val A = typeA()
  val B = typeB()
  val C = typeC()
  val D = typeD()
  val E = typeE()
}