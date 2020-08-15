package com.ku.SparkWOA

import breeze.linalg._
import breeze.math._
import breeze.numerics._
import breeze.stats.distributions._

object BenchmarksFun {
  
  val dim = 30
  val listDim = List.range(0,dim)

  def getBoundary( benchmark: String ) : (Double, Double) = {
    benchmark match {
    case "F1"|"F3"|"F4" => return (-100.0, 100.0)
    case "F2"           => return (-10.0, 10.0)
    case "F5"           => return (-30.0, 30.0)
    case "F6"           => return (-5.12, 5.12)
    case "F7"           => return (-32.0, 32.0)
    }
  }
  
  // Sphere
  def f1( searchAgent: Array[Double] ) = {
    sum(searchAgent.map(agent => pow(agent, 2)))
  }

  // Schwefel 2.22
  def f2( searchAgent: Array[Double] ) = {

    sum( searchAgent.map { i => i.abs } ) + searchAgent.map { i => i.abs }.product
  }

  // Schwefel 1.2
  def f3( searchAgent: Array[Double] ) = {

    val dim = searchAgent.length

    var result = 0.0
    for (i <- 1 to  dim) {
      result = result + pow(sum(searchAgent.slice(0, i)), 2)
    }

    result
  }

  // Schwefel 2.21
  def f4( searchAgent: Array[Double] ) = {

    max( searchAgent.map { i => i.abs } )
  }
  
  // Rosenbrock
  def f5( searchAgent: Array[Double] ) = {
    
    val dim = searchAgent.length
    
    val tmp = searchAgent.slice(0, dim-1)

    sum(pow((searchAgent.slice(1, dim) - pow(tmp, 2)), 2).map(_*100.0).zip(pow(tmp.map(_-1), 2)).map { case (a, b) => a + b })
  }
  
  // Rastrigin
  def f6(x: Array[Double]) = {

    val dim = x.length

    sum(x.map(i=>pow(i, 2)-10.0*cos(2.0*i*(constants.Pi))+10.0))
  }
  
  // Ackley
  def f7(x: Array[Double]) = {

    val dim = x.length
    
    -20.0*exp(-0.2*sqrt(sum(x.map(pow(_, 2)))/dim)) - exp(sum(x.map(i=>cos(2.0*i*(constants.Pi))))/dim)+20.0+exp(1.0)
  }
}