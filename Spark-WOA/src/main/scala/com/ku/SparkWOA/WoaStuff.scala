package com.ku.SparkWOA

import breeze.linalg._
import breeze.math._
import breeze.numerics._
import breeze.stats.distributions._

object WoaStuff {

  def updatePosBC( searchAgentNum: Int, X_rand: Array[Double], pos: Array[Double], Leader_pos: Array[Double], iter: Double, tNum: Double ) = {

      var newPositions = Array.fill(X_rand.length)(0.0)
      
      // a decreases linearly from 2 to 0 in Eq. (2.3) : Double
      val a = 2.0-tNum*((2.0)/iter)
    
      // a2 linearly decreases from -1 to -2 to calculate t in Eq. (3.12)
      val a2 = -1.0+tNum*((-1.0)/iter)
      
      val b = 1.0 // parameters in Eq. (2.5)
      
      BenchmarksFun.listDim.foreach { index =>
     
      // r1 is a random number in [0,1]. nextFloat returns a value between 0.0 and 1.0
      val r1 = scala.util.Random.nextFloat
      
      // r2 is a random number in [0,1]. nextFloat returns a value between 0.0 and 1.0
      val r2 = scala.util.Random.nextFloat

      // Eq. (2.3) in the paper
      val A = 2.0*a*r1-a
      
      // Eq. (2.4) in the paper
      val C: Double = 2.0*r2
      
      // parameters in Eq. (2.5)
      val l = (a2-1.0)*math.random+1.0
      
      // p in Eq. (2.6)
      val p = math.random
        
        if (p < 0.5) {
          if (abs(A) >= 1) {

            val X_randJ  = X_rand(index)
            val D_X_rand = abs(C * X_randJ - pos(index))
            
            newPositions(index) = X_randJ - A * D_X_rand
            
          } else {
            
            val D_Leader        = abs(C * Leader_pos(index) - pos(index))
            
            newPositions(index) = Leader_pos(index) - A * D_Leader
          }
          
        } else {

          val distance2Leader = abs(Leader_pos(index) - pos(index))

          // Eq. (2.5)
          newPositions(index) = distance2Leader*math.exp(b*l)*math.cos(l*2.0*breeze.numerics.constants.Pi) + Leader_pos(index)
        }
      }
      
      (searchAgentNum, newPositions)
  }
  
  def updatePositions( searchAgent: Iterator[(Int, Array[Double])] ) = {

    searchAgent.map{ positions =>
      
      // Positions[i,:]=numpy.clip(Positions[i,:], lb, ub) = map(x => clip(x, lb, ub))
      val clippedPos = clip(positions._2, SparkWOA.lb, SparkWOA.ub)

      /* Calculate objective function for each search agent
     			* 
     			* It will return (fitness, searchAgent)
     			*/
           val fitness = BenchmarksFun.f1(clippedPos)

      (positions._1, clippedPos, fitness)
    }
  }
}