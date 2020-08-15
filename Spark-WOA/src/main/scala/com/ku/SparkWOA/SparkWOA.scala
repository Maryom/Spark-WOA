package com.ku.SparkWOA

import org.apache.log4j._
import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.mllib.rdd.RDDFunctions._
import scala.io.Source
import breeze.linalg._
import breeze.math._
import breeze.numerics._
import breeze.stats.distributions._
import org.apache.spark.rdd.RDD

object SparkWOA {
  
  val PopulationSize = 30
  val Max_iter       = 500
  var t              = 0 // Loop counter
  val (lb, ub)       = BenchmarksFun.getBoundary("F1")
  
  // initialize score for the leader
  var Leader_pos = (Double.PositiveInfinity, Array.fill(BenchmarksFun.dim)(0.0))
  
  def main(args: Array[String]): Unit = {
    
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val t1 = System.nanoTime
    // Create a SparkContext using every core of the cluster
    val sc: SparkContext = new SparkContext("local[*]", "SparkWOA")
    
    val partitionsNum = sc.defaultParallelism
    /* Initialize the positions of search agents
    *
    * scala.util.Random.nextDouble() = numpy.random.uniform(0,1)
    * List.fill(PopulationSize)(List.fill(dim) = (SearchAgents_no,dim)
    */ 
    val pos_fitnessTmp = sc.parallelize(List.range(0, PopulationSize), partitionsNum).map(index => (index, Array.fill(BenchmarksFun.dim)(scala.util.Random.nextDouble()*(ub-lb)+lb)))
    
    val pos_fitness = pos_fitnessTmp.mapPartitions(WoaStuff.updatePositions).cache
        
    val posBC       = sc.broadcast(pos_fitness.map(x=> (x._1, x._2)).collectAsMap)
    
    Leader_pos = pos_fitness.map(x=> (x._3, x._2)).sortByKey(true).take(1)(0)
        
    var Leader_posBC = sc.broadcast(Leader_pos)
    
    t = t + 1

    // Apply a function to update the position of search agents (each element of RDD)
    var posArray: Array[(Int, Array[Double])] = sc.parallelize(List.range(0, PopulationSize), partitionsNum).map { agentNum =>
          
          val rand_leader_index = math.floor(agentNum*math.random).toInt
 
          val X_rand            = posBC.value.getOrElse(rand_leader_index, Array[Double]())
          
          WoaStuff.updatePosBC(agentNum, X_rand, posBC.value.getOrElse(agentNum, Array[Double]()), Leader_posBC.value._2, Max_iter.toDouble, t.toDouble) 

    }.collect
    
    // cleaning
    posBC.destroy
    
    while( t < Max_iter ) {
      
      val pos_fitness = sc.parallelize(posArray, partitionsNum).mapPartitions(WoaStuff.updatePositions).cache
      
      val posBC       = sc.broadcast(pos_fitness.map(x=> (x._1, x._2)).collectAsMap)
      
      val tmpFitness  = pos_fitness.map(x=> (x._3, x._2)).sortByKey(true).take(1)(0)
        
      if (tmpFitness._1 < Leader_posBC.value._1 ) {
        
        Leader_pos = tmpFitness
        Leader_posBC.destroy
        Leader_posBC = sc.broadcast(Leader_pos)
      }
      
      t = t + 1
      
      // apply a function to update the Position of search agents (each element of RDD)
      posArray = sc.parallelize(List.range(0, PopulationSize), partitionsNum).map { agentNum =>
          
          val rand_leader_index = math.floor(agentNum*math.random).toInt
 
          val X_rand            = posBC.value.getOrElse(rand_leader_index, Array[Double]())
          
          WoaStuff.updatePosBC( agentNum, X_rand, posBC.value.getOrElse(agentNum, Array[Double]()), Leader_posBC.value._2, Max_iter.toDouble, t.toDouble) 
        
      }.collect

      // cleaning
      posBC.destroy
    }
    
    // As it is a nano second we need to divide it by 1000000000. in 1e9d "d" stands for double
    val duration = (System.nanoTime - t1) / 1e9d
    
    println("Timer", duration)
    
    sc.stop()
  }
}