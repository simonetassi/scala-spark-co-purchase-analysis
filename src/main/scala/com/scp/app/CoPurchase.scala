package com.scp.app

import org.apache.spark.{SparkConf, SparkContext, HashPartitioner}

object CoPurchase {
  def coPurchase(fileName: String, resultAddr: String): Unit = {
    val conf = new SparkConf().setAppName("coPurchase")
    val sc = new SparkContext(conf)

    val ordersCsv = sc.textFile(fileName)
    val orders = ordersCsv.map(line => line.split(","))
    .map(arr => (arr(0).toInt, arr(1).toInt))

    val groupedOrders = orders.groupByKey()

    val productPairs = groupedOrders.flatMapValues(products => products.toList.combinations(2).map(_.sorted))
    val productPairsHP = productPairs.partitionBy(new HashPartitioner(3))

    val coPurchase = productPairsHP.map(pair => (pair._2, 1))
    .reduceByKey(_ + _).map(pair => (pair._1.mkString(","), pair._2))
    .sortBy(_._2, false).map(pair => pair._1 + "," + pair._2)
    
    coPurchase.saveAsTextFile(resultAddr)
  }

  def main(args: Array[String]): Unit = {
    val inputAddr = args(0)
    val resultAddr = args(1)
    coPurchase(inputAddr, resultAddr)
  }
}
