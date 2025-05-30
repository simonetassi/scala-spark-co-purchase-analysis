package com.scp.app

import org.apache.spark.{SparkConf, SparkContext, HashPartitioner}
import org.apache.spark.rdd.RDD

object CoPurchaseAnalyzer {
  case class ProductPair(product1: Int, product2: Int) {
    def toSortedString: String = s"$product1,$product2"
  }

  case class CoPurchaseResult(productPair: ProductPair, frequency: Int) {
    def toCsvString: String = s"${productPair.toSortedString},$frequency"
  }

  def parseOrderLine(line: String): (Int, Int) = {
    val fields = line.split(",")
    require(fields.length >= 2, s"Invalid CSV format: $line")

    (fields(0).toInt, fields(1).toInt)
  }

  def generateProductPairs(products: Iterable[Int]): List[ProductPair] = {
    val productList = products.toList.distinct
    productList.combinations(2)
      .map(_.sorted)
      .map { case List(p1, p2) => ProductPair(p1, p2) }
      .toList
  }

  def extractProductPairs(orders: RDD[(Int, Int)]): RDD[ProductPair] = {
    orders
      .groupByKey()
      .flatMapValues(generateProductPairs)
      .values
  }

  def calculateCoPurchaseFrequencies(productPairs: RDD[ProductPair], nPartitions: Int): RDD[CoPurchaseResult] = {
    productPairs
      .map(pair => (pair, 1))
      .partitionBy(new HashPartitioner(nPartitions))
      .reduceByKey(_ + _)
      .map { case (pair, count) => CoPurchaseResult(pair, count) }
      // .sortBy(_.frequency, ascending = false)
      .coalesce(1)
  }

  def analyzeCoPurchases(inputPath: String, outputPath: String, numPartitions: Int): Unit = {
    val conf = new SparkConf()
      .setAppName("CoPurchase Analysis").setMaster("local[*]")
      // .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    
    val sc = new SparkContext(conf)
    
    try {
      // Read and parse input data directly into tuples
      val rawData = sc.textFile(inputPath)
      val orders = rawData.map(parseOrderLine)
      
      // Extract product pairs from customer orders
      val productPairs = extractProductPairs(orders)
      
      // Calculate frequencies and sort (with optimal partitioning)
      val coPurchaseResults = calculateCoPurchaseFrequencies(productPairs, numPartitions)
      
      // Save results as CSV
      val csvOutput = coPurchaseResults.map(_.toCsvString)
      csvOutput.saveAsTextFile(outputPath)
      
      println(s"Co-purchase analysis completed. Results saved to: $outputPath")
      
    } finally {
      sc.stop()
    }
  }

  def main(args: Array[String]): Unit = {
    if (args.length != 3) {
      println("Usage: CoPurchaseAnalyzer <input_path> <output_path> <num_partitions>")
      System.exit(1)
    }
    
    val inputPath = args(0)
    val outputPath = args(1)
    val numPartitions = try {
      args(2).toInt
    } catch {
      case _: NumberFormatException =>
        println(s"Invalid partition number: ${args(2)}")
        System.exit(1)
        ???
    }
    
    if (numPartitions <= 0) {
      println("Number of partitions must be positive")
      System.exit(1)
    }
    
    analyzeCoPurchases(inputPath, outputPath, numPartitions)
  }
}
