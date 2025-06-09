package com.scp.app

import org.apache.spark.sql.SparkSession
import org.apache.spark.HashPartitioner
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
  }

  def analyzeCoPurchases(inputPath: String, outputPath: String): Unit = {
    val spark = SparkSession.builder()
      .appName("CoPurchase Analysis")
      .master(sys.props.getOrElse("spark.master", "local[*]"))     
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") 
      .getOrCreate()

    val cores = spark.conf.get("spark.executor.cores", "4").toInt
    val nodes = spark.conf.get("spark.executor.instances", "4").toInt
    val numPartitions =
      math.max(cores * nodes * 2, spark.sparkContext.defaultParallelism * 2)
    
    try {
      // Register start time
      val startTime = System.currentTimeMillis() 


      // Read and parse input data directly into tuples
      val rawData = spark.sparkContext.textFile(inputPath)
      val orders = rawData.map(parseOrderLine)
      
      // Extract product pairs from customer orders
      val productPairs = extractProductPairs(orders)
      
      // Calculate frequencies and sort (with optimal partitioning)
      val coPurchaseResults = calculateCoPurchaseFrequencies(productPairs, numPartitions)
      
      // Save results as CSV
      val csvOutput = coPurchaseResults.map(_.toCsvString).coalesce(1)
      csvOutput.saveAsTextFile(outputPath)
      
      // Register end time
      val endTime = System.currentTimeMillis()

      // Calculate elapsed time 
      val elapsedTime = endTime - startTime
      
      println(s"Co-purchase analysis completed in ${elapsedTime}ms (${elapsedTime/1000.0}s)")
      println(s"Results saved to: $outputPath")      
    } finally {
      spark.stop()
    }
  }

  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println("Usage: CoPurchaseAnalyzer <input_path> <output_path>")
      System.exit(1)
    }
    
    val inputPath = args(0)
    val outputPath = args(1)
    
    analyzeCoPurchases(inputPath, outputPath)
  }
}
