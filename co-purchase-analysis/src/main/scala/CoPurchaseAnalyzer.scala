import org.apache.spark.sql.SparkSession
import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD

object CoPurchaseAnalyzer {
  
  def parseOrderLine(line: String): (Int, Int) = {
    val Array(customerId, productId) = line.split(",", 2)
    (customerId.toInt, productId.toInt)
  }

  def generateProductPairs(products: Array[Int]): Iterator[(Int, Int)] = {
    val distinctProducts = products.distinct.sorted
    for {
      i <- distinctProducts.indices.iterator
      j <- (i + 1 until distinctProducts.length).iterator
    } yield (distinctProducts(i), distinctProducts(j))
  }

  def analyzeCoPurchases(inputPath: String, outputPath: String): Unit = {
    val spark = SparkSession.builder()
      .appName("CoPurchase Analysis")
      .config("spark.executor.memory", "6g")
      .config("spark.executor.cores", "4")
      .config("spark.driver.memory", "4g")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .master(sys.props.getOrElse("spark.master", "local[*]"))
      .getOrCreate()

    val sc = spark.sparkContext
    
    // Calculate optimal partitions based on cluster size
    val executorInstances = sc.getConf.get("spark.executor.instances", "4").toInt
    val executorCores = sc.getConf.get("spark.executor.cores", "4").toInt
    val partitions = executorInstances * executorCores * 3
    
    try {
      val startTime = System.currentTimeMillis()

      // Read and parse orders
      val orders = sc.textFile(inputPath, partitions)
        .map(parseOrderLine)

      // Partition orders by customer for optimal distribution
      val partitionedOrders = orders
        .partitionBy(new HashPartitioner(partitions))

      // Group by customer and generate product pairs
      val productPairs = partitionedOrders
        .groupByKey()
        .flatMap { case (_, products) => 
          generateProductPairs(products.toArray) 
        }

      // Count frequencies
      val coPurchaseFrequencies = productPairs
        .map((_, 1))
        .reduceByKey(_ + _)

      // Convert to CSV format and save with final repartition
      val csvOutput = coPurchaseFrequencies
        .map { case ((product1, product2), frequency) => 
          s"$product1,$product2,$frequency" 
        }
        .repartition(1)
        
      csvOutput.saveAsTextFile(outputPath)

      val endTime = System.currentTimeMillis()
      val elapsedTime = endTime - startTime
      
      println(s"Analysis completed in ${elapsedTime}ms (${elapsedTime/1000.0}s)")
      println(s"Results saved to: $outputPath")
    } finally {
      spark.stop()
    }
  }

  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println("Usage: CoPurchaseAnalyzer <input_path> <output_path>")
      println(args(0), args(1))
      System.exit(1)
    }
    
    analyzeCoPurchases(args(0), args(1))
  }
}