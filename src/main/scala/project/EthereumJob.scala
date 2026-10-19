package project

import org.apache.spark.HashPartitioner
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}
import utils.Commons

/**
 * Main Spark application for the Ethereum Big Data project.
 *
 * Implements three variations of an Ethereum analytics pipeline that joins
 * transactions with blocks to compute daily metrics grouped by `(Date, Miner, IsContractCall)`:
 *   - Job 1: Baseline unoptimized execution (shuffle join followed by reduceByKey).
 *   - Job 2: Optimized execution using block-level pre-aggregation and co-partitioning.
 *   - Job 3: Optimized execution using broadcast hash join.
 */
object EthereumJob {

  private final val sampleBlocksPath: String = "/datasets/sample/blocks-sample.csv"
  private final val bigBlocksPath: String = "/datasets/big/blocks.csv"
  private final val sampleTxPath: String = "/datasets/sample/transactions-sample.csv"
  private final val bigTxPath: String = "/datasets/big/transactions.csv"
  private final val defaultNumPartitions: Int = 8

  /**
   * Combines two metric tuples `(count, totalValueGwei, totalGasPrice)`.
   */
  private final val sumMetrics: ((Long, Long, Long), (Long, Long, Long)) => (Long, Long, Long) =
    (v1, v2) => (v1._1 + v2._1, v1._2 + v2._2, v1._3 + v2._3)

  /**
   * Combines per-block metrics split by transaction type:
   * `(simpleMetrics, contractMetrics)` where each is `(count, totalValueGwei, totalGasPrice)`.
   */
  private final val sumBlockMetrics: (
      ((Long, Long, Long), (Long, Long, Long)),
      ((Long, Long, Long), (Long, Long, Long))
  ) => ((Long, Long, Long), (Long, Long, Long)) =
    (b1, b2) => (sumMetrics(b1._1, b2._1), sumMetrics(b1._2, b2._2))


  /**
   * Application entry point.
   *
   * @param args command-line arguments:
   *             args(0) - deployment mode ("local", "remote", or "sharedRemote")
   *             args(1) - job number ("1", "2", or "3")
   *             args(2) - optional partition count (default: 8)
   */
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      println("Usage: <deploymentMode> <jobNumber> [numPartitions]")
      println("  deploymentMode: local | remote | sharedRemote")
      println("  jobNumber:      1 (Unoptimized: join-and-aggregate)")
      println("                  2 (Optimized: pre-aggregation + co-partitioning)")
      println("                  3 (Optimized: broadcast join)")
      println("  numPartitions:  optional integer (default: 8)")
      sys.exit(1)
    }

    val deploymentMode = args(0)
    val writeMode = if (deploymentMode == "sharedRemote") "remote" else deploymentMode
    val job = args(1)
    val numPartitions = if (args.length >= 3) args(2).toInt else defaultNumPartitions

    val sparkConf = new SparkConf().setAppName(s"Ethereum Project - Job $job (P=$numPartitions)")
    implicit val spark: SparkSession = SparkSession.builder.config(sparkConf).getOrCreate()
    val sc = spark.sparkContext

    val malformedBlocksAcc = sc.longAccumulator("MalformedBlockLines")
    val malformedTxAcc = sc.longAccumulator("MalformedTxLines")

    val blocksPath = if (deploymentMode == "local") sampleBlocksPath else bigBlocksPath
    val txPath = if (deploymentMode == "local") sampleTxPath else bigTxPath
    val outPath = s"/output/ethereumJob$job"

    val rddBlocks = sc.textFile(Commons.getDatasetPath(deploymentMode, blocksPath))
      .flatMap(line => EthereumParser.parseBlock(line, malformedBlocksAcc))

    val rddTx = sc.textFile(Commons.getDatasetPath(deploymentMode, txPath))
      .flatMap(line => EthereumParser.parseTransaction(line, malformedTxAcc))

    job match {
      case "1" => runJob1(rddTx, rddBlocks, numPartitions, writeMode, outPath)
      case "2" => runJob2(rddTx, rddBlocks, numPartitions, writeMode, outPath)
      case "3" => runJob3(rddTx, rddBlocks, sc, numPartitions, writeMode, outPath)
      case _   => println(s"Invalid job number: $job. Valid options are 1, 2, or 3.")
    }

    println(
      s"[Job $job | P=$numPartitions] Completed. " +
        s"Malformed blocks: ${malformedBlocksAcc.value}, Malformed tx: ${malformedTxAcc.value}"
    )
  }

  /**
   * Executes Job 1: Baseline unoptimized pipeline (2 shuffles).
   * Uses explicit `numPartitions` on both join and reduceByKey for a fair comparison with Job 2.
   *
   * @param rddTx         parsed transactions RDD: (blockNumber, (valueGwei, gasPrice, isContractCall))
   * @param rddBlocks     parsed blocks RDD: (blockNumber, (dateString, minerAddress))
   * @param numPartitions number of partitions to use for join and aggregation
   * @param writeMode     storage target mode ("local" or "remote")
   * @param outPath       relative destination directory for output CSV
   * @param spark         implicit active SparkSession
   */
  def runJob1(
      rddTx: RDD[(Long, (Long, Long, Boolean))],
      rddBlocks: RDD[(Long, (String, String))],
      numPartitions: Int,
      writeMode: String,
      outPath: String
  )(implicit spark: SparkSession): Unit = {
    println(s"Running Job 1: Unoptimized (Join-and-Aggregate, partitions=$numPartitions)")

    val aggregated = rddTx
      .join(rddBlocks, numPartitions)
      .map { case (_, ((valueGwei, gasPrice, isContractCall), (date, miner))) =>
        ((date, miner, isContractCall), (1L, valueGwei, gasPrice))
      }
      .reduceByKey(sumMetrics, numPartitions)

    saveResults(aggregated, writeMode, outPath)
  }

  /**
   * Executes Job 2: Optimized pipeline using pre-aggregation and co-partitioning.
   * Pre-aggregates transaction counts, values, and gas prices per block before joining,
   * and uses a common [[HashPartitioner]] to avoid shuffle during the join phase.
   *
   * @param rddTx         parsed transactions RDD: (blockNumber, (valueGwei, gasPrice, isContractCall))
   * @param rddBlocks     parsed blocks RDD: (blockNumber, (dateString, minerAddress))
   * @param numPartitions number of partitions to use for co-partitioning
   * @param writeMode     storage target mode ("local" or "remote")
   * @param outPath       relative destination directory for output CSV
   * @param spark         implicit active SparkSession
   */
  def runJob2(
      rddTx: RDD[(Long, (Long, Long, Boolean))],
      rddBlocks: RDD[(Long, (String, String))],
      numPartitions: Int,
      writeMode: String,
      outPath: String
  )(implicit spark: SparkSession): Unit = {
    println(s"Running Job 2: Optimized (Pre-aggregation + Co-Partitioning, partitions=$numPartitions)")

    val partitioner = new HashPartitioner(numPartitions)
    val zeroMetric = (0L, 0L, 0L)

    val preAggTx = rddTx
      .mapValues { case (valueGwei, gasPrice, isContractCall) =>
        val metric = (1L, valueGwei, gasPrice)
        if (isContractCall) (zeroMetric, metric) else (metric, zeroMetric)
      }
      .reduceByKey(partitioner, sumBlockMetrics)

    val partitionedBlocks = rddBlocks.partitionBy(partitioner)

    val aggregated = preAggTx
      .join(partitionedBlocks)
      .flatMap { case (_, ((simpleMetrics, contractMetrics), (date, miner))) =>
        val simpleEntry =
          if (simpleMetrics._1 > 0) Seq(((date, miner, false), simpleMetrics)) else Seq.empty
        val contractEntry =
          if (contractMetrics._1 > 0) Seq(((date, miner, true), contractMetrics)) else Seq.empty
        simpleEntry ++ contractEntry
      }
      .reduceByKey(partitioner, sumMetrics)

    saveResults(aggregated, writeMode, outPath)
  }

  /**
   * Executes Job 3: Optimized pipeline using broadcast hash join.
   * Collects the block dimension table as a map, broadcasts it to worker nodes,
   * and maps transactions directly without any join-induced shuffle.
   *
   * @param rddTx         parsed transactions RDD: (blockNumber, (valueGwei, gasPrice, isContractCall))
   * @param rddBlocks     parsed blocks RDD: (blockNumber, (dateString, minerAddress))
   * @param sc            active SparkContext for broadcasting
   * @param numPartitions number of partitions to use for final aggregation
   * @param writeMode     storage target mode ("local" or "remote")
   * @param outPath       relative destination directory for output CSV
   * @param spark         implicit active SparkSession
   */
  def runJob3(
      rddTx: RDD[(Long, (Long, Long, Boolean))],
      rddBlocks: RDD[(Long, (String, String))],
      sc: SparkContext,
      numPartitions: Int,
      writeMode: String,
      outPath: String
  )(implicit spark: SparkSession): Unit = {
    println(s"Running Job 3: Optimized (Broadcast Join, partitions=$numPartitions)")

    val bBlocks = sc.broadcast(rddBlocks.collectAsMap())

    val aggregated = rddTx
      .flatMap { case (blockNum, (valueGwei, gasPrice, isContractCall)) =>
        bBlocks.value.get(blockNum).map { case (date, miner) =>
          ((date, miner, isContractCall), (1L, valueGwei, gasPrice))
        }
      }
      .reduceByKey(sumMetrics, numPartitions)

    saveResults(aggregated, writeMode, outPath)
  }


  /**
   * Sorts results deterministically by key `(Date, Miner, IsContractCall)` into a single
   * partition, computes average gas price, and writes the output as a CSV file.
   *
   * @param rdd       aggregated RDD of `((date, miner, isContractCall), (count, totalValueGwei, totalGasPrice))`
   * @param writeMode storage target mode ("local" or "remote")
   * @param outPath   relative destination directory for output CSV
   * @param spark     implicit active SparkSession
   */
  private def saveResults(
      rdd: RDD[((String, String, Boolean), (Long, Long, Long))],
      writeMode: String,
      outPath: String
  )(implicit spark: SparkSession): Unit = {
    import spark.implicits._

    val formattedRdd: RDD[(String, String, Boolean, Long, Long, Double)] = rdd
      .sortByKey(ascending = true, numPartitions = 1)
      .map { case ((date, miner, isContractCall), (count, totalVal, totalGasPrice)) =>
        val avgGasPrice = if (count > 0) totalGasPrice.toDouble / count else 0.0
        (date, miner, isContractCall, count, totalVal, avgGasPrice)
      }

    formattedRdd
      .toDF("Date", "Miner", "IsContractCall", "TxCount", "TotalValueGwei", "AvgGasPrice")
      .write
      .format("csv")
      .mode(SaveMode.Overwrite)
      .option("header", "true")
      .save(Commons.getDatasetPath(writeMode, outPath))
  }
}