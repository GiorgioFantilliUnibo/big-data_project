package project

import org.apache.spark.HashPartitioner
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel
import EthereumParser.Tx
import utils.Commons

/**
 * Main Spark application for the Ethereum Big Data project.
 *
 * Implements three variations of the same Ethereum analytics pipeline. Each transaction is
 * enriched with the activity tier of its sender and the day and miner of its block.
 * Daily metrics are then computed and grouped by `(Date, Miner, IsContractCall, Tier)`:
 *   - Job 1: Baseline unoptimized execution (plain shuffle joins followed by reduceByKey).
 *   - Job 2: Optimized execution using a co-partitioned and cached self-join,
 *            and pre-aggregation before the join with blocks.
 *   - Job 3: As Job 2, but the join with blocks is a broadcast hash join.
 */
object EthereumJob {

  private final val sampleBlocksPath: String = "/datasets/sample/blocks-sample.csv"
  private final val bigBlocksPath: String = "/datasets/big/blocks.csv"
  private final val sampleTxPath: String = "/datasets/sample/transactions-sample.csv"
  private final val bigTxPath: String = "/datasets/big/transactions.csv"
  private final val defaultNumPartitions: Int = 8

  private final val Tier1Max: Long = 1L
  private final val Tier2Max: Long = 10L
  private final val Tier3Max: Long = 100L

  private def tierOf(txCount: Long): Int =
    if (txCount <= Tier1Max) 1
    else if (txCount <= Tier2Max) 2
    else if (txCount <= Tier3Max) 3
    else 4

  /**
   * Combines two metric tuples `(count, totalValueGwei, totalGasPrice)`.
   */
  private final val sumMetrics: ((Long, Long, Long), (Long, Long, Long)) => (Long, Long, Long) =
    (v1, v2) => (v1._1 + v2._1, v1._2 + v2._2, v1._3 + v2._3)

  private type Key = (String, String, Boolean, Int)


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
      println("  jobNumber:      1 (Unoptimized: self-join + join-and-aggregate)")
      println("                  2 (Optimized: co-partitioned self-join + cache + pre-aggregation)")
      println("                  3 (Optimized: co-partitioned self-join + cache + broadcast join)")
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
   * Executes Job 1: Baseline unoptimized pipeline.
   * Enriches transactions with sender activity tiers and block metadata using plain
   * shuffle joins, and computes aggregated metrics grouped by `(Date, Miner, IsContractCall, Tier)`.
   *
   * @param rddTx         parsed transactions RDD: (blockNumber, (from, valueGwei, gasPrice, isContractCall))
   * @param rddBlocks     parsed blocks RDD: (blockNumber, (dateString, minerAddress))
   * @param numPartitions number of partitions to use for joins and aggregations
   * @param writeMode     storage target mode ("local" or "remote")
   * @param outPath       relative destination directory for output CSV
   * @param spark         implicit active SparkSession
   */
  def runJob1(
      rddTx: RDD[(Long, Tx)],
      rddBlocks: RDD[(Long, (String, String))],
      numPartitions: Int,
      writeMode: String,
      outPath: String
  )(implicit spark: SparkSession): Unit = {
    println(s"Running Job 1: Unoptimized (Self-join + Join-and-Aggregate, partitions=$numPartitions)")

    val tiers = rddTx
      .map { case (_, (from, _, _, _)) => (from, 1L) }
      .reduceByKey(_ + _, numPartitions)
      .mapValues(tierOf)

    val txWithTier = rddTx
      .map { case (block, (from, valueGwei, gasPrice, isContractCall)) =>
        (from, (block, valueGwei, gasPrice, isContractCall))
      }
      .join(tiers, numPartitions)
      .map { case (_, ((block, valueGwei, gasPrice, isContractCall), tier)) =>
        (block, (tier, valueGwei, gasPrice, isContractCall))
      }

    val aggregated = txWithTier
      .join(rddBlocks, numPartitions)
      .map { case (_, ((tier, valueGwei, gasPrice, isContractCall), (date, miner))) =>
        ((date, miner, isContractCall, tier), (1L, valueGwei, gasPrice))
      }
      .reduceByKey(sumMetrics, numPartitions)

    saveResults(aggregated, writeMode, outPath)
  }

  /**
   * Executes Job 2: Optimized pipeline using co-partitioning and pre-aggregation.
   * Computes the same metrics as Job 1, optimizing the self-join through co-partitioning
   * and caching, and reducing shuffle volume via pre-aggregation prior to joining with blocks.
   *
   * @param rddTx         parsed transactions RDD: (blockNumber, (from, valueGwei, gasPrice, isContractCall))
   * @param rddBlocks     parsed blocks RDD: (blockNumber, (dateString, minerAddress))
   * @param numPartitions number of partitions to use for co-partitioning
   * @param writeMode     storage target mode ("local" or "remote")
   * @param outPath       relative destination directory for output CSV
   * @param spark         implicit active SparkSession
   */
  def runJob2(
      rddTx: RDD[(Long, Tx)],
      rddBlocks: RDD[(Long, (String, String))],
      numPartitions: Int,
      writeMode: String,
      outPath: String
  )(implicit spark: SparkSession): Unit = {
    println(s"Running Job 2: Optimized (Co-partitioned self-join + cache + pre-aggregation, partitions=$numPartitions)")

    val partitioner = new HashPartitioner(numPartitions)

    val aggregated = tierSelfJoinOptimized(rddTx, partitioner)
      .map { case (block, (tier, valueGwei, gasPrice, isContractCall)) =>
        ((block, tier, isContractCall), (1L, valueGwei, gasPrice))
      }
      .reduceByKey(partitioner, sumMetrics)
      .map { case ((block, tier, isContractCall), metrics) =>
        (block, (tier, isContractCall, metrics))
      }
      .join(rddBlocks, partitioner)
      .map { case (_, ((tier, isContractCall, metrics), (date, miner))) =>
        ((date, miner, isContractCall, tier), metrics)
      }
      .reduceByKey(partitioner, sumMetrics)

    saveResults(aggregated, writeMode, outPath)
  }

  /**
   * Executes Job 3: Optimized pipeline using co-partitioning and broadcast join.
   * Computes the same metrics as Job 1, optimizing the self-join via co-partitioning
   * and caching, and performing the join with blocks through a broadcast hash join.
   *
   * @param rddTx         parsed transactions RDD: (blockNumber, (from, valueGwei, gasPrice, isContractCall))
   * @param rddBlocks     parsed blocks RDD: (blockNumber, (dateString, minerAddress))
   * @param sc            active SparkContext for broadcasting
   * @param numPartitions number of partitions to use for the self-join and the final aggregation
   * @param writeMode     storage target mode ("local" or "remote")
   * @param outPath       relative destination directory for output CSV
   * @param spark         implicit active SparkSession
   */
  def runJob3(
      rddTx: RDD[(Long, Tx)],
      rddBlocks: RDD[(Long, (String, String))],
      sc: SparkContext,
      numPartitions: Int,
      writeMode: String,
      outPath: String
  )(implicit spark: SparkSession): Unit = {
    println(s"Running Job 3: Optimized (Co-partitioned self-join + cache + broadcast join, partitions=$numPartitions)")

    val partitioner = new HashPartitioner(numPartitions)
    val bBlocks = sc.broadcast(rddBlocks.collectAsMap())

    val aggregated = tierSelfJoinOptimized(rddTx, partitioner)
      .flatMap { case (blockNum, (tier, valueGwei, gasPrice, isContractCall)) =>
        bBlocks.value.get(blockNum).map { case (date, miner) =>
          ((date, miner, isContractCall, tier), (1L, valueGwei, gasPrice))
        }
      }
      .reduceByKey(partitioner, sumMetrics)

    saveResults(aggregated, writeMode, outPath)
  }


  /**
   * Performs an optimized self-join to compute and attach sender activity tiers to transactions.
   * Reuses a shared partitioner and caching to eliminate redundant shuffles during sender aggregation and joining.
   *
   * @param rddTx       parsed transactions RDD: (blockNumber, (from, valueGwei, gasPrice, isContractCall))
   * @param partitioner partitioner shared by transactions and per-sender tiers
   * @return transactions keyed by block: (blockNumber, (tier, valueGwei, gasPrice, isContractCall))
   */
  private def tierSelfJoinOptimized(
      rddTx: RDD[(Long, Tx)],
      partitioner: HashPartitioner
  ): RDD[(Long, (Int, Long, Long, Boolean))] = {

    val txByFrom = rddTx
      .map { case (block, (from, valueGwei, gasPrice, isContractCall)) =>
        (from, (block, valueGwei, gasPrice, isContractCall))
      }
      .partitionBy(partitioner)
      .persist(StorageLevel.MEMORY_AND_DISK)

    val tiers = txByFrom
      .mapValues(_ => 1L)
      .reduceByKey(partitioner, _ + _)
      .mapValues(tierOf)

    txByFrom
      .join(tiers)
      .map { case (_, ((block, valueGwei, gasPrice, isContractCall), tier)) =>
        (block, (tier, valueGwei, gasPrice, isContractCall))
      }
  }

  /**
   * Sorts results deterministically by key `(Date, Miner, IsContractCall, Tier)` into a single
   * partition, computes average gas price, and writes the output as a CSV file.
   *
   * @param rdd       aggregated RDD of `((date, miner, isContractCall, tier), (count, totalValueGwei, totalGasPrice))`
   * @param writeMode storage target mode ("local" or "remote")
   * @param outPath   relative destination directory for output CSV
   * @param spark     implicit active SparkSession
   */
  private def saveResults(
      rdd: RDD[(Key, (Long, Long, Long))],
      writeMode: String,
      outPath: String
  )(implicit spark: SparkSession): Unit = {
    import spark.implicits._

    val formattedRdd: RDD[(String, String, Boolean, Int, Long, Long, Double)] = rdd
      .sortByKey(ascending = true, numPartitions = 1)
      .map { case ((date, miner, isContractCall, tier), (count, totalVal, totalGasPrice)) =>
        val avgGasPrice = if (count > 0) totalGasPrice.toDouble / count else 0.0
        (date, miner, isContractCall, tier, count, totalVal, avgGasPrice)
      }

    formattedRdd
      .toDF("Date", "Miner", "IsContractCall", "Tier", "TxCount", "TotalValueGwei", "AvgGasPrice")
      .write
      .format("csv")
      .mode(SaveMode.Overwrite)
      .option("header", "true")
      .save(Commons.getDatasetPath(writeMode, outPath))
  }

}
