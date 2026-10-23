package project

import java.time.{Instant, ZoneId}
import org.apache.spark.util.LongAccumulator

/**
 * Utility object providing parsing and data transformation methods
 * for Ethereum blockchain datasets (blocks and transactions).
 */
object EthereumParser {

  type Tx = (String, Long, Long, Boolean)

  private final val BlockNumberIdx: Int = 0
  private final val BlockMinerIdx: Int = 9
  private final val BlockTimestampIdx: Int = 16
  private final val MinBlockCols: Int = 17

  private final val TxBlockNumberIdx: Int = 3
  private final val TxFromIdx: Int = 5
  private final val TxValueIdx: Int = 7
  private final val TxGasPriceIdx: Int = 9
  private final val TxInputIdx: Int = 10
  private final val MinTxCols: Int = 12

  private final val WeiPerGwei: Long = 1000000000L

  /**
   * Converts a UNIX timestamp in seconds to a calendar date string (YYYY-MM-DD) in UTC.
   *
   * @param timestamp the UNIX epoch timestamp string in seconds
   * @return formatted date string in "YYYY-MM-DD" format
   */
  def timestampToDay(timestamp: String): String = {
    val ts = timestamp.trim.toLong
    Instant.ofEpochSecond(ts)
      .atZone(ZoneId.of("UTC"))
      .toLocalDate
      .toString
  }

  /**
   * Parses a raw CSV line from the Ethereum blocks dataset.
   *
   * @param line the raw CSV record representing a block
   * @param malformedAcc accumulator tracking the count of malformed records
   * @return [[Some]] containing `(blockNumber, (dateString, minerAddress))` if parsing succeeds,
   *         or [[None]] if the line is empty, a header row, or malformed
   */
  def parseBlock(line: String, malformedAcc: LongAccumulator): Option[(Long, (String, String))] = {
    if (line.isEmpty || line.startsWith("number,")) {
      None
    } else {
      try {
        val cols = line.split(",", -1)
        if (cols.length < MinBlockCols) {
          malformedAcc.add(1L)
          None
        } else {
          val number = cols(BlockNumberIdx).trim.toLong
          val miner = cols(BlockMinerIdx).trim.toLowerCase.intern()
          val date = timestampToDay(cols(BlockTimestampIdx)).intern()
          Some((number, (date, miner)))
        }
      } catch {
        case _: Exception =>
          malformedAcc.add(1L)
          None
      }
    }
  }

  /**
   * Parses a raw CSV line from the Ethereum transactions dataset.
   *
   * @param line the raw CSV record representing a transaction
   * @param malformedAcc accumulator tracking the count of malformed records
   * @return [[Some]] containing `(blockNumber, (fromAddress, valueInGwei, gasPrice, isContractCall))` if parsing succeeds,
   *         or [[None]] if the line is empty, a header row, or malformed
   */
  def parseTransaction(
      line: String,
      malformedAcc: LongAccumulator
  ): Option[(Long, Tx)] = {
    if (line.isEmpty || line.startsWith("hash,")) {
      None
    } else {
      try {
        val cols = line.split(",", -1)
        if (cols.length < MinTxCols) {
          malformedAcc.add(1L)
          None
        } else {
          val blockNumber = cols(TxBlockNumberIdx).trim.toLong
          val from = cols(TxFromIdx).trim.toLowerCase.intern()
          val valueWei = BigInt(cols(TxValueIdx).trim)
          val valueGwei = (valueWei / WeiPerGwei).toLong
          val gasPrice = cols(TxGasPriceIdx).trim.toLong
          val input = cols(TxInputIdx).trim
          val isContractCall = input.nonEmpty && input != "0x"

          Some((blockNumber, (from, valueGwei, gasPrice, isContractCall)))
        }
      } catch {
        case _: Exception =>
          malformedAcc.add(1L)
          None
      }
    }
  }
}