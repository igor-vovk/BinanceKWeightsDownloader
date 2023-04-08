package com.ihorvovk.binance.archive_downloader.repository

import scalikejdbc._

case class BinanceBatchRow(id: Option[Int],
                           symbol: String,
                           interval: String,
                           year: Int,
                           month: Int,
                           uploadComplete: Boolean = false)

case class BinanceKLineRow(symbol: String,
                           openTime: Long,
                           openPrice: BigDecimal,
                           highPrice: BigDecimal,
                           lowPrice: BigDecimal,
                           closePrice: BigDecimal,
                           volume: BigDecimal,
                           closeTime: Long,
                           numberOfTrades: Long,
                           ignore: Boolean)

object KLinesRepository {
  private val binanceBatchMapper: WrappedResultSet => BinanceBatchRow = { rs =>
    BinanceBatchRow(
      id = Some(rs.get[Int]("id")),
      symbol = rs.get[String]("symbol"),
      interval = rs.get[String]("interval"),
      year = rs.get[Int]("year"),
      month = rs.get[Int]("month"),
      uploadComplete = rs.get[Boolean]("upload_complete")
    )
  }

  def findBatchBy(symbol: String,
                  interval: String,
                  year: Int,
                  month: Int): Option[BinanceBatchRow] =
    DB localTx { implicit sesssion =>
      sql"""
           |SELECT *
           |FROM k_lines_batches
           |WHERE
           |  `symbol` = $symbol AND
           |  `interval` = $interval AND
           |  `year` = $year AND
           |  `month` = $month
           |""".stripMargin.map(binanceBatchMapper).single.apply()
    }

  def upsertBatch(batch: BinanceBatchRow): BinanceBatchRow = {
    DB localTx { implicit session =>
      import batch._

      sql"""
           |INSERT IGNORE INTO k_lines_batches
           |  (symbol, `interval`, `year`, `month`, upload_complete)
           |VALUES
           |  ($symbol, $interval, $year, $month, $uploadComplete)
           |ON DUPLICATE KEY UPDATE
           |  upload_complete = $uploadComplete
       """.stripMargin.update.apply()
    }

    findBatchBy(batch.symbol, batch.interval, batch.year, batch.month).get
  }

  def insertKLines(rows: Seq[BinanceKLineRow], batchId: Option[Long]): Unit =
    DB localTx { implicit session =>
      val batchParams: Seq[Seq[Any]] = rows.map { row =>
        import row._

        Seq(symbol,
            batchId,
            openTime,
            closeTime,
            openPrice,
            highPrice,
            lowPrice,
            closePrice,
            volume,
            numberOfTrades,
            ignore)
      }

      sql"""
           |INSERT INTO `k_lines_5m` (
           |  symbol, batch_id, open_time, close_time, open_price, high_price, low_price, close_price,
           |  volume, num_of_trades, `ignore`
           |) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
           |""".stripMargin.batch(batchParams: _*).apply()
    }

  def removeKLinesByBatchId(batchId: Long): Int =
    DB localTx { implicit session =>
      sql"DELETE FROM `k_lines_5m` WHERE batch_id = $batchId".update.apply()
    }

}
