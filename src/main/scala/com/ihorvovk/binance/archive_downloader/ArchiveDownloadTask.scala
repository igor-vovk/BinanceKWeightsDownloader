package com.ihorvovk.binance.archive_downloader

import cats.effect._
import cats.effect.implicits._
import cats.implicits._
import com.ihorvovk.binance.archive_downloader.repository.{BinanceBatchRow, KLinesRepository}
import org.slf4j.LoggerFactory

import java.time.{LocalDate, Period}
import scala.jdk.CollectionConverters._

object ArchiveDownloadTask {
  case class Config(
      symbols: Seq[String],
      loadFromDate: LocalDate,
      loadToDate: LocalDate
  )
}

class ArchiveDownloadTask[F[_]: Async](binanceApiClient: BinanceApiClient[F],
                                       config: ArchiveDownloadTask.Config) {

  private val log = LoggerFactory.getLogger(getClass)

  def run(): F[Unit] = {
    val batches = for {
      symbol <- config.symbols
      interval <- Seq("5m")
      date <- rangeInMonths(config.loadFromDate, config.loadToDate)
    } yield BinanceBatchRow(None, symbol, interval, date.getYear, date.getMonthValue)

    batches.zipWithIndex
      .filterNot {
        case (b, _) =>
          KLinesRepository
            .findBatchBy(b.symbol, b.interval, b.year, b.month)
            .exists(_.uploadComplete)
      }
      .parTraverseN(5) {
        case (batch, i) =>
          binanceApiClient
            .monthlyKLines(batch.symbol, batch.interval, batch.year, batch.month)
            .map { res =>
              (batch, res, i)
            }
      }
      .flatMap(_.parTraverseN(6) {
        case (batch, kLines, i) =>
          if (kLines.nonEmpty) {
            Async[F].blocking {
              log.info(s"Step ${i + 1} / ${batches.size}. Processing batch $batch")

              val batchId = KLinesRepository.upsertBatch(batch).id.get
              KLinesRepository.removeKLinesByBatchId(batchId)

              KLinesRepository.insertKLines(kLines, batchId = Some(batchId))

              KLinesRepository.upsertBatch(batch.copy(uploadComplete = true))
              log.info(s"Upload complete. Inserted ${kLines.length} klines.")
            }
          } else {
            log.warn(s"Failed to download the file for batch $batch, skipping").pure[F]
          }
      })
      .map { _ =>
        log.info("Done")
      }
  }

  private def rangeInMonths(from: LocalDate, until: LocalDate): Seq[LocalDate] = {
    from.datesUntil(until, Period.ofMonths(1)).iterator().asScala.toSeq
  }

}
