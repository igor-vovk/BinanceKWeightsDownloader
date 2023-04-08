package com.ihorvovk.binance.archive_downloader

import cats.effect._
import cats.implicits._
import com.ihorvovk.binance.archive_downloader.BinanceApiClient.KLinesRequest
import com.ihorvovk.binance.archive_downloader.repository.{BinanceBatchRow, KLinesRepository}
import org.slf4j.LoggerFactory

import java.time.{LocalDate, Period}
import scala.jdk.CollectionConverters._
import fs2.Stream

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

    Stream
      .emits[F, BinanceBatchRow](batches)
      .zipWithIndex
      .filterNot {
        case (b, _) =>
          KLinesRepository
            .findBatchBy(b.symbol, b.interval, b.year, b.month)
            .exists(_.uploadComplete)
      }
      .mapAsyncUnordered(6) {
        case (batch, i) =>
          binanceApiClient
            .monthlyKLines(KLinesRequest(batch.symbol, batch.interval, batch.year, batch.month))
            .flatMap {
              case klines @ _ :: _ =>
                Async[F].blocking {
                  log.info(s"Step ${i + 1} / ${batches.size}. Processing batch $batch")

                  val batchId = KLinesRepository.upsertBatch(batch).id.get
                  KLinesRepository.removeKLinesByBatchId(batchId)

                  KLinesRepository.insertKLines(klines, batchId = Some(batchId))

                  KLinesRepository.upsertBatch(batch.copy(uploadComplete = true))
                  log.info(s"Upload complete. Inserted ${klines.length} klines.")
                }
              case _ =>
                log
                  .warn(s"Step ${i + 1} / ${batches.size}. Failed to download the file, skipping")
                  .pure[F]
            }
      }
      .compile
      .drain
      .map { _ =>
        log.info("Done")
      }
  }

  private def rangeInMonths(from: LocalDate, until: LocalDate): Seq[LocalDate] = {
    from.datesUntil(until, Period.ofMonths(1)).iterator().asScala.toSeq
  }

}
