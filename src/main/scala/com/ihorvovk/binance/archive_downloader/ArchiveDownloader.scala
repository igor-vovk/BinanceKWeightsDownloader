package com.ihorvovk.binance.archive_downloader

import cats.effect._
import cats.effect.implicits._
import com.ihorvovk.binance.archive_downloader.repository._
import net.ceedubs.ficus.Ficus._
import org.slf4j.LoggerFactory

import java.io.FileNotFoundException
import java.net.URL
import java.time.{LocalDate, Period}
import java.util.zip.ZipInputStream
import scala.io.Source
import scala.jdk.CollectionConverters._

object ArchiveDownloader extends IOApp.Simple {
  private val log = LoggerFactory.getLogger(getClass)

  val run: IO[Unit] = {
    val batches = for {
      symbol <- Dependencies.conf.as[Seq[String]]("symbols")
      interval <- Seq("5m")
      date <- rangeInMonths(
        from = Dependencies.conf.as[LocalDate]("download_from_date"),
        until = LocalDate.now()
      )
    } yield BinanceBatchRow(None, symbol, interval, date.getYear, date.getMonthValue)

    batches.zipWithIndex
      .filterNot {
        case (b, _) =>
          KLinesRepository
            .findBatchBy(b.symbol, b.interval, b.year, b.month)
            .exists(_.uploadComplete)
      }
      .parTraverseN(6) {
        case (batch, index) =>
          log.info(s"Step ${index + 1}/${batches.size}. Processing batch $batch")

          loadZippedCsv[IO](mkBinanceArchiveUrl(batch))
            .use { iter =>
              IO.delay {
                iter.map { cols =>
                  BinanceKLineRow(
                    symbol = batch.symbol,
                    openTime = cols(0).toLong,
                    openPrice = BigDecimal(cols(1)),
                    highPrice = BigDecimal(cols(2)),
                    lowPrice = BigDecimal(cols(3)),
                    closePrice = BigDecimal(cols(4)),
                    volume = BigDecimal(cols(5)),
                    closeTime = cols(6).toLong,
                    numberOfTrades = cols(8).toLong,
                    ignore = cols(11).toInt > 0
                  )
                }.toSeq
              }
            }
            .map { kLines =>
              val batchId = KLinesRepository.upsertBatch(batch).id.get
              log.info(s"Batch ID: $batchId. Removing existing records...")

              KLinesRepository.removeKLinesByBatchId(batchId)
              log.info(s"Inserting new records...")

              KLinesRepository.insertKLines(kLines, batchId = Some(batchId))
              log.info(s"Upload complete. Inserted ${kLines.length} klines.")

              KLinesRepository.upsertBatch(batch.copy(uploadComplete = true))
            }
            .attempt
            .flatMap {
              case Right(_) => IO.unit
              case Left(e: FileNotFoundException) =>
                IO.delay(log.warn(s"Failed to download the file, skipping: ${e.getMessage}"))
              case Left(other) => IO.raiseError(other)
            }
      }
      .map { _ =>
        log.info("Done")
      }
  }

  def mkBinanceArchiveUrl(batch: BinanceBatchRow): URL = {
    import batch._
    val s = f"https://data.binance.vision/data/spot/monthly/klines" +
      f"/$symbol/$interval/$symbol-$interval-$year-$month%02d.zip"
    new URL(s)
  }

  def loadZippedCsv[F[_]: Sync](url: URL): Resource[F, Iterator[Array[String]]] = {
    log.info(s"Loading $url")

    for {
      is <- Resource.fromAutoCloseable(Sync[F].blocking(url.openStream()))
      zis <- Resource.fromAutoCloseable(Sync[F].blocking(new ZipInputStream(is)))
      _ = zis.getNextEntry // Hack to force ZipInputStream to read the first file
      source <- Resource.fromAutoCloseable(Sync[F].blocking(Source.createBufferedSource(zis)))
    } yield {
      source.getLines().map { line =>
        line.split(",").map(_.trim)
      }
    }
  }

  def rangeInMonths(from: LocalDate, until: LocalDate): Seq[LocalDate] = {
    from.datesUntil(until, Period.ofMonths(1)).iterator().asScala.toSeq
  }

}
