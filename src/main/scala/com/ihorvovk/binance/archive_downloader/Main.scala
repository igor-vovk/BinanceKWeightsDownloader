package com.ihorvovk.binance.archive_downloader

import cats.effect._
import cats.implicits._
import com.ihorvovk.binance.archive_downloader.repository._
import org.slf4j.LoggerFactory

import java.io.FileNotFoundException
import java.net.URL
import java.time.{LocalDate, Period}
import java.util.zip.ZipInputStream
import scala.io.Source
import scala.jdk.CollectionConverters._

object Main extends IOApp.Simple {
  private val log = LoggerFactory.getLogger(getClass)

  val run: IO[Unit] = {
    val batches = for {
      symbol <- Dependencies.conf.getStringList("archive-downloader.symbols").asScala.toList
      interval <- Seq("5m")
      date <- rangeInMonths(
        from = LocalDate.parse(Dependencies.conf.getString("archive-downloader.download_from_date")),
        until = LocalDate.now()
      )
    } yield BinanceBatch(None, symbol, interval, date.getYear, date.getMonthValue)

    batches
      .zipWithIndex
      .filterNot { case (b, _) =>
        KLinesRepository.findBatchBy(b.symbol, b.interval, b.year, b.month).exists(_.uploadComplete)
      }
      .traverse { case (batch, index) =>
        log.info(s"Step ${index + 1}/${batches.size}. Processing batch $batch")

        val resource = for {
          rows <- loadZippedCsv[IO](mkBinanceArchiveUrl(batch))
        } yield {
          val kLines = rows.map { cols =>
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
          }

          val batchId = KLinesRepository.upsertBatch(batch).id.get
          log.info(s"Batch ID: $batchId. Removing existing records...")

          KLinesRepository.removeKLinesByBatchId(batchId)
          log.info(s"Inserting new records...")

          KLinesRepository.insertKLines(kLines, batchId = Some(batchId))
          log.info(s"Upload complete. Inserted ${kLines.length} klines.")

          KLinesRepository.upsertBatch(batch.copy(uploadComplete = true))
        }

        resource.attempt.allocated.flatMap { case (ttry, close) =>
          close *> (ttry match {
            case Right(_) => IO.unit
            case Left(e: FileNotFoundException) => IO.delay(log.warn(s"Failed to download the file, skipping: ${e.getMessage}"))
            case Left(other) => IO.raiseError(other)
          })
        }
      }
      .map { _ =>
        log.info("Done")
      }
  }


  def mkBinanceArchiveUrl(batch: BinanceBatch): URL = {
    import batch._

    val s = f"https://data.binance.vision/data/spot/monthly/klines/$symbol/$interval/$symbol-$interval-$year-$month%02d.zip"
    new URL(s)
  }


  def loadZippedCsv[F[_] : Sync](url: URL): Resource[F, Seq[Array[String]]] = {
    log.info(s"Loading $url")

    for {
      is <- Resource.fromAutoCloseable(Sync[F].delay(url.openStream()))
      zis <- Resource.fromAutoCloseable(Sync[F].delay(new ZipInputStream(is)))
      _ = zis.getNextEntry // Hack to force ZipInputStream to read the first file
      source <- Resource.fromAutoCloseable(Sync[F].delay(Source.createBufferedSource(zis)))
    } yield {
      source.getLines().map { line =>
        line.split(",").map(_.trim)
      }.toSeq
    }
  }

  def rangeInMonths(from: LocalDate, until: LocalDate): Seq[LocalDate] = {
    from.datesUntil(until, Period.ofMonths(1)).iterator().asScala.toSeq
  }

}