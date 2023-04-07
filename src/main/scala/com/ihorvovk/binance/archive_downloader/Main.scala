package com.ihorvovk.binance.archive_downloader

import cats.effect._
import cats.implicits._
import com.ihorvovk.binance.archive_downloader.repository._
import org.slf4j.LoggerFactory

import java.io.FileNotFoundException
import java.net.URL
import java.util.zip.ZipInputStream
import scala.io.Source
import scala.jdk.CollectionConverters._

object Main extends IOApp.Simple {
  private val log = LoggerFactory.getLogger(getClass)

  val run: IO[Unit] = {
    val batches = for {
      symbol <- Dependencies.conf.getStringList("archive-downloader.symbols").asScala.toList
      interval <- Seq("5m")
      year <- Seq("2021", "2022", "2023")
      month <- Seq("01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12")
    } yield BinanceBatch(None, symbol, interval, year, month)

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
          }.toSeq

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

    val s = s"https://data.binance.vision/data/spot/monthly/klines/$symbol/$interval/$symbol-$interval-$year-$month.zip"
    new URL(s)
  }


  def loadZippedCsv[F[_] : Sync](url: URL): Resource[F, Iterator[Array[String]]] = {
    for {
      is <- Resource.fromAutoCloseable(Sync[F].delay(url.openStream()))
      zis <- Resource.fromAutoCloseable(Sync[F].delay(new ZipInputStream(is)))
      source <- Resource.fromAutoCloseable(Sync[F].delay(Source.createBufferedSource(zis)))
    } yield {
      source.getLines().map { line =>
        line.split(",").map(_.trim)
      }
    }
  }

}