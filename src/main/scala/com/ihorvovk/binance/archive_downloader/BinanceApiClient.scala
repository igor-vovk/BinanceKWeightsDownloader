package com.ihorvovk.binance.archive_downloader

import cats.effect._
import cats.implicits._
import com.ihorvovk.binance.archive_downloader.fs2utils.ZipCompression
import com.ihorvovk.binance.archive_downloader.repository.BinanceKLineRow
import fs2._
import org.http4s.Uri
import org.http4s.client.Client

object BinanceApiClient {
  case class Config(datasetsBaseUrl: String)

  case class KLinesRequest(symbol: String, interval: String, year: Int, month: Int)
}

class BinanceApiClient[F[_]: Async](client: Client[F], config: BinanceApiClient.Config) {

  import BinanceApiClient._

  private def parseBinanceKRow(symbol: String, row: Array[String]): BinanceKLineRow = {
    BinanceKLineRow(
      symbol = symbol,
      openTime = row(0).toLong,
      openPrice = BigDecimal(row(1)),
      highPrice = BigDecimal(row(2)),
      lowPrice = BigDecimal(row(3)),
      closePrice = BigDecimal(row(4)),
      volume = BigDecimal(row(5)),
      closeTime = row(6).toLong,
      numberOfTrades = row(8).toLong,
      ignore = row(11).toInt > 0
    )
  }

  def monthlyKLinesChecksum(req: KLinesRequest): F[Option[String]] = {
    val checksumUrl = mkArchiveUrl(req).toString() + ".CHECKSUM"

    client.get(checksumUrl) { r =>
      if (r.status.isSuccess) {
        r.body.through(text.utf8.decode).map(_.takeWhile(_ != ' ')).compile.last
      } else {
        Option.empty[String].pure[F]
      }
    }
  }

  def monthlyKLines(req: KLinesRequest): F[List[BinanceKLineRow]] = {
    client.get(mkArchiveUrl(req)) { r =>
      if (r.status.isSuccess) {
        r.body
          .through(ZipCompression.unzip[F]())
          .flatMap(_.content)
          .through(text.utf8.decode)
          .through(text.lines)
          .filter(_.nonEmpty)
          .map(_.split(",").map(_.trim))
          .map(parseBinanceKRow(req.symbol, _))
          .compile
          .toList
      } else {
        List.empty[BinanceKLineRow].pure[F]
      }
    }
  }

  private def mkArchiveUrl(r: KLinesRequest) = {
    import r._
    Uri.unsafeFromString(config.datasetsBaseUrl) / "spot" / "monthly" / "klines" /
      symbol / interval / f"$symbol-$interval-$year-$month%02d.zip"
  }

}
