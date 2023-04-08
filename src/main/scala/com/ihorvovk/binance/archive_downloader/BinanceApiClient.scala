package com.ihorvovk.binance.archive_downloader

import cats.effect.kernel.Async
import cats.implicits.catsSyntaxApplicativeId
import com.ihorvovk.binance.archive_downloader.fs2utils.ZipCompression
import com.ihorvovk.binance.archive_downloader.repository.BinanceKLineRow
import fs2.text
import org.http4s.client.Client
import org.http4s.{Request, Uri}

case class BinanceApiClientConfig(datasetsBaseUrl: String)

class BinanceApiClient[F[_]: Async](client: Client[F], config: BinanceApiClientConfig) {

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

  def monthlyKLines(symbol: String,
                    interval: String,
                    year: Int,
                    month: Int): F[List[BinanceKLineRow]] = {
    val url = Uri.unsafeFromString(config.datasetsBaseUrl) / "spot" / "monthly" / "klines" /
      symbol / interval / f"$symbol-$interval-$year-$month%02d.zip"

    client.run(Request(uri = url)).use { r =>
      if (r.status.isSuccess) {
        r.body
          .through(ZipCompression.unzip[F]())
          .flatMap(_.content)
          .through(text.utf8.decode)
          .through(text.lines)
          .map(_.split(",").map(_.trim))
          .map(parseBinanceKRow(symbol, _))
          .compile
          .toList
      } else {
        List.empty[BinanceKLineRow].pure[F]
      }
    }
  }

}
