package com.ihorvovk.binance.archive_downloader

import cats.effect.kernel.Async
import com.typesafe.config.{Config, ConfigFactory}
import net.ceedubs.ficus.Ficus._
import org.http4s.client.{Client, JavaNetClientBuilder}
import scalikejdbc.config._

import java.time.LocalDate

object Dependencies {

  val config: Config = ConfigFactory.load().getConfig("archive-downloader")

  DBs.setupAll()
  sys.addShutdownHook {
    DBs.closeAll()
  }

  def client[F[_]: Async]: Client[F] = {
    JavaNetClientBuilder[F].create
  }

  def binanceApiClient[F[_]: Async]: BinanceApiClient[F] = {
    new BinanceApiClient[F](
      client = client,
      config = BinanceApiClient.Config(
        datasetsBaseUrl = config.as[String]("binance.base_url")
      )
    )
  }

  def archiveDownloadTask[F[_]: Async]: ArchiveDownloadTask[F] = {
    new ArchiveDownloadTask[F](
      binanceApiClient[F],
      ArchiveDownloadTask.Config(
        symbols = config.as[Seq[String]]("symbols"),
        loadFromDate = config.as[LocalDate]("download_from_date"),
        loadToDate = LocalDate.now()
      )
    )
  }

}
