package com.ihorvovk.binance.archive_downloader

import cats.effect._

object ArchiveDownloader extends IOApp.Simple {
  private val task = Dependencies.archiveDownloadTask[IO]

  val run: IO[Unit] = task.run()

}
