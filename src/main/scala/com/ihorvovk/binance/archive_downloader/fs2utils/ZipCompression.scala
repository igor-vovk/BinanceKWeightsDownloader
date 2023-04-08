package com.ihorvovk.binance.archive_downloader.fs2utils

import cats.effect._
import cats.implicits._
import fs2._
import fs2.io._

import java.util.zip.ZipInputStream

object ZipCompression {

  case class UnzipResult[F[_]](content: Stream[F, Byte], filename: String)

  // Implementation supports unpacking only 1-file archives, which is ok for the purposes of this project
  def unzip[F[_]: Async](): Pipe[F, Byte, UnzipResult[F]] = { stream =>
    stream
      .through(toInputStream)
      .map(is => new ZipInputStream(is))
      .map { zis =>
        val entry = zis.getNextEntry
        val res = UnzipResult(
          content = readInputStream[F](zis.pure[F].widen, 16 * 1024, closeAfterUse = true),
          filename = entry.getName
        )

        res
      }

  }

}
