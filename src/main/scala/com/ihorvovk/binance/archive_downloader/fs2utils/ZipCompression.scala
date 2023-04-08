package com.ihorvovk.binance.archive_downloader.fs2utils

import cats.effect._
import cats.implicits._
import fs2._
import fs2.io._

import java.util.zip.ZipInputStream

object ZipCompression {

  case class UnzipResult[F[_]](content: Stream[F, Byte], filename: String)

  def unzip[F[_]: Async](): Pipe[F, Byte, UnzipResult[F]] = { stream =>
    stream
      .through(toInputStream)
      .flatMap(is => Stream.fromAutoCloseable(Async[F].delay(new ZipInputStream(is))))
      .flatMap(Stream.unfold(_) { zis =>
        Option(zis.getNextEntry).map { entry =>
          val res = UnzipResult(
            content = readInputStream[F](zis.pure[F].widen, 16, closeAfterUse = false),
            filename = entry.getName
          )

          (res, zis)
        }
      })

  }

}
