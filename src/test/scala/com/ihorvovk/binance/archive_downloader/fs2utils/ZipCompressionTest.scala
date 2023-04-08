package com.ihorvovk.binance.archive_downloader.fs2utils

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.effect.unsafe.implicits.global
import fs2.io.file.{Files, Path}
import fs2.text
import org.scalatest.flatspec.{AnyFlatSpec, AsyncFlatSpec}
import org.scalatest.matchers.must.Matchers

class ZipCompressionTest extends AnyFlatSpec with Matchers {

  private val testArchivePath = Path(getClass.getClassLoader.getResource("testfile.zip").getPath)

  behavior of "ZipCompression"

  it should "unzip archive" in {
    Files[IO]
      .exists(testArchivePath)
      .unsafeRunSync() must be(true)

    Files[IO]
      .readAll(testArchivePath)
      .through(ZipCompression.unzip[IO]())
      .map(_.filename)
      .compile
      .string
      .unsafeRunSync() must be("testfile.txt")

    Files[IO]
      .readAll(testArchivePath)
      .through(ZipCompression.unzip[IO]())
      .flatMap(_.content.through(text.utf8.decode))
      .compile
      .string
      .unsafeRunSync() must be("This is test message")
  }

}
