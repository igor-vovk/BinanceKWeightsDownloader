package com.ihorvovk.binance.archive_downloader

import com.typesafe.config.{Config, ConfigFactory}
import scalikejdbc.config._

object Dependencies {

  val conf: Config = ConfigFactory.load().getConfig("archive-downloader")

  DBs.setupAll()
  sys.addShutdownHook {
    DBs.closeAll()
  }

}
