DROP TABLE IF EXISTS `symbols`;
DROP TABLE IF EXISTS `k_lines_batches`;
DROP TABLE IF EXISTS `k_lines_5m`;


CREATE TABLE `k_lines_5m` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `symbol` varchar(32) NOT NULL,
  `open_time` bigint(20) unsigned NOT NULL,
  `close_time` bigint(20) unsigned NOT NULL,
  `open_price` decimal(16,8) unsigned NOT NULL,
  `close_price` decimal(16,8) unsigned NOT NULL,
  `low_price` decimal(16,8) unsigned NOT NULL,
  `high_price` decimal(16,8) unsigned NOT NULL,
  `volume` decimal(16,8) unsigned NOT NULL,
  `num_of_trades` int(10) unsigned NOT NULL,
  `ignore` tinyint(1) unsigned NOT NULL DEFAULT 0,
  `batch_id` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `symbol` (`symbol`),
  KEY `batch` (`batch_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;


CREATE TABLE `k_lines_batches` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `symbol` varchar(32) NOT NULL DEFAULT '',
  `interval` varchar(32) NOT NULL DEFAULT '',
  `year` int(10) unsigned NOT NULL DEFAULT 0,
  `month` int(10) unsigned NOT NULL DEFAULT 0,
  `upload_complete` tinyint(1) unsigned NOT NULL DEFAULT 0,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uniq_fields` (`symbol`,`interval`,`year`,`month`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;


CREATE TABLE `symbols` (
  `symbol` varchar(32) NOT NULL DEFAULT '',
  PRIMARY KEY (`symbol`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
