-- MySQL dump 10.13  Distrib 5.5.24, for Linux (x86_64)
--
-- Host: localhost    Database: db_airflow
-- ------------------------------------------------------
-- Server version	5.5.24-1.3-log

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `alembic_version`
--

DROP TABLE IF EXISTS `alembic_version`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `alembic_version` (
  `version_num` varchar(32) NOT NULL,
  PRIMARY KEY (`version_num`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `app_log`
--

DROP TABLE IF EXISTS `app_log`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `app_log` (
  `id` bigint(11) NOT NULL AUTO_INCREMENT,
  `dag_id` varchar(250) DEFAULT NULL COMMENT 'DAG Id',
  `execution_date` datetime DEFAULT NULL COMMENT 'DAG调度时间',
  `task_id` varchar(250) DEFAULT NULL COMMENT '任务Id',
  `create_time` datetime DEFAULT NULL COMMENT '日志记录时间',
  `create_user` varchar(64) DEFAULT NULL COMMENT '日志调用用户',
  `log_category` varchar(256) DEFAULT NULL COMMENT '日志类别',
  `log_level` varchar(64) DEFAULT NULL COMMENT '日志级别',
  `message` mediumblob COMMENT '日志内容',
  PRIMARY KEY (`id`),
  KEY `dag_id` (`dag_id`),
  KEY `execution_date` (`execution_date`),
  KEY `task_id` (`task_id`),
  KEY `create_time` (`create_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `celery_taskmeta`
--

DROP TABLE IF EXISTS `celery_taskmeta`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `celery_taskmeta` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `task_id` varchar(155) DEFAULT NULL,
  `status` varchar(50) DEFAULT NULL,
  `result` blob,
  `date_done` datetime DEFAULT NULL,
  `traceback` text,
  PRIMARY KEY (`id`),
  UNIQUE KEY `task_id` (`task_id`)
) ENGINE=InnoDB AUTO_INCREMENT=35933 DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `celery_tasksetmeta`
--

DROP TABLE IF EXISTS `celery_tasksetmeta`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `celery_tasksetmeta` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `taskset_id` varchar(155) DEFAULT NULL,
  `result` blob,
  `date_done` datetime DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `taskset_id` (`taskset_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `chart`
--

DROP TABLE IF EXISTS `chart`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `chart` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `label` varchar(200) DEFAULT NULL,
  `conn_id` varchar(250) NOT NULL,
  `user_id` int(11) DEFAULT NULL,
  `chart_type` varchar(100) DEFAULT NULL,
  `sql_layout` varchar(50) DEFAULT NULL,
  `sql` text,
  `y_log_scale` tinyint(1) DEFAULT NULL,
  `show_datatable` tinyint(1) DEFAULT NULL,
  `show_sql` tinyint(1) DEFAULT NULL,
  `height` int(11) DEFAULT NULL,
  `default_params` varchar(5000) DEFAULT NULL,
  `x_is_date` tinyint(1) DEFAULT NULL,
  `iteration_no` int(11) DEFAULT NULL,
  `last_modified` datetime DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `user_id` (`user_id`),
  CONSTRAINT `chart_ibfk_1` FOREIGN KEY (`user_id`) REFERENCES `users` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `chart`
--

INSERT INTO `chart` VALUES (1,'Airflow task instance by type','airflow_db',NULL,'bar','series','SELECT state, COUNT(1) as number FROM task_instance WHERE dag_id LIKE \'example%\' GROUP BY state',NULL,NULL,1,600,'{}',0,0,'2018-08-18 09:37:34');

--
-- Table structure for table `connection`
--

DROP TABLE IF EXISTS `connection`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `connection` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `conn_id` varchar(250) DEFAULT NULL,
  `conn_type` varchar(500) DEFAULT NULL,
  `host` varchar(500) DEFAULT NULL,
  `schema` varchar(500) DEFAULT NULL,
  `login` varchar(500) DEFAULT NULL,
  `password` varchar(500) DEFAULT NULL,
  `port` int(11) DEFAULT NULL,
  `extra` varchar(5000) DEFAULT NULL,
  `is_encrypted` tinyint(1) DEFAULT NULL,
  `is_extra_encrypted` tinyint(1) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `connection`
--

INSERT INTO `connection` VALUES (1,'airflow_db','mysql','localhost','airflow','root',NULL,NULL,NULL,0,0),(2,'airflow_ci','mysql','localhost','airflow_ci','root',NULL,NULL,'gAAAAABau3qp39qB7EjoFKt_parGdhPRwxtu39O4pi2w2osFy7PJv9ZBoh22yG0OTlvDmKho3H16fNcV3bChR2WY5taoeYXIKat4GbCjWELXxB4HC8KVPMg=',0,1),(3,'beeline_default','beeline','localhost','default',NULL,NULL,10000,'gAAAAABau3qpEw4GhqbFoHkCOgHtdnqg5yGNWqeeCH7MAg7UB-rA7GC6cXo4BND4nPuvkQusTolHDDX_NwcUL2mCwmq4VpNqsBXhpPK29lVEIdqJRpTp0a6hlYEh_kPThS-Ed8E_CW-X',0,1),(4,'bigquery_default','bigquery',NULL,NULL,NULL,NULL,NULL,NULL,0,0),(5,'local_mysql','mysql','localhost','airflow','airflow','gAAAAABau3qpfCzT-L6cWKNoxEuUbKpwvlOLuYgx3obwRAEbo-c3S-LECx-4Jtj1HC09L9yqeEmz_93WsIIKVOfSD6baARyciw==',NULL,NULL,1,0),(6,'presto_default','presto','localhost','hive',NULL,NULL,3400,NULL,0,0),(7,'hive_cli_default','hive_cli',NULL,'default',NULL,NULL,NULL,NULL,0,0),(8,'hiveserver2_default','hiveserver2','localhost','default',NULL,NULL,10000,NULL,0,0),(9,'metastore_default','hive_metastore','localhost',NULL,NULL,NULL,9083,'gAAAAABau3qpCuLTEO8k-Y6813N7FOfsWTBhqNntlffQsXbA1A7ddgSoD7hR5oibmgFR5ttNYxICGB5hTG09wvXwk6JLa7vpKO0rvyxZRv3pciRIZZJXoC4=',0,1),(10,'mysql_default','mysql','localhost',NULL,'root',NULL,NULL,NULL,0,0),(11,'postgres_default','postgres','localhost','airflow','postgres',NULL,NULL,NULL,0,0),(12,'sqlite_default','sqlite','/tmp/sqlite_default.db',NULL,NULL,NULL,NULL,NULL,0,0),(13,'http_default','http','https://www.google.com/',NULL,NULL,NULL,NULL,NULL,0,0),(14,'mssql_default','mssql','localhost',NULL,NULL,NULL,1433,NULL,0,0),(15,'vertica_default','vertica','localhost',NULL,NULL,NULL,5433,NULL,0,0),(16,'webhdfs_default','hdfs','localhost',NULL,NULL,NULL,50070,NULL,0,0),(17,'ssh_default','ssh','localhost',NULL,NULL,NULL,NULL,NULL,0,0),(18,'fs_default','fs',NULL,NULL,NULL,NULL,NULL,'gAAAAABau3qpdB7AYb2FdYzRpVXtVPlvA2DuX20nwXRkp5fx8WVffd7PFuUT0mekY4Z1q_QsyO9j9p_pfUgbx8yEUh7QXGV5qg==',0,1),(19,'aws_default','aws',NULL,NULL,NULL,NULL,NULL,'gAAAAABau3qpbklVw2wTngC6YvUqdzIzh2iP6Zxca9qDvMuWRykCTSLvdG4sqD_Gj9VokWVORH4VZVKGDhGlRYQzeMr51T_vSLt2Sssyjn6RCyA6l_iW9z0=',0,1),(20,'spark_default','spark','yarn',NULL,NULL,NULL,NULL,'gAAAAABau3qpCKO4Lln7FxJo3RkDNN0OpGe1u1oZOwqKUIIwZQ1dX_1Er1CAqtR3u-_Qg9K7YCkDeWwtsqtaeT7v7BJiD95RFRAvEECp3dfRwPy3AtDg_pk=',0,1),(21,'emr_default','emr',NULL,NULL,NULL,NULL,NULL,'gAAAAABau3qptRsAr-6a0q77ZOaUeyOgZFGyT5LFBambXn-cxcPKmDRua_n7rMCVPG3nO8iz6KEgHL6tKo7gmp5kOC8Dl8h9RmbolpgsappRX1lDQWA6HqNIQk9mkBtg4W0ZfiBfNiUVul1kzglZ1gSljB9XBRwzhKRUwFsC69DJYc35NSmc7CKOgfIK24c13vMJNmiYm6BV4uekLaOuXOkIQY8wZg6J0aPXScb8OBM4v5n2sYLGpoh9Cnz-Bt-7yt8aqbATRbefK__qVI5q1_kMyZAcD6MFHBWfKTYpJFzkzMoP-o21zyCUmHtMNGXUrcVTgYgUwI0Teyd8IOuG7AnObGP9QgY70ju5rBcoCM_R-w1wRlBXqBjy3Ky0y89Lz8whK--24fuZ7gZLAmK1hs1f62uKnUACbbpEKyKQxh2iqaRx4WtW6fu63iHBom6Zp0Tjegc6LBw6yqQmYKs03vD29jRjHGfLShGvGUhzWWiNYfbiL8Wpg1cRRKeGBiQFzTDN6lWjW1SS_ipCmadotGJbkIPhQ_QnGcrPZCpaZkZdQWOCmCBR7c2G0BhcZ5xEbxg0N0fHDqPNvgrBA_61ritMZbJYVq1p0qY1pwOCtri-l-CrjKq_6QnlvVxNv0mb_JFwmSWvXlT8TRDYDkXP-590SqVjER68S2sSug84e9u1kliAY9PuRtT-WyD7K0UBcA8GqtRGPLRbbR5QZ0Ztr4uahOHuiKxrxRNlVXJTLj--8QvEALuMqV4Ke8IF0phDDD8L02edr63KnVP-EKsL8B8yEDLqIsyAJC_sm6Z5LpYldPoxPNMf0fHJqbYXPtE4ZD8WIvcKazvP3SH9Rq-Ce8cenb6pYOzt3kZUfhKPwu5Q-HBX6C4EttOrRbzilXgRqesN8ei5JhcokTZABeTKUaQwibqdOaYPGlg0ImBYebpMNNGxA9ReqDZXv7gLWSY0If1-Y3sgAD1-2agMxXIrmK-ftn457Wo31YB6fzhsJh6LwTnAAJ2kGE5-BWxRGzmAvyp46cVF_owApoU11KceetZv8_m0xOIkkwwg_dpVZaB7oBCWsD2AKk4YWFLfFDRxpJhV_Wr4LU_lSwTRkxR9vsiRIy5Lf3n8oNFBIhryVaRUaCjYEfN2oPmQVxstDg7FsGra8EaOlPhxf4MkEh8yLg6IqZoH6THS11UxshxB2I2h1lrs53wFe6EYgCA9XNjs_XISAJ12ToWqj5GlHQH2YwnqvuU_YBjxlkmeV3b3orC_g9-sNYE5Ksj09tQBvM1565_W9Ff6kown-6HPLbbCScKoiEj5rmKsc3xEy51XQ32BpmvoK8MgQz0tkQRrOvi6zf3Gnbu1T6a1xUbJLqFE6xarYS6GuiHH2Zsw5yN4L686WJa-btrGI0fjpdWOLq2Ri0lqEzjqtdiqVTjb__7MtI1cev__4uIJ4UhKI3FNjcJVXkCxQlkTRnOeY7fm3L7qj1e7KiYmsWICxTtRI7qNJb_K2yYCQwej9NV-6-MaVZ63mKdUoR2Unts8FHIiHtbO5_4VSSlAHPxcYS4oI5MlUUJLeokDyNLIB4tYhi2jwBzxFq-hCAWi5xpkguyjLj362lNNpHMj4dw8MuJZkezpFXfhvPtd3apMKHcgbsmAX1DbGX7CpU_QPv9aU7kp1LcR69w_x4IEAKGeWLrAR4WYkps-LZ_c_eIVyp8M-sQt8yEdyPF3YZc-ZFRL8jqC8JRaQy7aVi-irodSQ1bGGqWzbVVojNPrxF0fM8Wd307SnomninC8H7-qYl_FRvcwN740aCz6oqkuTog8DTl_khG_2tBnyZs9rc4I4Ilg_iKQvB1_nSRCrDuMsdVy1GFmtCmioWsk0o9EvT7ZKHDN3lf2CXCi6tGVGlJ2nrANUkI6Ie8szRlFm-1Y7x94Jf-WpeiPyE_G5EWVlw1qNIo-1Sv_m5PSagcldf9ld5yxHyJbreI3j-iOXmfgHx91iWVAlN871XNeo6L9AAtalC7Y10Nc4MhJRLbZcQhRxSrVuzL0xb-0RLiPRtJiCknbuO37LOrCxuXA52nhTlmTlG2oEZyB2ifP-VH7Nss4EbODw0XHJraF3SxTo92m2BZtMeTBdbmMASc3Q0QveYgeXR-8dtsiHgxx0s1yM0aVtVlBakCvqejMNQ0zBzlsXsxkba3vAk3prQvhJXETyTZyUjDIIiGk_GJh1jfzvLKWJQ7snwr_BnLDfjlRRCDWj1mW6Dkw5jSYQhfrlV4F1vDLyDUAAuXTLMw6HwvwdnXFknG4o_sDnB3zUgMaAOJxZJrC2OzLRf445j2JYQuy-isnfx6tus3ErjZOJU-zELRTKv_KjreueKPw58TbWHjTRAsE9YwZItfEHhllh-nJG3EADtcuMHUf8jkkWm3c3wVaXmlrFuAQKYx3evr7lsPTIDE8zhVFODK9UC4QivGUJy-js0r2Mfjdm5ZlR6aWnv_XdcrLBZUiBuHPKh4iAIEZxGBA64R45GmtAhDX4fsObbVD0frW8z5RHRzV6gLAecUlhg==',0,1);

--
-- Table structure for table `dag`
--

DROP TABLE IF EXISTS `dag`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `dag` (
  `dag_id` varchar(250) NOT NULL,
  `is_paused` tinyint(1) DEFAULT NULL,
  `is_subdag` tinyint(1) DEFAULT NULL,
  `is_active` tinyint(1) DEFAULT NULL,
  `last_scheduler_run` datetime DEFAULT NULL,
  `last_pickled` datetime DEFAULT NULL,
  `last_expired` datetime DEFAULT NULL,
  `scheduler_lock` tinyint(1) DEFAULT NULL,
  `pickle_id` int(11) DEFAULT NULL,
  `fileloc` varchar(2000) DEFAULT NULL,
  `owners` varchar(2000) DEFAULT NULL,
  PRIMARY KEY (`dag_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `dag_pickle`
--

DROP TABLE IF EXISTS `dag_pickle`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `dag_pickle` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `pickle` blob,
  `created_dttm` datetime DEFAULT NULL,
  `pickle_hash` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `dag_run`
--

DROP TABLE IF EXISTS `dag_run`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `dag_run` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `dag_id` varchar(250) DEFAULT NULL,
  `execution_date` datetime DEFAULT NULL,
  `state` varchar(50) DEFAULT NULL,
  `run_id` varchar(250) DEFAULT NULL,
  `external_trigger` tinyint(1) DEFAULT NULL,
  `conf` blob,
  `end_date` datetime DEFAULT NULL,
  `start_date` datetime DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `dag_id` (`dag_id`,`execution_date`),
  UNIQUE KEY `dag_id_2` (`dag_id`,`run_id`),
  KEY `dag_id_state` (`dag_id`,`state`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `dag_stats`
--

DROP TABLE IF EXISTS `dag_stats`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `dag_stats` (
  `dag_id` varchar(250) NOT NULL,
  `state` varchar(50) NOT NULL,
  `count` int(11) NOT NULL,
  `dirty` tinyint(1) NOT NULL,
  PRIMARY KEY (`dag_id`,`state`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `import_error`
--

DROP TABLE IF EXISTS `import_error`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `import_error` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `timestamp` datetime DEFAULT NULL,
  `filename` varchar(1024) DEFAULT NULL,
  `stacktrace` text,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `job`
--

DROP TABLE IF EXISTS `job`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `job` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `dag_id` varchar(250) DEFAULT NULL,
  `state` varchar(20) DEFAULT NULL,
  `job_type` varchar(30) DEFAULT NULL,
  `start_date` datetime DEFAULT NULL,
  `end_date` datetime DEFAULT NULL,
  `latest_heartbeat` datetime DEFAULT NULL,
  `executor_class` varchar(500) DEFAULT NULL,
  `hostname` varchar(500) DEFAULT NULL,
  `unixname` varchar(1000) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `job_type_heart` (`job_type`,`latest_heartbeat`),
  KEY `idx_job_state_heartbeat` (`state`,`latest_heartbeat`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `known_event`
--

DROP TABLE IF EXISTS `known_event`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `known_event` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `label` varchar(200) DEFAULT NULL,
  `start_date` datetime DEFAULT NULL,
  `end_date` datetime DEFAULT NULL,
  `user_id` int(11) DEFAULT NULL,
  `known_event_type_id` int(11) DEFAULT NULL,
  `description` text,
  PRIMARY KEY (`id`),
  KEY `known_event_type_id` (`known_event_type_id`),
  KEY `user_id` (`user_id`),
  CONSTRAINT `known_event_ibfk_1` FOREIGN KEY (`known_event_type_id`) REFERENCES `known_event_type` (`id`),
  CONSTRAINT `known_event_ibfk_2` FOREIGN KEY (`user_id`) REFERENCES `users` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `known_event_type`
--

DROP TABLE IF EXISTS `known_event_type`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `known_event_type` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `know_event_type` varchar(200) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `known_event_type`
--

INSERT INTO `known_event_type` VALUES (1,'Holiday'),(2,'Outage'),(3,'Natural Disaster'),(4,'Marketing Campaign');

--
-- Table structure for table `kube_resource_version`
--

DROP TABLE IF EXISTS `kube_resource_version`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `kube_resource_version` (
  `one_row_id` tinyint(1) NOT NULL DEFAULT '1',
  `resource_version` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`one_row_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `kube_resource_version`
--

INSERT INTO `kube_resource_version` VALUES (1,'');

--
-- Table structure for table `kube_worker_uuid`
--

DROP TABLE IF EXISTS `kube_worker_uuid`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `kube_worker_uuid` (
  `one_row_id` tinyint(1) NOT NULL DEFAULT '1',
  `worker_uuid` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`one_row_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `kube_worker_uuid`
--

INSERT INTO `kube_worker_uuid` VALUES (1,'');

--
-- Table structure for table `log`
--

DROP TABLE IF EXISTS `log`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `log` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `dttm` datetime DEFAULT NULL,
  `dag_id` varchar(250) DEFAULT NULL,
  `task_id` varchar(250) DEFAULT NULL,
  `event` varchar(30) DEFAULT NULL,
  `execution_date` datetime DEFAULT NULL,
  `owner` varchar(500) DEFAULT NULL,
  `extra` text,
  PRIMARY KEY (`id`),
  KEY `idx_log_dag` (`dag_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `sla_miss`
--

DROP TABLE IF EXISTS `sla_miss`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `sla_miss` (
  `task_id` varchar(250) NOT NULL,
  `dag_id` varchar(250) NOT NULL,
  `execution_date` datetime NOT NULL,
  `email_sent` tinyint(1) DEFAULT NULL,
  `timestamp` datetime DEFAULT NULL,
  `description` text,
  `notification_sent` tinyint(1) DEFAULT NULL,
  PRIMARY KEY (`task_id`,`dag_id`,`execution_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `slot_pool`
--

DROP TABLE IF EXISTS `slot_pool`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `slot_pool` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `pool` varchar(50) DEFAULT NULL,
  `slots` int(11) DEFAULT NULL,
  `description` text,
  PRIMARY KEY (`id`),
  UNIQUE KEY `pool` (`pool`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `task_fail`
--

DROP TABLE IF EXISTS `task_fail`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `task_fail` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `task_id` varchar(250) NOT NULL,
  `dag_id` varchar(250) NOT NULL,
  `execution_date` datetime DEFAULT NULL,
  `start_date` datetime DEFAULT NULL,
  `end_date` datetime DEFAULT NULL,
  `duration` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_task_fail_dag_task_date` (`dag_id`,`task_id`,`execution_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `task_instance`
--

DROP TABLE IF EXISTS `task_instance`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `task_instance` (
  `task_id` varchar(250) NOT NULL,
  `dag_id` varchar(250) NOT NULL,
  `execution_date` datetime NOT NULL,
  `start_date` datetime DEFAULT NULL,
  `end_date` datetime DEFAULT NULL,
  `duration` float DEFAULT NULL,
  `state` varchar(20) DEFAULT NULL,
  `try_number` int(11) DEFAULT NULL,
  `hostname` varchar(1000) DEFAULT NULL,
  `unixname` varchar(1000) DEFAULT NULL,
  `job_id` int(11) DEFAULT NULL,
  `pool` varchar(50) DEFAULT NULL,
  `queue` varchar(50) DEFAULT NULL,
  `priority_weight` int(11) DEFAULT NULL,
  `operator` varchar(1000) DEFAULT NULL,
  `queued_dttm` datetime DEFAULT NULL,
  `pid` int(11) DEFAULT NULL,
  `max_tries` int(11) DEFAULT '-1',
  `executor_config` blob,
  PRIMARY KEY (`task_id`,`dag_id`,`execution_date`),
  KEY `ti_dag_state` (`dag_id`,`state`),
  KEY `ti_pool` (`pool`,`state`,`priority_weight`),
  KEY `ti_state_lkp` (`dag_id`,`task_id`,`execution_date`,`state`),
  KEY `ti_state` (`state`),
  KEY `ti_job_id` (`job_id`),
  KEY `dag_id_execution_date` (`dag_id`,`execution_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `users`
--

DROP TABLE IF EXISTS `users`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `users` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `username` varchar(250) DEFAULT NULL,
  `email` varchar(500) DEFAULT NULL,
  `password` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `username` (`username`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `variable`
--

DROP TABLE IF EXISTS `variable`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `variable` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `key` varchar(250) DEFAULT NULL,
  `val` mediumtext,
  `is_encrypted` tinyint(1) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `key` (`key`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `xcom`
--

DROP TABLE IF EXISTS `xcom`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `xcom` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `key` varchar(512) DEFAULT NULL,
  `value` mediumblob,
  `timestamp` datetime DEFAULT NULL,
  `execution_date` datetime DEFAULT NULL,
  `task_id` varchar(250) NOT NULL,
  `dag_id` varchar(250) NOT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_xcom_dag_task_date` (`dag_id`,`task_id`,`execution_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2018-08-29 10:41:40
