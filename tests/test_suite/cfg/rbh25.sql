-- MySQL dump 10.13  Distrib 5.1.73, for redhat-linux-gnu (x86_64)
--
-- Host: localhost    Database: robinhood_lustre
-- ------------------------------------------------------
-- Server version	5.1.73

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
-- Table structure for table `ACCT_STAT`
--

DROP TABLE IF EXISTS `ACCT_STAT`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `ACCT_STAT` (
  `owner` varchar(127) NOT NULL DEFAULT '',
  `gr_name` varchar(127) NOT NULL DEFAULT '',
  `type` enum('symlink','dir','file','chr','blk','fifo','sock') NOT NULL DEFAULT 'symlink',
  `status` int(11) NOT NULL DEFAULT '0',
  `size` bigint(20) unsigned DEFAULT NULL,
  `blocks` bigint(20) unsigned DEFAULT NULL,
  `count` bigint(20) unsigned DEFAULT NULL,
  `sz0` bigint(20) unsigned DEFAULT '0',
  `sz1` bigint(20) unsigned DEFAULT '0',
  `sz32` bigint(20) unsigned DEFAULT '0',
  `sz1K` bigint(20) unsigned DEFAULT '0',
  `sz32K` bigint(20) unsigned DEFAULT '0',
  `sz1M` bigint(20) unsigned DEFAULT '0',
  `sz32M` bigint(20) unsigned DEFAULT '0',
  `sz1G` bigint(20) unsigned DEFAULT '0',
  `sz32G` bigint(20) unsigned DEFAULT '0',
  `sz1T` bigint(20) unsigned DEFAULT '0',
  PRIMARY KEY (`owner`,`gr_name`,`type`,`status`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `ACCT_STAT`
--

LOCK TABLES `ACCT_STAT` WRITE;
/*!40000 ALTER TABLE `ACCT_STAT` DISABLE KEYS */;
INSERT INTO `ACCT_STAT` VALUES ('root','root','file',1,0,0,0,0,0,0,0,0,0,0,0,0,0);
/*!40000 ALTER TABLE `ACCT_STAT` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `ANNEX_INFO`
--

DROP TABLE IF EXISTS `ANNEX_INFO`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `ANNEX_INFO` (
  `id` varchar(64) NOT NULL,
  `creation_time` int(10) unsigned DEFAULT NULL,
  `last_archive` int(10) unsigned DEFAULT NULL,
  `last_restore` int(10) unsigned DEFAULT NULL,
  `link` text,
  `archive_id` int(10) unsigned DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `ANNEX_INFO`
--

LOCK TABLES `ANNEX_INFO` WRITE;
/*!40000 ALTER TABLE `ANNEX_INFO` DISABLE KEYS */;
/*!40000 ALTER TABLE `ANNEX_INFO` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `ENTRIES`
--

DROP TABLE IF EXISTS `ENTRIES`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `ENTRIES` (
  `id` varchar(64) NOT NULL,
  `owner` varchar(127) DEFAULT 'unknown',
  `gr_name` varchar(127) DEFAULT 'unknown',
  `size` bigint(20) unsigned DEFAULT NULL,
  `blocks` bigint(20) unsigned DEFAULT NULL,
  `last_access` int(10) unsigned DEFAULT NULL,
  `last_mod` int(10) unsigned DEFAULT NULL,
  `type` enum('symlink','dir','file','chr','blk','fifo','sock') DEFAULT NULL,
  `mode` smallint(5) unsigned DEFAULT NULL,
  `nlink` int(10) unsigned DEFAULT NULL,
  `status` int(11) DEFAULT '0',
  `md_update` int(10) unsigned DEFAULT NULL,
  `no_release` tinyint(1) DEFAULT NULL,
  `no_archive` tinyint(1) DEFAULT NULL,
  `archive_class` varchar(127) DEFAULT NULL,
  `arch_cl_update` int(10) unsigned DEFAULT NULL,
  `release_class` varchar(127) DEFAULT NULL,
  `rel_cl_update` int(10) unsigned DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `ENTRIES`
--

LOCK TABLES `ENTRIES` WRITE;
/*!40000 ALTER TABLE `ENTRIES` DISABLE KEYS */;
/*!40000 ALTER TABLE `ENTRIES` ENABLE KEYS */;
UNLOCK TABLES;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = latin1 */ ;
/*!50003 SET character_set_results = latin1 */ ;
/*!50003 SET collation_connection  = latin1_swedish_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
/*!50003 CREATE*/ /*!50017 DEFINER=`robinhood`@`localhost`*/ /*!50003 TRIGGER ACCT_ENTRY_INSERT AFTER INSERT ON ENTRIES FOR EACH ROW BEGIN DECLARE val BIGINT UNSIGNED; SET val=FLOOR(LOG2(NEW.size)/5);INSERT INTO ACCT_STAT(owner,gr_name,type,status,size,blocks, count, sz0, sz1, sz32, sz1K, sz32K, sz1M, sz32M, sz1G, sz32G, sz1T) VALUES (NEW.owner,NEW.gr_name,NEW.type,NEW.status,NEW.size,NEW.blocks, 1, NEW.size=0, IFNULL(val=0,0), IFNULL(val=1,0), IFNULL(val=2,0), IFNULL(val=3,0), IFNULL(val=4,0), IFNULL(val=5,0), IFNULL(val=6,0), IFNULL(val=7,0), IFNULL(val>=8,0)) ON DUPLICATE KEY UPDATE size=CAST(size as SIGNED)+CAST(NEW.size as SIGNED) , blocks=CAST(blocks as SIGNED)+CAST(NEW.blocks as SIGNED) , count=count+1, sz0=CAST(sz0 as SIGNED)+CAST((NEW.size=0) as SIGNED), sz1=CAST(sz1 as SIGNED)+CAST(IFNULL(val=0,0) as SIGNED), sz32=CAST(sz32 as SIGNED)+CAST(IFNULL(val=1,0) as SIGNED), sz1K=CAST(sz1K as SIGNED)+CAST(IFNULL(val=2,0) as SIGNED), sz32K=CAST(sz32K as SIGNED)+CAST(IFNULL(val=3,0) as SIGNED), sz1M=CAST(sz1M as SIGNED)+CAST(IFNULL(val=4,0) as SIGNED), sz32M=CAST(sz32M as SIGNED)+CAST(IFNULL(val=5,0) as SIGNED), sz1G=CAST(sz1G as SIGNED)+CAST(IFNULL(val=6,0) as SIGNED), sz32G=CAST(sz32G as SIGNED)+CAST(IFNULL(val=7,0) as SIGNED), sz1T=CAST(sz1T as SIGNED)+CAST(IFNULL(val>=8,0) as SIGNED); END */;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = latin1 */ ;
/*!50003 SET character_set_results = latin1 */ ;
/*!50003 SET collation_connection  = latin1_swedish_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
/*!50003 CREATE*/ /*!50017 DEFINER=`robinhood`@`localhost`*/ /*!50003 TRIGGER ACCT_ENTRY_UPDATE AFTER UPDATE ON ENTRIES FOR EACH ROW BEGIN DECLARE val_old, val_new BIGINT UNSIGNED;SET val_old=FLOOR(LOG2(OLD.size)/5); SET val_new=FLOOR(LOG2(NEW.size)/5);
IF NEW.owner=OLD.owner AND NEW.gr_name=OLD.gr_name AND NEW.type=OLD.type AND NEW.status=OLD.status THEN 
	 IF NEW.size<>OLD.size OR NEW.blocks<>OLD.blocks THEN 
		 UPDATE ACCT_STAT SET  size=size+CAST(NEW.size as SIGNED)-CAST(OLD.size as SIGNED) , blocks=blocks+CAST(NEW.blocks as SIGNED)-CAST(OLD.blocks as SIGNED) , sz0=CAST(sz0 as SIGNED)-CAST(((OLD.size=0)+(NEW.size=0)) as SIGNED), sz1=CAST(sz1 as SIGNED)-CAST(IFNULL(val_old=0,0) as SIGNED)+CAST(IFNULL(val_new=0,0) as SIGNED), sz32=CAST(sz32 as SIGNED)-CAST(IFNULL(val_old=1,0) as SIGNED)+CAST(IFNULL(val_new=1,0) as SIGNED), sz1K=CAST(sz1K as SIGNED)-CAST(IFNULL(val_old=2,0) as SIGNED)+CAST(IFNULL(val_new=2,0) as SIGNED), sz32K=CAST(sz32K as SIGNED)-CAST(IFNULL(val_old=3,0) as SIGNED)+CAST(IFNULL(val_new=3,0) as SIGNED), sz1M=CAST(sz1M as SIGNED)-CAST(IFNULL(val_old=4,0) as SIGNED)+CAST(IFNULL(val_new=4,0) as SIGNED), sz32M=CAST(sz32M as SIGNED)-CAST(IFNULL(val_old=5,0) as SIGNED)+CAST(IFNULL(val_new=5,0) as SIGNED), sz1G=CAST(sz1G as SIGNED)-CAST(IFNULL(val_old=6,0) as SIGNED)+CAST(IFNULL(val_new=6,0) as SIGNED), sz32G=CAST(sz32G as SIGNED)-CAST(IFNULL(val_old=7,0) as SIGNED)+CAST(IFNULL(val_new=7,0) as SIGNED), sz1T=CAST(sz1T as SIGNED)-CAST(IFNULL(val_old>=8,0) as SIGNED)+CAST(IFNULL(val_new>=8,0) as SIGNED) WHERE owner=NEW.owner AND gr_name=NEW.gr_name AND type=NEW.type AND status=NEW.status ; 
	 END IF; 
ELSEIF NEW.owner<>OLD.owner OR NEW.gr_name<>OLD.gr_name OR NEW.type<>OLD.type OR NEW.status<>OLD.status THEN 
	INSERT INTO ACCT_STAT(owner,gr_name,type,status,size,blocks, count, sz0, sz1, sz32, sz1K, sz32K, sz1M, sz32M, sz1G, sz32G, sz1T) VALUES (NEW.owner,NEW.gr_name,NEW.type,NEW.status,NEW.size,NEW.blocks, 1, NEW.size=0, IFNULL(val_new=0,0), IFNULL(val_new=1,0), IFNULL(val_new=2,0), IFNULL(val_new=3,0), IFNULL(val_new=4,0), IFNULL(val_new=5,0), IFNULL(val_new=6,0), IFNULL(val_new=7,0), IFNULL(val_new>=8,0)) 
	ON DUPLICATE KEY UPDATE size=CAST(size as SIGNED)+CAST(NEW.size as SIGNED) , blocks=CAST(blocks as SIGNED)+CAST(NEW.blocks as SIGNED) , count=count+1, sz0=CAST(sz0 as SIGNED)+CAST((NEW.size=0) as SIGNED), sz1=CAST(sz1 as SIGNED)+CAST(IFNULL(val_new=0,0) as SIGNED), sz32=CAST(sz32 as SIGNED)+CAST(IFNULL(val_new=1,0) as SIGNED), sz1K=CAST(sz1K as SIGNED)+CAST(IFNULL(val_new=2,0) as SIGNED), sz32K=CAST(sz32K as SIGNED)+CAST(IFNULL(val_new=3,0) as SIGNED), sz1M=CAST(sz1M as SIGNED)+CAST(IFNULL(val_new=4,0) as SIGNED), sz32M=CAST(sz32M as SIGNED)+CAST(IFNULL(val_new=5,0) as SIGNED), sz1G=CAST(sz1G as SIGNED)+CAST(IFNULL(val_new=6,0) as SIGNED), sz32G=CAST(sz32G as SIGNED)+CAST(IFNULL(val_new=7,0) as SIGNED), sz1T=CAST(sz1T as SIGNED)+CAST(IFNULL(val_new>=8,0) as SIGNED);
	UPDATE ACCT_STAT SET size=CAST(size as SIGNED)-CAST(OLD.size as SIGNED) , blocks=CAST(blocks as SIGNED)-CAST(OLD.blocks as SIGNED) , count=count-1 , sz0=CAST(sz0 as SIGNED)-CAST((OLD.size=0) as SIGNED), sz1=CAST(sz1 as SIGNED)-CAST(IFNULL(val_old=0,0) as SIGNED), sz32=CAST(sz32 as SIGNED)-CAST(IFNULL(val_old=1,0) as SIGNED), sz1K=CAST(sz1K as SIGNED)-CAST(IFNULL(val_old=2,0) as SIGNED), sz32K=CAST(sz32K as SIGNED)-CAST(IFNULL(val_old=3,0) as SIGNED), sz1M=CAST(sz1M as SIGNED)-CAST(IFNULL(val_old=4,0) as SIGNED), sz32M=CAST(sz32M as SIGNED)-CAST(IFNULL(val_old=5,0) as SIGNED), sz1G=CAST(sz1G as SIGNED)-CAST(IFNULL(val_old=6,0) as SIGNED), sz32G=CAST(sz32G as SIGNED)-CAST(IFNULL(val_old=7,0) as SIGNED), sz1T=CAST(sz1T as SIGNED)-CAST(IFNULL(val_old>=8,0) as SIGNED) WHERE owner=OLD.owner AND gr_name=OLD.gr_name AND type=OLD.type AND status=OLD.status ;
END IF;
 END */;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = latin1 */ ;
/*!50003 SET character_set_results = latin1 */ ;
/*!50003 SET collation_connection  = latin1_swedish_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
/*!50003 CREATE*/ /*!50017 DEFINER=`robinhood`@`localhost`*/ /*!50003 TRIGGER ACCT_ENTRY_DELETE BEFORE DELETE ON ENTRIES FOR EACH ROW BEGIN DECLARE val BIGINT UNSIGNED; SET val=FLOOR(LOG2(OLD.size)/5);UPDATE ACCT_STAT SET size=CAST(size as SIGNED)-CAST(OLD.size as SIGNED) , blocks=CAST(blocks as SIGNED)-CAST(OLD.blocks as SIGNED) , count=count-1, sz0=CAST(sz0 as SIGNED)-CAST((OLD.size=0) as SIGNED), sz1=CAST(sz1 as SIGNED)-CAST(IFNULL(val=0,0) as SIGNED), sz32=CAST(sz32 as SIGNED)-CAST(IFNULL(val=1,0) as SIGNED), sz1K=CAST(sz1K as SIGNED)-CAST(IFNULL(val=2,0) as SIGNED), sz32K=CAST(sz32K as SIGNED)-CAST(IFNULL(val=3,0) as SIGNED), sz1M=CAST(sz1M as SIGNED)-CAST(IFNULL(val=4,0) as SIGNED), sz32M=CAST(sz32M as SIGNED)-CAST(IFNULL(val=5,0) as SIGNED), sz1G=CAST(sz1G as SIGNED)-CAST(IFNULL(val=6,0) as SIGNED), sz32G=CAST(sz32G as SIGNED)-CAST(IFNULL(val=7,0) as SIGNED), sz1T=CAST(sz1T as SIGNED)-CAST(IFNULL(val>=8,0) as SIGNED) WHERE owner=OLD.owner AND gr_name=OLD.gr_name AND type=OLD.type AND status=OLD.status ; END */;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;

--
-- Table structure for table `NAMES`
--

DROP TABLE IF EXISTS `NAMES`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `NAMES` (
  `id` varchar(64) DEFAULT NULL,
  `pkn` varchar(40) NOT NULL,
  `parent_id` varchar(64) DEFAULT NULL,
  `name` varchar(255) DEFAULT NULL,
  `path_update` int(10) unsigned DEFAULT NULL,
  PRIMARY KEY (`pkn`),
  KEY `parent_id_index` (`parent_id`),
  KEY `id_index` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `NAMES`
--

LOCK TABLES `NAMES` WRITE;
/*!40000 ALTER TABLE `NAMES` DISABLE KEYS */;
/*!40000 ALTER TABLE `NAMES` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `SOFT_RM`
--

DROP TABLE IF EXISTS `SOFT_RM`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `SOFT_RM` (
  `fid` varchar(64) NOT NULL,
  `fullpath` varchar(4095) DEFAULT NULL,
  `soft_rm_time` int(10) unsigned DEFAULT NULL,
  `real_rm_time` int(10) unsigned DEFAULT NULL,
  `archive_id` int(10) unsigned DEFAULT NULL,
  PRIMARY KEY (`fid`),
  KEY `rm_time` (`real_rm_time`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `SOFT_RM`
--

LOCK TABLES `SOFT_RM` WRITE;
/*!40000 ALTER TABLE `SOFT_RM` DISABLE KEYS */;
INSERT INTO `SOFT_RM` VALUES ('0x200000400:0x1:0x0','/mnt/lustre/file.1',1467228579,1467228609,0);
/*!40000 ALTER TABLE `SOFT_RM` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `STRIPE_INFO`
--

DROP TABLE IF EXISTS `STRIPE_INFO`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `STRIPE_INFO` (
  `id` varchar(64) NOT NULL,
  `validator` int(11) DEFAULT NULL,
  `stripe_count` int(10) unsigned DEFAULT NULL,
  `stripe_size` int(10) unsigned DEFAULT NULL,
  `pool_name` varchar(16) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `STRIPE_INFO`
--

LOCK TABLES `STRIPE_INFO` WRITE;
/*!40000 ALTER TABLE `STRIPE_INFO` DISABLE KEYS */;
/*!40000 ALTER TABLE `STRIPE_INFO` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `STRIPE_ITEMS`
--

DROP TABLE IF EXISTS `STRIPE_ITEMS`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `STRIPE_ITEMS` (
  `id` varchar(64) DEFAULT NULL,
  `stripe_index` int(10) unsigned DEFAULT NULL,
  `ostidx` int(10) unsigned DEFAULT NULL,
  `details` binary(20) DEFAULT NULL,
  KEY `id_index` (`id`),
  KEY `st_index` (`ostidx`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `STRIPE_ITEMS`
--

LOCK TABLES `STRIPE_ITEMS` WRITE;
/*!40000 ALTER TABLE `STRIPE_ITEMS` DISABLE KEYS */;
/*!40000 ALTER TABLE `STRIPE_ITEMS` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `VARS`
--

DROP TABLE IF EXISTS `VARS`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `VARS` (
  `varname` varchar(255) NOT NULL,
  `value` text,
  PRIMARY KEY (`varname`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `VARS`
--

LOCK TABLES `VARS` WRITE;
/*!40000 ALTER TABLE `VARS` DISABLE KEYS */;
INSERT INTO `VARS` VALUES ('ChangelogLastCommit_MDT0000','9'),('FS_Path','/mnt/lustre'),('VersionFunctionSet','1.1'),('VersionTriggerSet','1.1');
/*!40000 ALTER TABLE `VARS` ENABLE KEYS */;
UNLOCK TABLES;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2016-06-29 21:30:38
