-- MySQL dump 10.13  Distrib 5.6.19, for osx10.7 (i386)
--
-- Host: 127.0.0.1    Database: platform_homework
-- ------------------------------------------------------
-- Server version	5.6.23

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
-- Table structure for table `platform_article`
--

DROP TABLE IF EXISTS `platform_article`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `platform_article` (
  `article_id` varchar(45) NOT NULL,
  `writer_id` varchar(45) NOT NULL,
  `article_title` varchar(45) DEFAULT NULL,
  `content` varchar(45) DEFAULT NULL,
  `create_time` datetime NOT NULL,
  PRIMARY KEY (`article_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `platform_article`
--

LOCK TABLES `platform_article` WRITE;
/*!40000 ALTER TABLE `platform_article` DISABLE KEYS */;
INSERT INTO `platform_article` VALUES ('1','5','软件工程导论','这是一条测试数据，里面并没有讲软件工程','2015-09-20 13:23:33'),('2','5','晚上几点睡觉最好','看是不是单身咯','2015-09-23 09:20:50'),('3','5','早上几点起最好','最好不要起','2015-09-24 13:59:44'),('4','5','软件工程哪家强','山东有蓝翔，深圳富士康','2015-10-15 15:30:20'),('5','5','心好塞','出去跑跑就好了','2015-10-15 18:08:09'),('6','1','计算与软件工程一','这是一门必修课','2013-10-17 10:08:10'),('7','3','软绵绵','这是什么鬼','2015-10-10 08:00:03'),('8','2','计算与软件工程二','这也是一门必修课','2013-12-25 12:01:03'),('9','2','计算与软件工程三','这门课的老师和作业的老师是同一个！','2014-01-13 12:30:08');
/*!40000 ALTER TABLE `platform_article` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `platform_deal`
--

DROP TABLE IF EXISTS `platform_deal`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `platform_deal` (
  `deal_id` varchar(45) NOT NULL,
  `article_id` varchar(45) NOT NULL,
  `reader_id` varchar(45) NOT NULL,
  `deal_payment` int(11) NOT NULL,
  `create_time` datetime NOT NULL,
  PRIMARY KEY (`deal_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `platform_deal`
--

LOCK TABLES `platform_deal` WRITE;
/*!40000 ALTER TABLE `platform_deal` DISABLE KEYS */;
INSERT INTO `platform_deal` VALUES ('1','6','8',10,'2015-11-01 13:00:08'),('2','8','8',2,'2015-11-01 13:20:03'),('3','2','3',5,'2015-11-01 14:20:02'),('4','6','3',9,'2015-11-01 14:04:50'),('5','7','8',5,'2015-11-03 10:02:50'),('6','3','9',2,'2015-11-04 09:09:09');
/*!40000 ALTER TABLE `platform_deal` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `platform_reader`
--

DROP TABLE IF EXISTS `platform_reader`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `platform_reader` (
  `reader_id` varchar(45) NOT NULL,
  `reader_name` varchar(45) DEFAULT NULL,
  `create_time` datetime NOT NULL,
  PRIMARY KEY (`reader_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `platform_reader`
--

LOCK TABLES `platform_reader` WRITE;
/*!40000 ALTER TABLE `platform_reader` DISABLE KEYS */;
INSERT INTO `platform_reader` VALUES ('1','adam','2013-05-20 10:00:08'),('10','zeus','2015-07-17 09:00:30'),('2','allan','2013-05-23 15:33:23'),('3','bob','2014-06-16 18:06:03'),('4','carren','2015-04-30 14:00:32'),('5','sunshine','2015-05-01 08:16:23'),('6','xiaoming','2015-05-15 16:23:43'),('7','jully','2015-09-19 00:00:14'),('8','zoe','2015-10-15 00:00:13'),('9','god','2015-08-13 10:03:56');
/*!40000 ALTER TABLE `platform_reader` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `platform_writer`
--

DROP TABLE IF EXISTS `platform_writer`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `platform_writer` (
  `writer_id` varchar(20) NOT NULL,
  `writer_name` varchar(45) NOT NULL,
  `writer_email` varchar(45) NOT NULL,
  `create_time` datetime NOT NULL,
  PRIMARY KEY (`writer_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `platform_writer`
--

LOCK TABLES `platform_writer` WRITE;
/*!40000 ALTER TABLE `platform_writer` DISABLE KEYS */;
INSERT INTO `platform_writer` VALUES ('1','Eddie','eddie@platform.com','2013-01-01 10:00:08'),('2','Ruby Pier','ruby_pier@platform.com','2013-02-14 18:08:10'),('3','Joe','joe@platform.com','2014-02-14 08:00:53'),('4','Dominguez','domin@platform.com','2015-05-15 14:23:07'),('5','Karen','karen@platform.com','2015-09-19 13:58:35');
/*!40000 ALTER TABLE `platform_writer` ENABLE KEYS */;
UNLOCK TABLES;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2015-11-07 22:26:04
