-- Creation.hql : Création de la base, des tables, partitions et buckets
CREATE DATABASE IF NOT EXISTS hotel_booking;
USE hotel_booking;

-- Propriétés pour partitions et buckets
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=20000;
set hive.exec.max.dynamic.partitions.pernode=20000;
set hive.enforce.bucketing = true;

-- Table clients
CREATE TABLE IF NOT EXISTS clients (
  client_id INT,
  nom STRING,
  email STRING,
  telephone STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

-- Table hotels
CREATE TABLE IF NOT EXISTS hotels (
  hotel_id INT,
  nom STRING,
  ville STRING,
  etoiles INT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

-- Table reservations partitionnée par date_debut
CREATE TABLE IF NOT EXISTS reservations (
  reservation_id INT,
  client_id INT,
  hotel_id INT,
  date_fin DATE,
  prix_total DECIMAL(10,2)
)
PARTITIONED BY (date_debut DATE)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

-- Table hotels partitionnée par ville
CREATE TABLE IF NOT EXISTS hotels_partitioned (
  hotel_id INT,
  nom STRING,
  etoiles INT
)
PARTITIONED BY (ville STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

-- Table reservations bucketée par client_id
CREATE TABLE IF NOT EXISTS reservations_bucketed (
  reservation_id INT,
  client_id INT,
  hotel_id INT,
  date_debut DATE,
  date_fin DATE,
  prix_total DECIMAL(10,2)
)
CLUSTERED BY (client_id) INTO 4 BUCKETS
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE;
