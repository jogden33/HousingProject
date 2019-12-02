--
-- Use this script to create your database
--
-- 1. Connect to your local instance of MySQL
--    a. mysql -u <user> -p
-- 2. At the mysql> prompt, use source to read
--    this file and execute the SQL script
--    a. mysql> source databaseCreationScript.sql
--

DROP DATABASE IF EXISTS `housing_project`;

CREATE DATABASE housing_project;

USE housing_project;

CREATE TABLE housing
               (
                `id`               int not null auto_increment primary key,
                guid               char(32) not null,
                zip_code           int not null,
                city               char(32) not null,
                state              char(2) not null,
                county             char(32) not null,
                median_age         int not null,
                total_rooms        int not null,
                total_bedrooms     int not null,
                population         int not null,
                households         int not null,
                median_income      int not null,
                median_house_value int not null
               );
