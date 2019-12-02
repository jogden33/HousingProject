--
-- Use this script to create your database
--

DROP DATABASE IF EXISTS `housing_project`;

CREATE DATABASE housing_project;

USE housing_project;

CREATE TABLE housing
               (
                `id`               INT NOT NULL auto_increment PRIMARY KEY,
                guid               NVARCHAR(32) NOT NULL,
                zip_code           INT(5) NOT NULL,
                city               nvarchar(32) NOT NULL,
                state              nvarchar(2) NOT NULL,
                county             nvarchar(32) NOT NULL,
                median_age         INT(3) NOT NULL,
                total_rooms        INT(5) NOT NULL,
                total_bedrooms     INT(5) NOT NULL,
                population         INT(5) NOT NULL,
                households         INT(5) NOT NULL,
                median_income      INT(8) NOT NULL,
                median_house_value INT(8) NOT NULL
               );
