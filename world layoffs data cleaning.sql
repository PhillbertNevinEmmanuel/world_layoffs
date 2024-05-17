-- Data Cleaning in MySQL

/*
Create the Schema and table
*/
-- CREATE SCHEMA `world_layoffs`;
-- CREATE TABLE 'layoffs'

/*
Import the data from our layoff.csv dataset into layoffs table using import wizard
*/

/*
Inspect the data, see if it fits the correct format
*/
SELECT * FROM layoffs;

/*
The steps of data cleaning process after inspecting are:
    1.	Remove duplicates (if necessary)
    2.	Standardize the data
    3.	Check for null values and blank values (and decide whether to populate or to drop them)
	4.	Remove any unused/irrelevant columns to speed your query process
*/

/*
Let's separate the data into two tables, raw data and staging data.
First we create a new table and insert the raw data from raw table into the staging table.
*/
CREATE TABLE layoffs_staging LIKE layoffs;
INSERT layoffs_staging SELECT * FROM layoffs;

/*
Inspect the staging data table
*/
SELECT * FROM layoffs_staging;
-- Now we can clean staging table as we see fit.