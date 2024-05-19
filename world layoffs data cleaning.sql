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

/*
1. Checking for duplicates and removing duplicates if any are found.
we can do this by doing partition by over every column in the table, 
if the row number is more than one then it's a duplicate
*/
SELECT *,
ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) row_num
FROM layoffs_staging;

-- looking over every record that might been a duplicate using cte
WITH duplicate_cte AS(
	SELECT *,
	ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) row_num
	FROM layoffs_staging
)
SELECT * FROM duplicate_cte
WHERE row_num > 1;

-- Checking the data from the duplicate searching query
SELECT * FROM layoffs_staging
WHERE company LIKE 'Cazoo%';

-- we have detected the duplicate records, but to delete it in ssms it looks like this
WITH duplicate_cte AS(
	SELECT *,
	ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) row_num
	FROM layoffs_staging
)
DELETE FROM duplicate_cte
WHERE row_num > 1;

-- in mysql workbench, we need to create a new table and insert the data of the cte from our table.

-- creating second staging table
CREATE TABLE `layoffs_staging_second` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` bigint DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- checking the second staging table columns, check again after inserting data
SELECT * FROM layoffs_staging_second;

-- inserting the data
INSERT INTO layoffs_staging_second
SELECT *,
ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) row_num
FROM layoffs_staging;

-- identify the data we want to delete, check the data again to see if the query is correct
SELECT * FROM layoffs_staging_second
WHERE row_num > 1;

-- delete the duplicates
DELETE FROM layoffs_staging_second
WHERE row_num > 1;

/*
2. Standardizing the data
First maybe we need to check whitespaces in company names or industry names,
we need to maybe create a checkpoint by making the third stage
*/

-- creating the third staging table
CREATE TABLE `layoffs_staging_third` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` bigint DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- check the third stage table, check again after inserting the data
SELECT * FROM layoffs_staging_third;

-- inserting the data
INSERT INTO layoffs_staging_third
SELECT * FROM layoffs_staging_second;

-- checking whitespaces in company column, or any incorrect data
SELECT DISTINCT company, TRIM(company) FROM layoffs_staging_third
ORDER BY 1;

SELECT company, TRIM(company) FROM layoffs_staging_third
ORDER BY 1;

-- update it
UPDATE layoffs_staging_third
SET company = TRIM(company);

-- checking industry columns for any whitespaces and mispellings
SELECT DISTINCT industry, TRIM(industry) FROM layoffs_staging_third
ORDER BY 1;

-- looking at crypto since there are three different value
SELECT * FROM layoffs_staging_third
WHERE industry LIKE '%Crypto%';

-- let's update it
UPDATE layoffs_staging_third
SET industry = 'Crypto'
WHERE industry LIKE '%Crypto%';

-- check location column
SELECT DISTINCT location, TRIM(location) FROM layoffs_staging_third;

-- update it just to be sure
UPDATE layoffs_staging_third
SET location = TRIM(location);

-- check stage
SELECT DISTINCT stage, TRIM(stage) FROM layoffs_staging_third
ORDER BY 1;

-- update it just to be sureeeee
UPDATE layoffs_staging_third
SET stage = TRIM(stage);

-- check country
SELECT DISTINCT country, TRIM(TRAILING '.' FROM country) FROM layoffs_staging_third
ORDER BY 1;

-- update it
UPDATE layoffs_staging_third
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

/*
2b. time series standardization
this one is kind of tricky, we need to do the str_to_date and the format is '%m/%d/%Y'
*/

SELECT `date`, STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoffs_staging_third;

-- update the date, i love the rhythm
UPDATE layoffs_staging_third
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

-- alter the date column data type from text to date
ALTER TABLE layoffs_staging_third
MODIFY COLUMN `date` DATE;