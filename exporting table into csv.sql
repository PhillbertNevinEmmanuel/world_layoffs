SELECT *
FROM layoffs_staging
INTO OUTFILE 'D:\nevinphilbert\Portfolio\MySQL Projects\World Layoffs\layoffs_staging.csv'
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n';

SELECT *
FROM layoffs_staging_second
INTO OUTFILE 'D:\nevinphilbert\Portfolio\MySQL Projects\World Layoffs\layoffs_staging_second.csv'
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n';