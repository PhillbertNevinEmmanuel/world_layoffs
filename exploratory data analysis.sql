-- Exploratory Data Analysis of World Layoffs Data

-- We want to explore the data using the metric columns
SELECT *
FROM layoffs_staging_fifth;

SELECT MIN(total_laid_off), MAX(total_laid_off)
FROM layoffs_staging_fifth;

SELECT * FROM layoffs_staging_fifth
WHERE percentage_laid_off >= 1
ORDER BY funds_raised_millions DESC;

SELECT company, SUM(total_laid_off)
FROM layoffs_staging_fifth
WHERE total_laid_off IS NOT NULL
GROUP BY company ORDER BY 2 DESC;

SELECT MIN(`date`), MAX(`date`)
FROM layoffs_staging_fifth;
-- the time series is starting from 11 March 2020 to early March 2023

SELECT industry, SUM(total_laid_off)
FROM layoffs_staging_fifth
GROUP BY industry ORDER BY 2 DESC;

SELECT country, SUM(total_laid_off)
FROM layoffs_staging_fifth
GROUP BY 1 ORDER BY 2 DESC;

SELECT stage, SUM(total_laid_off)
FROM layoffs_staging_fifth
WHERE stage IS NOT NULL
GROUP BY 1 ORDER BY 1;

-- let's do groupby of total_laid_off by date
SELECT YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging_fifth
WHERE YEAR(`date`) IS NOT NULL
GROUP BY 1 ORDER BY 1;

SELECT SUBSTRING(`date`, 1, 7), SUM(total_laid_off)
FROM layoffs_staging_fifth
WHERE SUBSTRING(`date`, 1, 7) IS NOT NULL
GROUP BY 1 ORDER BY 1;

-- we want to create a rolling total of sum(total_laid_off), using over order by per month
WITH rolling_total_cte AS(
	SELECT SUBSTRING(`date`, 1, 7) `month`, SUM(total_laid_off) total_minerva
	FROM layoffs_staging_fifth
	WHERE SUBSTRING(`date`, 1, 7) IS NOT NULL
	GROUP BY 1 ORDER BY 1
)
SELECT `month`, total_minerva, SUM(total_minerva) OVER(ORDER BY `month`) rolling_total
FROM rolling_total_cte;

-- let's try to broader the scope by country
WITH rolling_total_cte AS(
	SELECT SUBSTRING(`date`, 1, 7) `month`, country, SUM(total_laid_off) total_minerva
	FROM layoffs_staging_fifth
	WHERE SUBSTRING(`date`, 1, 7) AND country IS NOT NULL
	GROUP BY 1, 2 ORDER BY 1
)
SELECT `month`, country, total_minerva, SUM(total_minerva) OVER(ORDER BY `month`, country) rolling_total
FROM rolling_total_cte
WHERE total_minerva IS NOT NULL;

