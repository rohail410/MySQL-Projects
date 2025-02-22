-- Exploratory Data Analysis

SELECT *
FROM layoffs_staging2;

-- Highest number of people laid-off by a company 
-- and Highest percentage of company laid off

SELECT MAX(total_laid_off), MAX(percentage_laid_off)
FROM layoffs_staging2;

-- Companies that laid off their entire staff

SELECT *
FROM layoffs_staging2
WHERE percentage_laid_off = 1;

-- Companies that laid off their entire staff order by total number of employee laid off

SELECT *
FROM layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY total_laid_off DESC
;

-- Companies that laid off their entire staff order by funds raised in million

SELECT *
FROM layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY funds_raised_millions DESC
;

-- Total layoffs based on companies

SELECT company, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 DESC
;

-- Total layoffs based on industry

SELECT industry, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY industry
ORDER BY 2 DESC;

-- Total layoffs based on country

SELECT country, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY country
ORDER BY 2 DESC
;

-- Earliest and Lastest Dates in dataset

SELECT MIN(`date`), MAX(`date`)
FROM layoffs_staging2;


-- Total Layoffs based on Year

SELECT YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY YEAR(`date`)
ORDER BY 1 DESC
;

-- Total Layoffs based on Stage of Company

SELECT stage, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY stage
ORDER BY 2 DESC
;

-- Rolling total of total laid off based on month-year (2020-03 to 2023-03)

SELECT *
FROM layoffs_staging2;

SELECT SUBSTRING(`date`,1,7) as `year_month`, SUM(total_laid_off)
FROM layoffs_staging2
WHERE SUBSTRING(`date`,1,7) IS NOT NULL
GROUP BY SUBSTRING(`date`,1,7)
ORDER BY `year_month`
;

WITH CTE_1 AS
(
SELECT SUBSTRING(`date`,1,7) as `year_month`, SUM(total_laid_off) as monthly_layoffs
FROM layoffs_staging2
WHERE SUBSTRING(`date`,1,7) IS NOT NULL
GROUP BY SUBSTRING(`date`,1,7)
ORDER BY `year_month`
)
SELECT `year_month`, monthly_layoffs, SUM(monthly_layoffs) OVER(ORDER BY `year_month`) as rolling_total
FROM CTE_1
;

-- Top 5 companies with highest total laid off each year from 2020 to 2023

SELECT company, Year(`date`) AS `year`, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company, Year(`date`)
;

WITH company_year (company, `year`, laid_off)  AS (
SELECT company, Year(`date`) AS `year`, SUM(total_laid_off) 
FROM layoffs_staging2
WHERE Year(`date`) IS NOT NULL
GROUP BY company, Year(`date`)
),
company_year_ranking AS
(SELECT *, DENSE_RANK() OVER(partition by `year` ORDER BY laid_off DESC) AS ranking
FROM company_year
)
SELECT *
FROM company_year_ranking
WHERE ranking <= 5
;



