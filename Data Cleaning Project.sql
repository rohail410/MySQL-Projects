Select * 
FROM layoffs;

-- DATA Cleaning 
-- 1. Remove Duplicates
-- 2. Standardize the Data
-- 3. Null Values or blank values
-- 4. Remove Any Columns

-- Creating a working table to avoid changes in raw data/table

CREATE TABLE layoffs_staging
LIKE layoffs;

Select * 
FROM layoffs_staging;

INSERT INTO layoffs_staging
SELECT *
FROM layoffs;

-- 1. Removing Duplicates

SELECT *,
row_number() OVER(PARTITION BY company, location, industry, total_laid_off,
percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
;


WITH duplicate_cte AS
(
SELECT *,
row_number() OVER(PARTITION BY company, location, industry, total_laid_off,
percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT * 
FROM duplicate_cte
WHERE row_num > 1;

SELECT *
FROM layoffs_staging
WHERE company = "Yahoo";

WITH duplicate_cte AS
(
SELECT *,
row_number() OVER(PARTITION BY company, location, industry, total_laid_off,
percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
DELETE 
FROM duplicate_cte
WHERE row_num > 1;



CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT *
FROM layoffs_staging2;

INSERT INTO layoffs_staging2
SELECT *,
row_number() OVER(PARTITION BY company, location, industry, total_laid_off,
percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
;

DELETE 
FROM layoffs_staging2
WHERE row_num > 1;

SET SQL_SAFE_UPDATES = 0;


SELECT *
FROM layoffs_staging2 
WHERE row_num > 1;

-- 2. Standardizing the Data

-- 2.1. TRIM data in company column

SELECT *
FROM layoffs_staging2;

SELECT distinct company
FROM layoffs_staging2;

SELECT company, TRIM(company)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company);


-- Setting industry name to Crypto for all industry with similar name

SELECT distinct industry
FROM layoffs_staging2
ORDER BY industry
;

SELECT *
FROM layoffs_staging2
WHERE industry LIKE "Crypto%";

UPDATE layoffs_staging2
SET industry = "Crypto"
WHERE industry LIKE "Crypto%";

-- Standardizing value of United States. to United States

SELECT distinct country
FROM layoffs_staging2
order by 1
;

SELECT distinct country, TRIM(Trailing "." FROM country)
FROM layoffs_staging2
order by 1
;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING "." FROM country);

-- Converting values of date columns from text to mysql dates 
-- Changing data type of date column from text to date

SELECT *
FROM layoffs_staging2;

SELECT `date`, str_to_date(`date`, "%m/%d/%Y")
FROM layoffs_staging2
;

UPDATE layoffs_staging2
set `date` = str_to_date(`date`, "%m/%d/%Y");

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` date;

-- Populating the null values in industry column with corresponding industry value
 
SELECT distinct industry
FROM layoffs_staging2
order by 1
;

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
;

UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = "NULL";

SELECT *
FROM layoffs_staging2
WHERE company = "Airbnb";


SELECT *
FROM layoffs_staging2 t1
 JOIN layoffs_staging2 t2 
 ON t1.company = t2.company
WHERE t1.company LIKE "Airbnb";

SELECT *
FROM layoffs_staging2 t1
 JOIN layoffs_staging2 t2 
 ON t1.company = t2.company
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL
;

UPDATE layoffs_staging2 t1
 JOIN layoffs_staging2 t2 
 ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL 
AND t2.industry IS NOT NULL;

-- Deleting rows which have no value for total_laid_off and percentage_laid_off

SELECT *
FROM layoffs_staging2
;

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL AND percentage_laid_off IS NULL;
;

DELETE 
FROM layoffs_staging2
WHERE total_laid_off IS NULL AND percentage_laid_off IS NULL;
;

-- Removing the row_num column as it is no longer needed

ALTER TABLE layoffs_staging2
DROP COLUMN row_num; 

-- TABLE after data cleaning

SELECT *
FROM layoffs_staging2;
