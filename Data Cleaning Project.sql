-- SQL Data Cleaning Project

-- Dataset https://www.kaggle.com/datasets/swaptr/layoffs-2022

-- 1. create a staging table 
-- 2. check for and remove duplicates
-- 3. standardize the data
-- 4. action null or blank values
-- 5. remove any unnecessary columns 

-- 1. Create staging table

SELECT * 
FROM world_layoffs.layoffs;

CREATE TABLE world_layoffs.layoffs_staging 
LIKE world_layoffs.layoffs;

INSERT layoffs_staging
SELECT *
FROM world_layoffs.layoffs;

-- 2. Check for and remove duplicates 

SELECT *, 
ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num 
FROM  world_layoffs.layoffs_staging;

WITH duplicate_cte AS 
(
SELECT *, 
ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num 
FROM  world_layoffs.layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

-- a look at Cazoo just to confirm

SELECT * 
FROM  world_layoffs.layoffs_staging
WHERE company = 'Cazoo';

-- another staging table using CREATE STATEMENT with a new column and adding row numbers to delete duplicates 

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

INSERT INTO  world_layoffs.layoffs_staging2
SELECT *, 
ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num 
FROM  world_layoffs.layoffs_staging;

-- delete duplicates rows using the new column 

DELETE
FROM  world_layoffs.layoffs_staging2
WHERE row_num > 1;

-- 3. Standardize data & 4. action blank or null values

SELECT * 
FROM world_layoffs.layoffs_staging2;

-- removing unnecessary space and characters 

SELECT company, (TRIM(company))
FROM world_layoffs.layoffs_staging2;

UPDATE world_layoffs.layoffs_staging2
SET company = TRIM(company);

SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM world_layoffs.layoffs_staging2
ORDER BY 1;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

-- standardize data by making blank values null and populating them where possible

SELECT DISTINCT industry
FROM world_layoffs.layoffs_staging2
ORDER BY industry;

SELECT *
FROM world_layoffs.layoffs_staging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;

UPDATE world_layoffs.layoffs_staging2
SET industry = NULL
WHERE industry = '';

UPDATE layoffs_staging2 t1 
JOIN layoffs_staging2 t2
	ON t1.company = t2.company 
SET t1.industry = t2.industry 
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

-- standardize inconsistencies in Crypto industry values that have been identified 

SELECT DISTINCT industry
FROM world_layoffs.layoffs_staging2
ORDER BY industry;

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

-- standardize the date column definition by converting it to date format 

select `date`
FROM world_layoffs.layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = str_to_date(`date`, '%m/%d/%Y');

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

SELECT *
FROM world_layoffs.layoffs_staging2; 

-- 4. Remove unnecessary columns 

-- columns/rows with blank values that cannot be populated and we cannot use are removed 

SELECT * 
FROM world_layoffs.layoffs_staging2
WHERE percentage_laid_off AND total_laid_off IS NULL;
 
DELETE 
FROM world_layoffs.layoffs_staging2
WHERE percentage_laid_off IS NULL 
AND total_laid_off IS NULL;

-- the row_num column is deleted as it is no longer necessary

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

SELECT *
FROM layoffs_staging2; 

