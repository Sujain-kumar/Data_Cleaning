-- Data cleaning

select *
from layoffs;

-- 1. remove Duplicates
-- 2. standardize the data
-- 3. null values or blank values
-- 4. remove any columns


create table layoffs_staging
like layoffs;

select * 
from layoffs_staging;

insert layoffs_staging
select *
from layoffs;

-- 1


select *,
Row_number() over(
partition by company,industry,total_laid_off,percentage_laid_off,'date') as row_num
from layoffs_staging;


with duplicate_cte as
(
select *,
Row_number() over(
partition by company,location,industry,total_laid_off,percentage_laid_off,'date',stage,country,funds_raised_millions) as row_num
from layoffs_staging
)
select * from duplicate_cte 
where row_num > 1;

select * 
from layoffs_staging
where company ='Casper';

create table `layoffs_staging2` (
`company` text,
`location` text,
`industry` text,
`total_laid_off` int Default null,
`percentage_laid_off` text,
`date` text,
`stage` text,
`country` text,
`funds_raised_millions` int default null,
`row_num` int
) engine=InnoDB default charset=utf8mb4 collate=utf8mb4_0900_ai_ci;

select * from layoffs_staging2;

insert into layoffs_staging2
select *,
Row_number() over(
partition by company,location,industry,total_laid_off,percentage_laid_off,'date',stage,country,funds_raised_millions) as row_num
from layoffs_staging;

select * 
from layoffs_staging2
where row_num >1;



delete
from layoffs_staging2
where row_num >1;

SET SQL_SAFE_UPDATES = 0;

 -- 2 standardizing data
 
 select company,trim(company)
 from layoffs_staging2;
 
 update layoffs_staging2
 set company = trim(company);
 







