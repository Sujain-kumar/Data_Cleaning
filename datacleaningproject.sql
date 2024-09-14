-- SQL Project - Data Cleaning

select *
from layoffs;

-- 1. remove Duplicates
-- 2. standardize the data
-- 3. null values or blank values
-- 4. remove any columns

-- first thing we want to do is create a staging table. This is the one we will work in and clean the data. We want a table with the raw data in case something happens

create table layoffs_staging
like layoffs;

select * 
from layoffs_staging;

insert layoffs_staging
select *
from layoffs;

-- 1 remove Duplicates

# check for duplicates

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

-- one solution, which I think is a good one. Is to create a new column and add those row numbers in. Then delete where row numbers are over 2, then delete that column

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

-- now that we have this we can delete rows were row_num is greater than 1

delete
from layoffs_staging2
where row_num >1;

SET SQL_SAFE_UPDATES = 0; #just turning off the safe updates for further process

 -- 2 standardizing data
 
 -- if we look at industry it looks like we have some null and empty rows.
 
 select company,trim(company)
 from layoffs_staging2;
 
 update layoffs_staging2
 set company = trim(company);

select distinct industry
 from layoffs_staging2
 order by 1;

select *
 from layoffs_staging2
 where industry like 'Crypto%';
 
 -- we setting up the industry and country name as one which may have been written in an extended form by mistake or unknowingly.
 
 update layoffs_staging2
 set industry = 'Crypto'
 where industry like 'crypto%';
 
 select distinct country
 from layoffs_staging2
 order by 1;
 
 select * from layoffs_staging2
  where country like 'United States%'
  order by 1;
  
  select distinct country, trim(trailing  '.' from country)
 from layoffs_staging2
 order by 1;
 
 update layoffs_staging2
 set country = trim(trailing  '.' from country)
 where country like 'United States%';
 

select distinct country from layoffs_staging2
order by 1;

-- setting the date as date as it was defined as text

select `date`,
str_to_date(`date`,'%m/%d/%y')
from layoffs_staging2;

update layoffs_staging2
set `date` = str_to_date(`date`,'%m/%d/%Y') ;

Alter table layoffs_staging2
modify column `date` DATE;


-- 3 Null values and blank values

select*
from layoffs_staging2;

select * 
from layoffs_staging2
where total_laid_off is null 
and percentage_laid_off is null;

update layoffs_staging2
set industry = null
where industry = '';

select * from layoffs_staging2
where industry is null or industry = '';

select * from layoffs_staging2
where company = 'Airbnb';

select *
 from layoffs_staging2 t1
join layoffs_staging2  t2
on t1.company = t2.company
where (t1.industry is null or t1.industry = '')
and t2.industry is not null;

select t1.industry,t2.industry
 from layoffs_staging2 t1
join layoffs_staging2  t2
on t1.company = t2.company
where (t1.industry is null or t1.industry = '')
and t2.industry is not null;

-- we populated the industry null value with other industry name of same company.

update layoffs_staging2 t1
join layoffs_staging2 t2
on t1.company = t2.company
set t1.industry = t2.industry
where t1.industry is null
and t2.industry is not null;


select * 
from layoffs_staging2;

select * 
from layoffs_staging2
where total_laid_off is null 
and percentage_laid_off is null;

-- Delete Useless data we can't really use

delete 
from layoffs_staging2
where total_laid_off is null 
and percentage_laid_off is null;

select * 
from layoffs_staging2;


-- 4 remove column 

alter table layoffs_staging2
drop column row_num;