# the variable local _infile must be 1 inorder to load data from a file on mysql.
SHOW VARIABLES LIKE 'local_infile';
set global local_infile =1;


create table coviddeaths2(
iso_code text,	continent text,	location text,	date text,	population text, total_cases text,
	new_cases text, 	new_cases_smoothed text, 	total_deaths text,	new_deaths text,	new_deaths_smoothed text,	total_cases_per_million	text,new_cases_per_million text,	new_cases_smoothed_per_million text,	total_deaths_per_million text,	new_deaths_per_million text, 	new_deaths_smoothed_per_million	text, reproduction_rate text,	icu_patients text,	icu_patients_per_million text,	hosp_patients text,	hosp_patients_per_million text,	weekly_icu_admissions text,	weekly_icu_admissions_per_million text,	weekly_hosp_admissions text,	weekly_hosp_admissions_per_million text


);

#loading data from a csv file.
# file must be in the location     C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/CovidDeaths1.csv'
INTO TABLE coviddeaths2
FIELDS TERMINATED BY ','   #cloumns spparated by ,
ENCLOSED BY '"'   #text may be in ""
LINES TERMINATED BY '\n'   #row endining by \n 
IGNORE 1 ROWS;



select *
from coviddeaths2;

create table covidvaccination2 (
iso_code text,	continent text,	location text,	`date` text,	new_tests text,	total_tests	text, total_tests_per_thousand	text, new_tests_per_thousand text,
	new_tests_smoothed text,	new_tests_smoothed_per_thousand text,	positive_rate text,	
    tests_per_case text,	tests_units text,	total_vaccinations text,	people_vaccinated text,	
    people_fully_vaccinated text,	new_vaccinations text,	new_vaccinations_smoothed text,	total_vaccinations_per_hundred text,
	people_vaccinated_per_hundred text,	people_fully_vaccinated_per_hundred text,	new_vaccinations_smoothed_per_million text,
	stringency_index text,	population text,	population_density text,	median_age text,	aged_65_older text,	aged_70_older text,	gdp_per_capita text,	extreme_poverty text,	cardiovasc_death_rate text,	diabetes_prevalence	text, female_smokers text,	
    male_smokers text,	 handwashing_facilities text,	hospital_beds_per_thousand text,	life_expectancy text,	human_development_index text
);

load data infile 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/Covidvaccinations1.csv'
into table covidvaccination2
fields terminated by ','   # columns are separated by coma in csv file
enclosed by '"'  # test is in ""
lines terminated by '\n'  # rows ending with  \n
ignore 1 rows;  # 1st row is header row and is not the part of the data

select *
from covidvaccination2;


select *
from coviddeaths2;

#finding out the total deaths percentage wrt total_cases

select location,`date`,total_cases,total_deaths,(total_deaths/total_cases*100)
from coviddeaths2
where location like 'Afghanistan'
order by location, str_to_date(`date`,'%d/%m/%Y') ;




describe coviddeaths2;  # used to check the datatypes

# changing the data type of date from text to date

# just ordered by date
select `date` 
from coviddeaths2
order by str_to_date(`date`,'%d/%m/%Y') ;  

# added column new_date with datatype date
ALTER TABLE coviddeaths2
add column new_date date;   
# updating table new_date with data str_to_date(`date`,'%d/%m/%Y')
update coviddeaths2
set new_date = str_to_date(`date`,'%d/%m/%Y');

#now checking date
select `date`, new_date
from coviddeaths2;

# now deleting previous `date` column
alter table coviddeaths2
drop `date`;

# renaming the column new_date with date

alter table coviddeaths2
change new_date `date` date;  #last date is datatype

select *
from coviddeaths2;

# finding out the total deaths percentage wrt population

select location, `date`,total_deaths,population,(total_deaths/population*100) as death_percentage
from coviddeaths2
where location like 'United states'
order by `date`;

# finding out the total case vs the population 
select location, `date`,total_cases,population,(total_cases/population*100) as death_percentage
from coviddeaths2
where location like 'United states'
order by `date`;

# finding the maximum percentage of the cases vs the population or the infection percentage country wise

select location, max(total_cases) as total_infection_count,population,max((total_cases/population*100)) as infection_percentage
from coviddeaths2
group by location,population
order by infection_percentage desc;



# creating backup of the coviddeaths2  and covidvaccination2 so that the orginal data may be kept safe

create table coviddeaths3
like coviddeaths2;

insert into coviddeaths3
select *
from coviddeaths2;

create table covidvaccination3
like covidvaccination2;

insert into covidvaccination3
select *
from covidvaccination2;




# finding the maximum no of deaths country wise 

select *
from coviddeaths3;

select location, max(cast(total_deaths as signed)) as max_total_deaths  #results were not correct with total_deaths as it was text we have changed it to integer.in mysql signed is used for integer in cast
from coviddeaths3
where continent != ''  # in data  location  column contains continent names but at the same time the continent column is empty in these rows. so we are selecting data only where continent is not empty.
group by location
order by max_total_deaths desc;

select *
from coviddeaths3;

# on which date maximum deaths occured in each country
# using subquery
# the query written is correct however it will not execute as dataset is very large and it takes time. this can be achived using window function.
select location,`date`,new_deaths as maximum_deaths_in_a_day
from coviddeaths3
where (location, new_deaths) in (
select location, max(new_deaths)
from coviddeaths3
group by location
)
order by maximum_deaths_in_a_day desc;

# on which date maximum deaths occured in each country
# using window function
select location,`date`, maximum_deaths_in_a_day  # selecting  location,`date`, maximum_deaths_in_a_day from t1 table
from(
select location,`date`,new_deaths as maximum_deaths_in_a_day,  
row_number() over(
partition by location
order by cast(new_deaths as signed) desc
) as rn     # row_number is giving row_number based on the group location and order bt new_deaths desending.
from coviddeaths3
where continent != '' and new_deaths!= ''  # only shows results where continent is not empty and new_deaths are not empty
) as t1
where rn =1;   # shows rows where row number is 1 which is maxumum deaths in a day


#finding the maximum no of deaths in a day in each country
SELECT location, MAX(cast(new_deaths as signed)) AS maximum_deaths_in_a_day
FROM coviddeaths3
where continent != ''
GROUP BY location
ORDER BY maximum_deaths_in_a_day DESC;

#finding out the total deaths for the continents,total pouplation,total_deaths percentage wrt the population,total_deaths percentage wrt total_cases 

select *
from coviddeaths3;


select continent, sum(total_deaths),sum(total_cases),sum(total_deaths)/sum(total_cases)*100, sum(population), sum(total_deaths)/sum(population)*100
from coviddeaths3
where continent != ''
group by continent
order by sum(total_deaths) desc;


# checking total_population vs the vaccination

select *
from portfolioproject.coviddeaths3;

select *
from covidvaccination3;
# CREATING Bckup of covidvaccination3

create table covidvaccination4
like covidvaccination3;

INSERT INTO covidvaccination4
select *
from covidvaccination3;

select *
from covidvaccination4; 

describe covidvaccination4;                                                                                                                                      ovidvaccination4;

select `date`,str_to_date(`date`,'%d/%m/%Y')
from covidvaccination4;

# adding column covidvaccination4
alter table covidvaccination4 
add column new_date date;
# updating new_date with date values
update covidvaccination4
set new_date = str_to_date(`date`,'%d/%m/%Y');
# droping date column
alter table covidvaccination4
drop `date`;
# changing column name from new_date to date 
alter table covidvaccination4
change new_date `date` date;  # old, new name, datatype

select dea.location,dea.population,max(vac.total_vaccinations),max(vac.total_vaccinations)/dea.population*100
from coviddeaths3 as dea
join covidvaccination4 as vac
on dea.location=vac.location and 
dea.date = vac.date
where dea.continent != ''
group by dea.location,dea.population
order by dea.location;

#anouther way of doing the same total_population vs the vaccination through CTE
with VacvsPop (location,date,population,rolling_total_vaccination) as(
select dea.location,dea.date,cast(dea.population as signed),
sum(cast(vac.new_vaccinations as signed)) over(partition by dea.location order by dea.date) as rolling_total_vaccination
from coviddeaths3 as dea
join covidvaccination4 as vac
on dea.location=vac.location and 
dea.date = vac.date
where dea.continent != ''
order by dea.location

)

select *, rolling_total_vaccination/population*100
from VacvsPop;

# another way of doing the same  total_population vs the vaccination through Table

describe covidvaccination3;
describe covidvaccination4;
# creating backup of covidvaccination4

create table covidvaccination5
like covidvaccination4;

insert into covidvaccination5
select *
from covidvaccination4;


select new_vaccinations,trim(new_vaccinations),cast(nullif(trim(new_vaccinations),'') as signed)
from covidvaccination5;

update covidvaccination5
set new_vaccinations =cast(nullif(trim(new_vaccinations),'') as signed);

alter table covidvaccination5
modify new_vaccinations int;

select population
from coviddeaths3
where population = '';

update coviddeaths3
set population = null
where population = '';



SELECT *
FROM coviddeaths3
wHERE population NOT REGEXP '^[0-9]+$'; # returns the rows where population is not numeric. ^ is showing string start and $ indicate string end and [0-9]+ means the numbers any numeric value


create table coviddeaths4
like coviddeaths3;
alter table coviddeaths4
modify population bigint;
 
 insert into coviddeaths4
 select *
 from coviddeaths3;
 
 describe coviddeaths4;


create table VacvsPop(
location nvarchar(255),
`date` date,
population  int,
rolling_total_vaccination int

);
insert into VacvsPop(
select dea.location,dea.date,dea.population ,
sum(
    vac.new_vaccinations
) 
over(partition by dea.location order by dea.date) as rolling_total_vaccination
from coviddeaths4 as dea
join covidvaccination5 as vac
on dea.location=vac.location and 
dea.date = vac.date
where dea.continent != ''
order by dea.location

);

select *,rolling_total_vaccination/population *100
from VacvsPop;

# changing the datatype
describe coviddeaths4;

alter table coviddeaths4
modify new_cases int;

select new_cases
from coviddeaths4
where new_cases= '';

update coviddeaths4
set new_cases = null
where new_cases = '';

# creating VIEW . VIEW is a virtual table that displays data from the query of sql. it displays data from other table.
# view is permanent and can be used as many times as required and it is used to for simplicity, added security such as to hide some column for the users like salary, abstraction etc
# view PercentagePopulationVaccinated is created here to find out percentage people vaccinated for each country 
create view PercentagePopulationVaccinated as
select location, `date`, population, rolling_total_vaccination,rolling_total_vaccination/population *100
from vacvspop
where  rolling_total_vaccination is not null and rolling_total_vaccination/population *100 is not null;

select location,population,max(rolling_total_vaccination),max(rolling_total_vaccination/population *100)
from percentagepopulationvaccinated
group by location,population;


