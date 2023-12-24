-- Data Collection

# Create tables
CREATE TABLE coviddeath(
	iso_code TEXT,
	continent TEXT,
    location TEXT,
    date TEXT,
    population BIGINT,
    total_cases BIGINT,
    new_cases INT,
    new_cases_smoothed FLOAT,
    total_deaths INT,
    new_deaths INT,
    new_deaths_smoothed FLOAT,
    total_cases_per_million FLOAT,
    new_cases_per_million FLOAT,
    new_cases_smoothed_per_million FLOAT,
    total_deaths_per_million FLOAT,
    new_deaths_per_million FLOAT,
    new_deaths_smoothed_per_million FLOAT,
    reproduction_rate FLOAT,
    icu_patients INT,
    icu_patients_per_million FLOAT,
    hosp_patients INT,
    hosp_patients_per_million FLOAT,
    weekly_icu_admissions INT,
    weekly_icu_admissions_per_million FLOAT,
    weekly_hosp_admissions INT,
    weekly_hosp_admissions_per_million FLOAT
);

CREATE TABLE covidvaccination(
	iso_code TEXT,
	continent TEXT,
    location TEXT,
    date TEXT,
    total_tests BIGINT,
    new_tests INT,
    total_tests_per_thousand FLOAT,
    new_tests_per_thousand FLOAT,
    new_tests_smoothed FLOAT,
    new_tests_smoothed_per_thousand FLOAT,
    positive_rate FLOAT,
    tests_per_case FLOAT,
    tests_units INT,
    total_vaccinations BIGINT,
	people_vaccinated BIGINT,
	people_fully_vaccinated BIGINT,
    total_boosters BIGINT,
    new_vaccinations BIGINT,
	new_vaccinations_smoothed BIGINT,
	total_vaccinations_per_hundred FLOAT,
	people_vaccinated_per_hundred FLOAT,
	people_fully_vaccinated_per_hundred FLOAT,
    total_boosters_per_hundred FLOAT,
	new_vaccinations_smoothed_per_million BIGINT,
    new_people_vaccinated_smoothed FLOAT,
    new_people_vaccinated_smoothed_per_hundred FLOAT
);

# LOAD data through MySQL command line
SET GLOBAL local_infile = on;

LOAD DATA LOCAL INFILE '/Users/hanzi/Desktop/CovidDeaths.csv'
INTO TABLE coviddeath
FIELDS TERMINATED BY ','
IGNORE 1 ROWS;

LOAD DATA LOCAL INFILE '/Users/hanzi/Desktop/CovidVaccinations.csv'
INTO TABLE covidvaccination
FIELDS TERMINATED BY ','
IGNORE 1 ROWS;


-- Data Cleaning

# Check for duplicate records
SELECT COUNT(*)
FROM coviddeath
GROUP BY location,date
HAVING COUNT(*) > 1;

SELECT COUNT(*)
FROM covidvaccination
GROUP BY location,date
HAVING COUNT(*)>1;

# Convert empty string value to Null in the "continent" column
SET sql_safe_updates = 0;
UPDATE coviddeath SET continent = NULL WHERE continent = '';


-- Exploratory Data Analysis

# Check the shape of the two dataset
SELECT COUNT(*)
FROM coviddeath;

SELECT COUNT(*)
FROM covidvaccination;

# Death toll break down
SELECT location,SUM(new_deaths) AS highest_death_count, ROUND(SUM(new_deaths)/SUM(SUM(new_deaths)) OVER()*100,2) AS percentage
FROM coviddeath
WHERE continent IS NOT NULL
GROUP BY location
ORDER BY highest_death_count DESC;

# Ranking countries in each continent by death rate using dense_rank
WITH cte AS(
SELECT continent,location,SUM(new_deaths) AS total_deaths,SUM(new_cases) AS total_cases,CONCAT(ROUND(SUM(new_deaths)/SUM(new_cases)*100,2),'%') AS death_rate
FROM coviddeath
WHERE continent IS NOT NULL
GROUP BY continent,location
)
SELECT continent,location,total_deaths,total_cases,death_rate,DENSE_RANK() OVER(PARTITION BY continent ORDER BY death_rate DESC)AS ranking
FROM cte;

# Ranking countries by infection rate
SELECT continent,location, CONCAT(ROUND(MAX(total_cases/population)*100,2),'%') AS infection_rate
FROM coviddeath
WHERE continent IS NOT NULL
GROUP BY continent,location
ORDER BY MAX(total_cases/population) DESC;

# Create a view to retrieve information from the two tables
CREATE VIEW `dea_vac` AS
SELECT dea.continent AS continent,dea.location AS location,dea.date AS date,dea.population AS population,dea.new_cases AS new_cases,dea.new_deaths AS new_deaths,
vac.new_vaccinations AS new_vaccinations, vac.people_vaccinated AS people_vaccinated, vac.people_fully_vaccinated AS people_fully_vaccinated
FROM coviddeath dea
INNER JOIN covidvaccination vac
ON dea.location = vac.location AND
dea.date = vac.date
WHERE dea.continent IS NOT NULL;

# Look at global vaccinations
SELECT continent,location,date,new_vaccinations,
	AVG(new_vaccinations) OVER(PARTITION BY location ORDER BY date) AS moving_avg_new_vaccination,
    SUM(new_vaccinations) OVER(PARTITION BY location ORDER BY date) AS total_vaccinations,
    ROUND(people_vaccinated / population*100,2) AS percentage_1_dose,
	ROUND(people_fully_vaccinated / population*100,2) AS percentage_fully_vaccinated
FROM dea_vac;

# Ranking countries based on percentage_1_dose
SELECT continent,location,MAX(ROUND(people_vaccinated / population*100,2)) AS percentage_1_dose
FROM dea_vac
GROUP BY continent,location
ORDER BY percentage_1_dose DESC
LIMIT 10;

# Ranking countries based on percentage_fully_vaccinated
SELECT continent,location,MAX(ROUND(people_fully_vaccinated / population*100,2)) AS percentage_fully_vaccinated
FROM dea_vac
GROUP BY continent,location
ORDER BY percentage_fully_vaccinated DESC
LIMIT 10;

# Fully vaccinated people break down by countries
SELECT continent,location,people_fully_vaccinated,ROUND(people_fully_vaccinated/SUM(people_fully_vaccinated)OVER()*100,2) AS percentage
FROM dea_vac
WHERE date = (
SELECT MAX(date) FROM dea_vac 
AS Subquery)  
ORDER BY percentage DESC;

# Look at Canada's data
SELECT location,date,population,
	SUM(new_cases) OVER(ORDER BY date) AS total_cases,
    SUM(new_deaths) OVER(ORDER BY date) AS total_deaths,
    ROUND(people_vaccinated / population*100,2) AS percentage_1_dose,
    ROUND(people_fully_vaccinated / population*100,2) AS percentage_fully_vaccinated
FROM dea_vac
WHERE location = 'Canada';

