--- === SQL Server for CIA World Factbook Data === ---

/*
DEVELOPER NOTES: 

Below is a SQL script that transforms CSV files from the CIA World Factbook website for use in Tableau.

This is an example of a SQL script that can ingest, test, and export raw CSV data into 
a relational, clean format.

SETUP & ETL:

SQL Queries were run on a contained server using Docker. 

Data ingestion to SQL was achieved using Azure Data Studio's GUI; this is not my preferred method.
Ideally, I'd have an automated pipeline to show off using Airflow. However: 
The CIA does not provide any other exportables beyond static CSV files. Each has confusing
columns and each table, while related, has no relational structure that can be exported.

This Docker SQL script was therefore the fastest way to get a clean dataset for my Tableau dashboard.

DATA QUALITY:

All data was obtained from the CIA World Factbook website, linked in the Dashboard under "Data Source".

Notably, the migrant data table is basically unusable, as the figures were not calculated correctly: 
    Both Ukraine and Peru are estimated to be losing people to migration right now,
    at about ~35 per 1,000 for Ukraine and ~1.5 per 1,000 for Peru. Unfortunately, in the CIA's table
    Ukraine has a positive migration rate; ie. Ukraine is erronously listed as gaining ~35 people per 1,000.
    Using Peru as a benchmark to read the table ("maybe they're all flipped from negative?") you can see that
    some migration rates were calculated correctly [(Immigration - Emigration)/Population], and others were
    flipped [(Emigration - Immigration)/Population]. The entire migration table needs to be redone by the CIA 
    to get correct values.

Finally, the CIA does not provide historical data beyond three years, and even then, it's in PDF form
optimized for print. If I was able to access historical, nested data for each country, I'd
be able to make a table for each year and animate changes over time, which would have been fun.
Instead, this script is a less-automated means of joining exportable flat files.
*/

---Create Insert Into section for test functionality & ability to add more files in future---

Drop table if exists master_reference;
Drop table if exists copied_data;
Drop table if exists cleansed_data;

Delete from [Population - total] Where slug = 'sample-slug';
Delete from [Real GDP (purchasing power parity)] where slug = 'sample-slug';
Delete from [Real GDP per capita] Where slug = 'sample-slug';

INSERT INTO [Population - total] (slug, region, name, value, ranking)
VALUES ('sample-slug', 'Sample Region', 'Sample Name', 500000, 250);

INSERT INTO [Real GDP (purchasing power parity)] (slug, region, name, value, date_of_information, ranking)
VALUES ('sample-slug', 'Sample Region', 'Sample Name', 300000000, '2022', 250);

INSERT INTO [Real GDP per capita] (slug, region, name, value, date_of_information, ranking)
VALUES ('sample-slug', 'Sample Region', 'Sample Name', 20000, '2022', 250);

--- === Build the master_reference table by joining all files === ---

select 
Pop.region as Region,
Pop.name as Name,
REPLACE(Pop.value, ',', '') as Population, 
Pop.ranking as Population_Rank,
RGDPppp.value as RGDP,
RGDPppp.date_of_information as RGDP_Year,
RGDPppp.ranking as RGDP_Rank,
RGDP_pc.value as RGDP_Per_Capita,
RGDP_pc.date_of_information as RGDP_Per_Capita_Year,
RGDP_pc.ranking as RGDP_Per_Capita_Rank,
RGDP_g.Column_3 as RGDP_Growth_Rate,
RGDP_g.date_of_information as RGDP_G_Year,
RGDP_g.ranking as RGDP_Growth_Rank,
CO2e.metric_tonnes_of_CO2 as CO2_Emissions_mTonnes,
CO2e.date_of_information as Emissions_Year,
CO2e.ranking as Emissions_Rank,
Edu.of_GDP as Education_Budget_Pct,
Edu.date_of_information as Edu_Budget_Year,
eCap.kW as Installed_Generating_Capacity_kW,
eCap.date_of_information as GenCap_Year,
eCap.ranking as GenCap_Rank,
eConsume_pc.Btu_person as Energy_Consumption_Per_Capita_btu,
eConsume_pc.date_of_information as Consumption_Year,
eConsume_pc.ranking as Energy_Consumption_pc_Rank,
GDPa.Column_3 as GDP_Pct_Agricultural,
GDPa.date_of_information as GDPa_pct_Year,
GDPi.Column_3 as GDP_Pct_Industrial,
GDPi.date_of_information as GDPi_pct_Year,
GDPs.Column_3 as GDP_Pct_Services,
GDPs.date_of_information as GDPs_pct_Year,
Gini.value as Gini_Index_Coefficient,
Gini.date_of_information as Gini_date,
Gini.ranking as Gini_Rank,
Infl.Column_3 as Inflation_Rate_YoY_Consumer_Prices,
Infl.date_of_information as Inflation_Rate_Year,
Infl.ranking as Inflation_rank,
REPLACE(WWW.value, ',', '') as Internet_Users,
WWW.date_of_information as Int_Users_Year,
WWW.ranking as Internet_Users_rank,
REPLACE(LaFo.value, ',', '') as Labor_Force,
LaFo.date_of_information as Labor_Force_Year,
LaFo.ranking as Labor_Force_rank,
Mil.of_GDP as Military_budget_pct,
Mil.date_of_information as Mil_Budget_Year,
Mig.migrant_s_1 as Net_Migration_Rate_issue,
Mig.date_of_information as Mig_rank_issue,
Mig._000_population as Mig_date_issue,
PopG.Column_3 as Population_Growth_Rate,
PopG.date_of_information as PopG_Year,
CoalRev.of_GDP as Coal_Revenue_Pct_GDP,
CoalRev.date_of_information as Coal_Rev_Year,
Unemp.Unemployment_Rate,
Unemp.Last_Updated_UR as Unemployment_Rate_Year

INTO master_reference

from [Population - total] as Pop
full join [Real GDP (purchasing power parity)] as RGDPppp on RGDPppp.slug = Pop.slug
full join [Real GDP per capita] as RGDP_pc on RGDP_pc.slug = Pop.slug
full join [Real GDP growth rate] as RGDP_g on RGDP_g.slug = Pop.slug
full join Area on Area.slug = Pop.slug --- Not included
full join [Carbon dioxide emissions] as CO2e on CO2e.slug = Pop.slug
full join Education as Edu on Edu.slug = Pop.slug
full join [Electricity capacity] as eCap on eCap.slug = Pop.slug
full join [Energy consumption per capita] as eConsume_pc on eConsume_pc.slug = Pop.slug
full join [GDP - composition, by sector of origin - agriculture] as GDPa on GDPa.slug = Pop.slug
full join [GDP - composition, by sector of origin - industry] as GDPi on GDPi.slug = Pop.slug
full join [GDP - composition, by sector of origin - services] as GDPs on GDPs.slug = Pop.slug
full join [Gini Index coefficient - distribution of family income] as Gini on Gini.slug = Pop.slug
full join [Inflation rate (consumer prices)] as Infl on Infl.slug = Pop.slug
full join [Internet users] as WWW on WWW.slug = Pop.slug
full join [Labor force] as LaFo on LaFo.slug = Pop.slug
full join [Military expenditures] as Mil on Mil.slug = Pop.slug
full join [Net migration rate] as Mig on Mig.slug = Pop.slug --- Table contains errors
full join [Population growth rate] as PopG on PopG.slug = Pop.slug
full join [Revenue from coal] as CoalRev on CoalRev.slug = Pop.slug
full join [Unemployment rate] as Unemp on Unemp.slug = Pop.slug
full join [Youth unemployment rate (ages 15-24)] as yUnemp on yUnemp.slug = Pop.slug --- Not included
;

--- === Unit Test 1 === ---
-- Check that data joined correctly in `master_reference`
DECLARE @test_passed BIT = 1;
DECLARE @expectedRegion NVARCHAR(50) = 'Sample Region';
DECLARE @expectedPopulation INT = 500000;
DECLARE @expectedRGDP DECIMAL(18, 2) = 300000000;

IF NOT EXISTS (
    SELECT 1
    FROM master_reference
    WHERE Region = @expectedRegion
      AND Population = @expectedPopulation
      AND RGDP = @expectedRGDP
)
BEGIN
    PRINT 'Test Failed: Expected data not found in master_reference.';
    SET @test_passed = 0;
END

-- Final test result output
IF @test_passed = 1
BEGIN
    PRINT 'Test Passed: master_reference is correctly populated.';
END
ELSE
BEGIN
    PRINT 'Test Failed: Check output for issues.';
END

--- master_reference behaves as expected

--- === copy data from master_refernce for transformation === ---

--- Copy master table
--- Drop test rows
--- Cleanse new working table of null rows

SELECT
    Region,
    Name,
    Population,
    Population_Rank,
    RGDP,
    RGDP_Year,
    RGDP_Rank,
    RGDP_Per_Capita,
    RGDP_Per_Capita_Rank,
    RGDP_Growth_Rate,
    RGDP_G_Year,
    RGDP_Growth_Rank,
    Population_Growth_Rate,
    PopG_Year,
    Gini_Index_Coefficient,
    Gini_Rank,
    Gini_date,
    Inflation_Rate_YoY_Consumer_Prices,
    Inflation_Rate_Year,
    Inflation_rank,
    
    -- Derive Unemployed Population Estimate
    Labor_Force,
    Labor_Force_Year,
    Labor_Force_rank,
    Unemployment_Rate,
    Unemployment_Rate_Year,

    -- These need derivatives from RGDP
    GDP_Pct_Agricultural,
    GDPa_pct_Year,
    GDP_Pct_Industrial,
    GDPi_pct_Year,
    GDP_Pct_Services,
    GDPs_pct_Year,
    
    -- Express as a percentage of population next to GDP sector mix
    Internet_Users,
    Int_Users_Year,
    Internet_Users_rank,

    -- Derive from GDP
    Education_Budget_Pct,
    Edu_Budget_Year,
    Military_budget_pct,
    Mil_Budget_Year,
    Coal_Revenue_Pct_GDP,

    Coal_Rev_Year,
    CO2_Emissions_mTonnes,
    Emissions_Year,
    Emissions_Rank,
    Installed_Generating_Capacity_kW,
    GenCap_Year,
    GenCap_Rank,
    Energy_Consumption_Per_Capita_btu,
    Consumption_Year,
    Energy_Consumption_pc_Rank
    -- Skipping Migration rate bc DQ issue
Into copied_data
FROM master_reference
WHERE Name <> 'Sample Name'
  And Name is not null
  AND Region IS NOT NULL 
  AND Population IS NOT NULL;

--- === Cleanse dataset for use in Tableau === ---
--- Rename Turkey for mapping
Update copied_data
Set Name = 'Turkey'
Where Name = 'Turkey (Turkiye)';

--- Remove commas from numerical datasets to enable calculations
SELECT
    *,
    TRY_CAST(REPLACE(Population, ',', '') AS INT) AS Clean_Pop,
    TRY_CAST(REPLACE(Labor_Force, ',', '') AS DECIMAL(18, 2)) AS Clean_LF,
    TRY_CAST(REPLACE(Internet_Users, ',', '') AS DECIMAL(18, 2)) AS Clean_IU
INTO cleansed_data
FROM copied_data;

-- Final result to validate data and derive columns
select 
    Region,
    Name,
    Clean_Pop,
    Population_Rank,
    RGDP,
    RGDP_Year,
    RGDP_Rank,
    RGDP_Per_Capita,
    RGDP_Per_Capita_Rank,
    RGDP_Growth_Rate,
    RGDP_G_Year,
    RGDP_Growth_Rank,
    Population_Growth_Rate,
    PopG_Year,
    Gini_Index_Coefficient,
    Gini_Rank,
    Gini_date,
    Inflation_Rate_YoY_Consumer_Prices,
    Inflation_Rate_Year,
    Inflation_rank,
    
    -- Derive Unemployed Population Estimate
    Clean_LF,
    Labor_Force_Year,
    Labor_Force_rank,
    Unemployment_Rate,
    Unemployment_Rate_Year,

    -- These need derivatives from RGDP
    GDP_Pct_Agricultural,
    GDPa_pct_Year,
    GDP_Pct_Industrial,
    GDPi_pct_Year,
    GDP_Pct_Services,
    GDPs_pct_Year,
    
    -- Express as a percentage of population next to GDP sector mix
    Clean_IU,
    Int_Users_Year,
    Internet_Users_rank,

    -- Derive from GDP
    Education_Budget_Pct,
    Edu_Budget_Year,
    Military_budget_pct,
    Mil_Budget_Year,
    Coal_Revenue_Pct_GDP,
    Coal_Rev_Year,

    CO2_Emissions_mTonnes,
    Emissions_Year,
    Emissions_Rank,
    Installed_Generating_Capacity_kW,
    GenCap_Year,
    GenCap_Rank,
    Energy_Consumption_Per_Capita_btu,
    Consumption_Year,
    Energy_Consumption_pc_Rank,
    -- Skipping Migration rate bc DQ issue
    CASE 
    WHEN Edu_Budget_Year = RGDP_Year THEN RGDP * (Education_Budget_Pct / 100)
    ELSE NULL 
    END AS Education_Budget_Est,

    CASE 
    WHEN Mil_Budget_Year = RGDP_Year THEN RGDP * (Military_budget_pct / 100)
    ELSE NULL 
    END AS Military_Budget_Est,

    CASE --- not included
    WHEN Coal_Rev_Year = RGDP_Year THEN RGDP * (Coal_Revenue_Pct_GDP / 100)
    ELSE NULL 
    END AS Coal_Revenue_Est,

    CASE 
    WHEN Int_Users_Year > 2020 THEN Clean_IU / Clean_Pop
    ELSE NULL 
    END AS pct_Population_with_internet_Est,

    CASE 
    WHEN Labor_Force_Year = Unemployment_Rate_Year THEN Clean_LF * (Unemployment_Rate / 100)
    ELSE NULL 
    END AS Unemployed_Population_Est,

    CASE --- not included
    WHEN GDPa_pct_Year = RGDP_Year THEN RGDP * (GDP_Pct_Agricultural / 100)
    ELSE NULL 
    END AS Agricultural_Product_Est,

    CASE --- not included
    WHEN GDPi_pct_Year = RGDP_Year THEN RGDP * (GDP_Pct_Industrial / 100)
    ELSE NULL 
    END AS Industrial_Product_Est,

    CASE --- not included
    WHEN GDPs_pct_Year = RGDP_Year THEN RGDP * (GDP_Pct_Services / 100)
    ELSE NULL 
    END AS Services_Product_Est
into derived_data
from cleansed_data;

--- Insert US Defense Spending Estimate (essential row) to table
/* Estimate sourcing (the IMF & Department of Defense) linked in visualization*/
Update derived_data
Set Military_Budget_Est = 8498000000000
Where Name = 'United States';

--- === Unit Test 2 === ---
--- Run tests to ensure functionality ---
 --- Non-matching dates ---
 --- Negative percentages ---
 --- Null values ---

---=== Data Quality Metrics ===---
drop table if exists data_quality
drop table if exists dq_completeness

-- Completeness Check for derived_data
SELECT 
    COUNT(Name) AS Countries_and_territories,

    -- Completeness checks for each column
    COUNT(Region) / COUNT(Name) AS Region_completeness,
    COUNT(Name) / COUNT(Name) AS Name_completeness,
    COUNT(Clean_Pop) / COUNT(Name) AS Clean_Pop_completeness,
    COUNT(Population_Rank) / COUNT(Name) AS Population_Rank_completeness,
    COUNT(RGDP) / COUNT(Name) AS RGDP_completeness,
    COUNT(RGDP_Year) / COUNT(Name) AS RGDP_Year_completeness,
    COUNT(RGDP_Rank) / COUNT(Name) AS RGDP_Rank_completeness,
    COUNT(RGDP_Per_Capita) / COUNT(Name) AS RGDP_Per_Capita_completeness,
    COUNT(RGDP_Per_Capita_Rank) / COUNT(Name) AS RGDP_Per_Capita_Rank_completeness,
    COUNT(RGDP_Growth_Rate) / COUNT(Name) AS RGDP_Growth_Rate_completeness,
    COUNT(RGDP_G_Year) / COUNT(Name) AS RGDP_G_Year_completeness,
    COUNT(RGDP_Growth_Rank) / COUNT(Name) AS RGDP_Growth_Rank_completeness,
    COUNT(Population_Growth_Rate) / COUNT(Name) AS Population_Growth_Rate_completeness,
    COUNT(PopG_Year) / COUNT(Name) AS PopG_Year_completeness,
    COUNT(Gini_Index_Coefficient) / COUNT(Name) AS Gini_Index_Coefficient_completeness,
    COUNT(Gini_Rank) / COUNT(Name) AS Gini_Rank_completeness,
    COUNT(Gini_date) / COUNT(Name) AS Gini_date_completeness,
    COUNT(Inflation_Rate_YoY_Consumer_Prices) / COUNT(Name) AS Inflation_Rate_YoY_Consumer_Prices_completeness,
    COUNT(Inflation_Rate_Year) / COUNT(Name) AS Inflation_Rate_Year_completeness,
    COUNT(Inflation_rank) / COUNT(Name) AS Inflation_rank_completeness,
    COUNT(Clean_LF) / COUNT(Name) AS Clean_LF_completeness,
    COUNT(Labor_Force_Year) / COUNT(Name) AS Labor_Force_Year_completeness,
    COUNT(Labor_Force_rank) / COUNT(Name) AS Labor_Force_rank_completeness,
    COUNT(Unemployment_Rate) / COUNT(Name) AS Unemployment_Rate_completeness,
    COUNT(Unemployment_Rate_Year) / COUNT(Name) AS Unemployment_Rate_Year_completeness,
    COUNT(GDP_Pct_Agricultural) / COUNT(Name) AS GDP_Pct_Agricultural_completeness,
    COUNT(GDPa_pct_Year) / COUNT(Name) AS GDPa_pct_Year_completeness,
    COUNT(GDP_Pct_Industrial) / COUNT(Name) AS GDP_Pct_Industrial_completeness,
    COUNT(GDPi_pct_Year) / COUNT(Name) AS GDPi_pct_Year_completeness,
    COUNT(GDP_Pct_Services) / COUNT(Name) AS GDP_Pct_Services_completeness,
    COUNT(GDPs_pct_Year) / COUNT(Name) AS GDPs_pct_Year_completeness,
    COUNT(Clean_IU) / COUNT(Name) AS Clean_IU_completeness,
    COUNT(Int_Users_Year) / COUNT(Name) AS Int_Users_Year_completeness,
    COUNT(Internet_Users_rank) / COUNT(Name) AS Internet_Users_rank_completeness,
    COUNT(Education_Budget_Pct) / COUNT(Name) AS Education_Budget_Pct_completeness,
    COUNT(Edu_Budget_Year) / COUNT(Name) AS Edu_Budget_Year_completeness,
    COUNT(Military_budget_pct) / COUNT(Name) AS Military_budget_pct_completeness,
    COUNT(Mil_Budget_Year) / COUNT(Name) AS Mil_Budget_Year_completeness,
    COUNT(Coal_Revenue_Pct_GDP) / COUNT(Name) AS Coal_Revenue_Pct_GDP_completeness,
    COUNT(Coal_Rev_Year) / COUNT(Name) AS Coal_Rev_Year_completeness,
    COUNT(CO2_Emissions_mTonnes) / COUNT(Name) AS CO2_Emissions_mTonnes_completeness,
    COUNT(Emissions_Year) / COUNT(Name) AS Emissions_Year_completeness,
    COUNT(Emissions_Rank) / COUNT(Name) AS Emissions_Rank_completeness,
    COUNT(Installed_Generating_Capacity_kW) / COUNT(Name) AS Installed_Generating_Capacity_kW_completeness,
    COUNT(GenCap_Year) / COUNT(Name) AS GenCap_Year_completeness,
    COUNT(GenCap_Rank) / COUNT(Name) AS GenCap_Rank_completeness,
    COUNT(Energy_Consumption_Per_Capita_btu) / COUNT(Name) AS Energy_Consumption_Per_Capita_btu_completeness,
    COUNT(Consumption_Year) / COUNT(Name) AS Consumption_Year_completeness,
    COUNT(Energy_Consumption_pc_Rank) / COUNT(Name) AS Energy_Consumption_pc_Rank_completeness,
    COUNT(Education_Budget_Est) / COUNT(Name) AS Education_Budget_Est_completeness,
    COUNT(Military_Budget_Est) / COUNT(Name) AS Military_Budget_Est_completeness,
    COUNT(Coal_Revenue_Est) / COUNT(Name) AS Coal_Revenue_Est_completeness,
    COUNT(pct_Population_with_internet_Est) / COUNT(Name) AS pct_Population_with_internet_Est_completeness,
    COUNT(Unemployed_Population_Est) / COUNT(Name) AS Unemployed_Population_Est_completeness,
    COUNT(Agricultural_Product_Est) / COUNT(Name) AS Agricultural_Product_Est_completeness,
    COUNT(Industrial_Product_Est) / COUNT(Name) AS Industrial_Product_Est_completeness,
    COUNT(Services_Product_Est) / COUNT(Name) AS Services_Product_Est_completeness
into dq_completeness
FROM derived_data;

SELECT 
    -- Completeness checks for "_year" or "_Year" columns
    COUNT(RGDP_Year) / COUNT(Name) AS RGDP_Year_completeness,
    COUNT(RGDP_G_Year) / COUNT(Name) AS RGDP_G_Year_completeness,
    COUNT(PopG_Year) / COUNT(Name) AS PopG_Year_completeness,
    COUNT(Inflation_Rate_Year) / COUNT(Name) AS Inflation_Rate_Year_completeness,
    COUNT(Labor_Force_Year) / COUNT(Name) AS Labor_Force_Year_completeness,
    COUNT(Unemployment_Rate_Year) / COUNT(Name) AS Unemployment_Rate_Year_completeness,
    COUNT(GDPa_pct_Year) / COUNT(Name) AS GDPa_pct_Year_completeness,
    COUNT(GDPi_pct_Year) / COUNT(Name) AS GDPi_pct_Year_completeness,
    COUNT(GDPs_pct_Year) / COUNT(Name) AS GDPs_pct_Year_completeness,
    COUNT(Int_Users_Year) / COUNT(Name) AS Int_Users_Year_completeness,
    COUNT(Edu_Budget_Year) / COUNT(Name) AS Edu_Budget_Year_completeness,
    COUNT(Mil_Budget_Year) / COUNT(Name) AS Mil_Budget_Year_completeness,
    COUNT(Coal_Rev_Year) / COUNT(Name) AS Coal_Rev_Year_completeness,
    COUNT(Emissions_Year) / COUNT(Name) AS Emissions_Year_completeness,
    COUNT(GenCap_Year) / COUNT(Name) AS GenCap_Year_completeness,
    COUNT(Consumption_Year) / COUNT(Name) AS Consumption_Year_completeness
into dq_timeliness
from derived_data;

DQ_overall