--- SQL Server for CIA World Factbook Data ---

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

This Docker SQL compiler was therefore the fastest way to get a clean dataset for my Tableau dashboard.

DATA QUALITY:

All data was obtained from the CIA World Factbook website, linked in the Dashboard.

Notably, the migrant data table is basically unusable, as the figures were not calculated correctly: 
    Both Ukraine and Peru are estimated to be losing people to migration right now,
    at about ~35 per 1,000 for Ukraine and ~1.5 per 1,000 for Peru. Unfortunately, in the CIA's table
    Ukraine has a positive migration rate; ie. Ukraine is erronously listed as gaining ~35 people per 1,000.
    Using Peru as a benchmark to read the table ("maybe they're all flipped from negative?") you can see that
    some migration rates were calculated correctly [(Immigration - Emigration)/Population], and others were
    flipped [(Emigration - Immigration)/Population]. The entire table needs to be redone to get correct values.

Finally, the CIA does not provide historical data beyond three years, and even then, it's in PDF form
optimized for print. If I was able to access historical or nested data for each country, I'd
be able to make a table for each year and animate changes over time, which would have been fun.
The compiler below can be run again in the future, assuming the schema and exportables have the same format.
*/

--- Build the master_reference table by joining all files ---
Drop table if exists
master_reference;

select 
Pop.region as Region,
Pop.name as Name,
Pop.value as Population,
Pop.ranking as Population_Rank,
RGDP.value as RGDP,
RGDP.date_of_information as RGDP_Year,
RGDP.ranking as RGDP_Rank,
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
WWW.value as Internet_Users,
WWW.date_of_information Int_Users_Year,
WWW.ranking as Internet_Users_rank,
LaFo.value as Labor_Force,
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
full join [Real GDP (purchasing power parity)] as RGDP on RGDP.slug = Pop.slug
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

Select * 
from master_reference;

--- Run tests to ensure joins & nulls are consistent ---


/* NEW FILE STARTS HERE? */

--- Copy master table ---
--- Drop test rows ---
--- Cleanse new working table of null rows ---

--- Math tables for derived values ---
--- Validate with date matching ---

--- full join Edu.date_of_information on RGDP.date_of_information
--- RGDP.[value] * (Edu.of_GDP/100) as Education_Budget
--- DENSE_RANK() Over(Order By RGDP.[value] * (Edu.of_GDP/100) DESC) as Education_Spend_Rank

--- Run tests to ensure functionality ---
--- Non-matching dates ---
--- Negative percentages ---
--- Null values ---