--- === SQL Server for CIA World Factbook Data === ---

---Create Insert Into section for test functionality & ability to add more files in future---

Drop table if exists master_reference;
Drop table if exists copied_data;
Drop table if exists cleansed_data;
drop table if exists derived_data;

---Create test rows for Unit Test 1

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
Gini.date_of_information as Gini_Year,
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

--- === Unit Test 1 conducted here: see logger === ---

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
    Gini_Year,
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
    TRY_CAST(REPLACE(Internet_Users, ',', '') AS DECIMAL(18, 2)) AS Clean_IU,
    TRY_CAST(LEFT(Gini_Year, 4) AS INT) AS Gini_y_valid,
    TRY_CAST(LEFT(RGDP_Year, 4) AS INT) AS RGDP_y_valid,
    TRY_CAST(LEFT(RGDP_G_Year, 4) AS INT) AS RGDP_G_y_valid,
    TRY_CAST(LEFT(PopG_Year, 4) AS INT) AS PopG_y_valid,
    TRY_CAST(LEFT(Inflation_Rate_Year, 4) AS INT) AS Inflation_Rate_y_valid,
    TRY_CAST(LEFT(Labor_Force_Year, 4) AS INT) AS Labor_Force_y_valid,
    TRY_CAST(LEFT(Unemployment_Rate_Year, 4) AS INT) AS Unemployment_Rate_y_valid,
    TRY_CAST(LEFT(GDPa_pct_Year, 4) AS INT) AS GDPa_pct_y_valid,
    TRY_CAST(LEFT(GDPi_pct_Year, 4) AS INT) AS GDPi_pct_y_valid,
    TRY_CAST(LEFT(GDPs_pct_Year, 4) AS INT) AS GDPs_pct_y_valid,
    TRY_CAST(LEFT(Int_Users_Year, 4) AS INT) AS Int_Users_y_valid,
    TRY_CAST(LEFT(Edu_Budget_Year, 4) AS INT) AS Edu_Budget_y_valid,
    TRY_CAST(LEFT(Mil_Budget_Year, 4) AS INT) AS Mil_Budget_y_valid,
    TRY_CAST(LEFT(Coal_Rev_Year, 4) AS INT) AS Coal_Rev_y_valid,
    TRY_CAST(LEFT(Emissions_Year, 4) AS INT) AS Emissions_y_valid,
    TRY_CAST(LEFT(GenCap_Year, 4) AS INT) AS GenCap_y_valid,
    TRY_CAST(LEFT(Consumption_Year, 4) AS INT) AS Consumption_y_valid
INTO cleansed_data
FROM copied_data;

---=== Validity of data types assessed in DQ script ===---

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
    Gini_Year,
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
/* Estimate sourcing (Department of Defense) linked in visualization*/
Update derived_data
Set Military_Budget_Est = 849800000000
Where Name = 'United States';

select * from derived_data;
