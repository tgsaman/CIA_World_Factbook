/*

DATA QUALITY:

These queries were used to generate excel workbooks of metadata that was used to derive a Data Quality Score. 

All data was obtained from the CIA World Factbook website, linked in the Dashboard under "Data Source".

In each case, propities about the tables developed, such as the ratio of data to nulls, the appropriateness of the 
saved data type (eg. text or integer) and the recency of that data (ed. 2023 vs 2016). 

More information about the dimensions of data quality, used to derive this score can be found here:
https://dama-nl.org/wp-content/uploads/2020/09/DDQ-Dimensions-of-Data-Quality-Research-Paper-version-1.2-d.d.-3-Sept-2020.pdf

Notably, the migrant data table is basically unusable, as the figures were not calculated correctly: 
    Both Ukraine and Peru are estimated to be losing people to migration right now,
    at about ~35 per 1,000 for Ukraine and ~1.5 per 1,000 for Peru. Unfortunately, in the CIA's table
    Ukraine has a positive migration rate; ie. Ukraine is erronously listed as gaining ~35 people per 1,000.
    Using Peru as a benchmark to read the table ("maybe they're all flipped from negative?") you can see that
    some migration rates were calculated correctly [(Immigration - Emigration)/Population], and others were
    flipped [(Emigration - Immigration)/Population]. The entire migration table needs to be redone by the CIA 
    to get correct values.

Finally, the CIA does not provide historical data or track their changes to these tables. 
Adding that additional functionality to this project is out of scope for what I'm trying to do. 
If you really want to see that, though, shoot me an email with a contract or job offer. 
*/

---=== Data Quality Metrics ===---
drop table if exists dq_completeness;

DROP TABLE IF EXISTS dq_validity;

DROP TABLE IF EXISTS dq_consistency

DROP TABLE IF EXISTS dq_timeliness_raw;
DROP TABLE IF EXISTS RGDP_timeliness;
DROP TABLE IF EXISTS RGDP_G_timeliness;
DROP TABLE IF EXISTS PopG_timeliness;
DROP TABLE IF EXISTS Inflation_timeliness;
DROP TABLE IF EXISTS Labor_Force_timeliness;
DROP TABLE IF EXISTS Unemployment_timeliness;
DROP TABLE IF EXISTS GDPa_pct_timeliness;
DROP TABLE IF EXISTS GDPi_pct_timeliness;
DROP TABLE IF EXISTS GDPs_pct_timeliness;
DROP TABLE IF EXISTS Int_Users_timeliness;
DROP TABLE IF EXISTS Edu_Budget_timeliness;
DROP TABLE IF EXISTS Mil_Budget_timeliness;
DROP TABLE IF EXISTS Coal_Rev_timeliness;
DROP TABLE IF EXISTS Emissions_timeliness;
DROP TABLE IF EXISTS GenCap_timeliness;
DROP TABLE IF EXISTS Consumption_timeliness;

--- Validity Check for derived_data
SELECT 
    COLUMN_NAME AS ColumnName,
    DATA_TYPE AS DataType,
    CASE
        --- Test string values' validitiy
        WHEN COLUMN_NAME IN ('REGION', 'NAME')
        AND DATA_TYPE IN ('VARCHAR', 'NVARCHAR') THEN 1
        
        --- Test Integer values' validity
        WHEN COLUMN_NAME IN ('Population', 
        'Clean_Pop', 
        'Labor_Force', 
        'Clean_LF', 
        'Internet_Users', 
        'Clean_IU', 
        'Energy_Consumption_Per_Capita_btu',
        'Installed_Generating_Capicity_kW',
        'CO2_Emissions_mTonnes',
        'Unemployed_Population_Est',
        'Population_Rank',
        'RGDP_Rank',
        'RGDP_Per_Capita_Rank',
        'RGDP_Growth_Rank',
        'Gini_Rank',
        'Inflation_rank',
        'Labor_Force_rank',
        'Internet_Users_rank',
        'Emissions_Rank',
        'GenCap_Rank',
        'Energy_Consumption_pc_Rank')
        AND DATA_TYPE IN ('INT', 'SMALLINT', 'TINYINT') THEN 1
        WHEN COLUMN_NAME IN ('Population', 
        'Clean_Pop', 
        'Labor_Force', 
        'Clean_LF', 
        'Internet_Users', 
        'Clean_IU', 
        'Energy_Consumption_Per_Capita_btu',
        'Installed_Generating_Capicity_kW',
        'CO2_Emissions_mTonnes',
        'Unemployed_Population_Est',
        'Population_Rank',
        'RGDP_Rank',
        'RGDP_Per_Capita_Rank',
        'RGDP_Growth_Rank',
        'Gini_Rank',
        'Inflation_rank',
        'Labor_Force_rank',
        'Internet_Users_rank',
        'Emissions_Rank',
        'GenCap_Rank',
        'Energy_Consumption_pc_Rank')
        AND DATA_TYPE IN ('FLOAT', 'DECIMAL') THEN 0.5 --- still works, but not the most appropriate data type


        --- Test Money values' validity
        WHEN COLUMN_NAME IN ('RGDP', 
        'RGDP_Per_Capita', 
        'Education_Budget_Est', 
        'Military_Budget_Est', 
        'Coal_Revenue_Est', 
        'Agricultural_Product_Est', 
        'Industrial_Product_Est', 
        'Services_Product_Est') 
        AND DATA_TYPE = 'MONEY' THEN 1
        WHEN COLUMN_NAME IN ('RGDP', 
        'RGDP_Per_Capita', 
        'Education_Budget_Est', 
        'Military_Budget_Est', 
        'Coal_Revenue_Est', 
        'Agricultural_Product_Est', 
        'Industrial_Product_Est', 
        'Services_Product_Est') 
        AND DATA_TYPE IN ('FLOAT', 'DECIMAL', 'INT') THEN 0.5 --- still works, but not the most appropriate data type

        -- Test pct & float values' validity
        WHEN COLUMN_NAME IN ('RGDP_Growth_Rate', 
        'Populaiton_Growth_Rate',
        'Gini_Index_Coefficient',
        'Inflation_Rate_YoY_Consumer_Prices',
        'Unemployment_Rate',
        'GDP_Pct_Agricultural',
        'GDP_Pct_Industrial',
        'GDP_Pct_Services',
        'Education_Budget_Pct',
        'Military_budget_pct',
        'Coal_Revenue_Pct_GDP',
        'pct_Population_with_internet_Est')
        AND DATA_TYPE IN('FLOAT', 'DECIMAL') THEN 1

        --- Test year values' validity
        WHEN COLUMN_NAME IN ('RGDP_Year',
        'RGDP_G_Year',
        'PopG_Year',
        'Gini_Year',
        'Inflation_Rate_Year',
        'Labor_Force_Year',
        'Unemployment_Rate_Year',
        'GDPa_pct_Year',
        'GDPi_pct_Year',
        'GDPs_pct_Year',
        'Int_Users_Year',
        'Edu_Budget_Year',
        'Mil_Budget_Year',
        'Coal_Rev_Year',
        'Emissions_Year',
        'GenCap_Year',
        'Consumption_Year')
        AND DATA_TYPE IN ('YEAR') THEN 1
        WHEN COLUMN_NAME IN ('RGDP_Year',
        'RGDP_G_Year',
        'PopG_Year',
        'Gini_Year',
        'Inflation_Rate_Year',
        'Labor_Force_Year',
        'Unemployment_Rate_Year',
        'GDPa_pct_Year',
        'GDPi_pct_Year',
        'GDPs_pct_Year',
        'Int_Users_Year',
        'Edu_Budget_Year',
        'Mil_Budget_Year',
        'Coal_Rev_Year',
        'Emissions_Year',
        'GenCap_Year',
        'Consumption_Year')
        AND DATA_TYPE IN ('INT', 'SMALLINT', 'TINYINT') THEN 0.5

        ELSE 0
    END AS validity_score
INTO dq_validity
FROM 
    INFORMATION_SCHEMA.COLUMNS
WHERE 
    TABLE_NAME = 'derived_data'
ORDER BY 
    ORDINAL_POSITION;


-- Completeness Check for derived_data
SELECT 
    COUNT(Name) AS Countries_and_territories,

    -- Completeness checks for each column as float values
    CAST(COUNT(Region) AS FLOAT) / COUNT(Name) AS Region_completeness,
    CAST(COUNT(Name) AS FLOAT) / COUNT(Name) AS Name_completeness,
    CAST(COUNT(Clean_Pop) AS FLOAT) / COUNT(Name) AS Clean_Pop_completeness,
    CAST(COUNT(Population_Rank) AS FLOAT) / COUNT(Name) AS Population_Rank_completeness,
    CAST(COUNT(RGDP) AS FLOAT) / COUNT(Name) AS RGDP_completeness,
    CAST(COUNT(RGDP_Year) AS FLOAT) / COUNT(Name) AS RGDP_Year_completeness,
    CAST(COUNT(RGDP_Rank) AS FLOAT) / COUNT(Name) AS RGDP_Rank_completeness,
    CAST(COUNT(RGDP_Per_Capita) AS FLOAT) / COUNT(Name) AS RGDP_Per_Capita_completeness,
    CAST(COUNT(RGDP_Per_Capita_Rank) AS FLOAT) / COUNT(Name) AS RGDP_Per_Capita_Rank_completeness,
    CAST(COUNT(RGDP_Growth_Rate) AS FLOAT) / COUNT(Name) AS RGDP_Growth_Rate_completeness,
    CAST(COUNT(RGDP_G_Year) AS FLOAT) / COUNT(Name) AS RGDP_G_Year_completeness,
    CAST(COUNT(RGDP_Growth_Rank) AS FLOAT) / COUNT(Name) AS RGDP_Growth_Rank_completeness,
    CAST(COUNT(Population_Growth_Rate) AS FLOAT) / COUNT(Name) AS Population_Growth_Rate_completeness,
    CAST(COUNT(PopG_Year) AS FLOAT) / COUNT(Name) AS PopG_Year_completeness,
    CAST(COUNT(Gini_Index_Coefficient) AS FLOAT) / COUNT(Name) AS Gini_Index_Coefficient_completeness,
    CAST(COUNT(Gini_Rank) AS FLOAT) / COUNT(Name) AS Gini_Rank_completeness,
    CAST(COUNT(Gini_Year) AS FLOAT) / COUNT(Name) AS Gini_Year_completeness,
    CAST(COUNT(Inflation_Rate_YoY_Consumer_Prices) AS FLOAT) / COUNT(Name) AS Inflation_Rate_YoY_Consumer_Prices_completeness,
    CAST(COUNT(Inflation_Rate_Year) AS FLOAT) / COUNT(Name) AS Inflation_Rate_Year_completeness,
    CAST(COUNT(Inflation_rank) AS FLOAT) / COUNT(Name) AS Inflation_rank_completeness,
    CAST(COUNT(Clean_LF) AS FLOAT) / COUNT(Name) AS Clean_LF_completeness,
    CAST(COUNT(Labor_Force_Year) AS FLOAT) / COUNT(Name) AS Labor_Force_Year_completeness,
    CAST(COUNT(Labor_Force_rank) AS FLOAT) / COUNT(Name) AS Labor_Force_rank_completeness,
    CAST(COUNT(Unemployment_Rate) AS FLOAT) / COUNT(Name) AS Unemployment_Rate_completeness,
    CAST(COUNT(Unemployment_Rate_Year) AS FLOAT) / COUNT(Name) AS Unemployment_Rate_Year_completeness,
    CAST(COUNT(GDP_Pct_Agricultural) AS FLOAT) / COUNT(Name) AS GDP_Pct_Agricultural_completeness,
    CAST(COUNT(GDPa_pct_Year) AS FLOAT) / COUNT(Name) AS GDPa_pct_Year_completeness,
    CAST(COUNT(GDP_Pct_Industrial) AS FLOAT) / COUNT(Name) AS GDP_Pct_Industrial_completeness,
    CAST(COUNT(GDPi_pct_Year) AS FLOAT) / COUNT(Name) AS GDPi_pct_Year_completeness,
    CAST(COUNT(GDP_Pct_Services) AS FLOAT) / COUNT(Name) AS GDP_Pct_Services_completeness,
    CAST(COUNT(GDPs_pct_Year) AS FLOAT) / COUNT(Name) AS GDPs_pct_Year_completeness,
    CAST(COUNT(Clean_IU) AS FLOAT) / COUNT(Name) AS Clean_IU_completeness,
    CAST(COUNT(Int_Users_Year) AS FLOAT) / COUNT(Name) AS Int_Users_Year_completeness,
    CAST(COUNT(Internet_Users_rank) AS FLOAT) / COUNT(Name) AS Internet_Users_rank_completeness,
    CAST(COUNT(Education_Budget_Pct) AS FLOAT) / COUNT(Name) AS Education_Budget_Pct_completeness,
    CAST(COUNT(Edu_Budget_Year) AS FLOAT) / COUNT(Name) AS Edu_Budget_Year_completeness,
    CAST(COUNT(Military_budget_pct) AS FLOAT) / COUNT(Name) AS Military_budget_pct_completeness,
    CAST(COUNT(Mil_Budget_Year) AS FLOAT) / COUNT(Name) AS Mil_Budget_Year_completeness,
    CAST(COUNT(Coal_Revenue_Pct_GDP) AS FLOAT) / COUNT(Name) AS Coal_Revenue_Pct_GDP_completeness,
    CAST(COUNT(Coal_Rev_Year) AS FLOAT) / COUNT(Name) AS Coal_Rev_Year_completeness,
    CAST(COUNT(CO2_Emissions_mTonnes) AS FLOAT) / COUNT(Name) AS CO2_Emissions_mTonnes_completeness,
    CAST(COUNT(Emissions_Year) AS FLOAT) / COUNT(Name) AS Emissions_Year_completeness,
    CAST(COUNT(Emissions_Rank) AS FLOAT) / COUNT(Name) AS Emissions_Rank_completeness,
    CAST(COUNT(Installed_Generating_Capacity_kW) AS FLOAT) / COUNT(Name) AS Installed_Generating_Capacity_kW_completeness,
    CAST(COUNT(GenCap_Year) AS FLOAT) / COUNT(Name) AS GenCap_Year_completeness,
    CAST(COUNT(GenCap_Rank) AS FLOAT) / COUNT(Name) AS GenCap_Rank_completeness,
    CAST(COUNT(Energy_Consumption_Per_Capita_btu) AS FLOAT) / COUNT(Name) AS Energy_Consumption_Per_Capita_btu_completeness,
    CAST(COUNT(Consumption_Year) AS FLOAT) / COUNT(Name) AS Consumption_Year_completeness,
    CAST(COUNT(Energy_Consumption_pc_Rank) AS FLOAT) / COUNT(Name) AS Energy_Consumption_pc_Rank_completeness,
    CAST(COUNT(Education_Budget_Est) AS FLOAT) / COUNT(Name) AS Education_Budget_Est_completeness,
    CAST(COUNT(Military_Budget_Est) AS FLOAT) / COUNT(Name) AS Military_Budget_Est_completeness,
    CAST(COUNT(Coal_Revenue_Est) AS FLOAT) / COUNT(Name) AS Coal_Revenue_Est_completeness,
    CAST(COUNT(pct_Population_with_internet_Est) AS FLOAT) / COUNT(Name) AS pct_Population_with_internet_Est_completeness,
    CAST(COUNT(Unemployed_Population_Est) AS FLOAT) / COUNT(Name) AS Unemployed_Population_Est_completeness,
    CAST(COUNT(Agricultural_Product_Est) AS FLOAT) / COUNT(Name) AS Agricultural_Product_Est_completeness,
    CAST(COUNT(Industrial_Product_Est) AS FLOAT) / COUNT(Name) AS Industrial_Product_Est_completeness,
    CAST(COUNT(Services_Product_Est) AS FLOAT) / COUNT(Name) AS Services_Product_Est_completeness
INTO dq_completeness
FROM derived_data;

--- summary analysis for completeness, average of all rows in dq_completeness
--- complete similar summaries for validity, compare validity to set values, and sum all counts of year regardless of column
--- pie chart each as overall timely, valid, and complete

--- Validity check for derived_data
-- Timeliness Check for RGDP_y_valid
SELECT 
    RGDP_y_valid,
    COUNT(*) AS RGDP_y_valid_Count,
    CAST(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM derived_data) AS DECIMAL(5, 2)) AS RGDPY_Pct_timely
INTO RGDP_timeliness
FROM cleansed_data
WHERE RGDP_y_valid IS NOT NULL
GROUP BY RGDP_y_valid
ORDER BY RGDP_y_valid DESC;

-- Timeliness Check for RGDP_G_y_valid
SELECT 
    RGDP_G_y_valid,
    COUNT(*) AS RGDP_G_y_valid_Count,
    CAST(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM derived_data) AS DECIMAL(5, 2)) AS RGDPG_Pct_timely
INTO RGDP_G_timeliness
FROM cleansed_data
WHERE RGDP_G_y_valid IS NOT NULL
GROUP BY RGDP_G_y_valid
ORDER BY RGDP_G_y_valid DESC;

-- Timeliness Check for PopG_y_valid
SELECT 
    PopG_y_valid,
    COUNT(*) AS PopG_y_valid_Count,
    CAST(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM derived_data) AS DECIMAL(5, 2)) AS PopG_Pct_timely
INTO PopG_timeliness
FROM cleansed_data
WHERE PopG_y_valid IS NOT NULL
GROUP BY PopG_y_valid
ORDER BY PopG_y_valid DESC;

-- Timeliness Check for Inflation_Rate_y_valid
SELECT 
    Inflation_Rate_y_valid,
    COUNT(*) AS Inflation_Rate_y_valid_Count,
    CAST(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM derived_data) AS DECIMAL(5, 2)) AS Inflation_Pct_timely
INTO Inflation_timeliness
FROM cleansed_data
WHERE Inflation_Rate_y_valid IS NOT NULL
GROUP BY Inflation_Rate_y_valid
ORDER BY Inflation_Rate_y_valid DESC;

-- Timeliness Check for Labor_Force_y_valid
SELECT 
    Labor_Force_y_valid,
    COUNT(*) AS Labor_Force_y_valid_Count,
    CAST(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM derived_data) AS DECIMAL(5, 2)) AS LaborForce_Pct_timely
INTO Labor_Force_timeliness
FROM cleansed_data
WHERE Labor_Force_y_valid IS NOT NULL
GROUP BY Labor_Force_y_valid
ORDER BY Labor_Force_y_valid DESC;

-- Timeliness Check for Unemployment_Rate_y_valid
SELECT 
    Unemployment_Rate_y_valid,
    COUNT(*) AS Unemployment_Rate_y_valid_Count,
    CAST(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM derived_data) AS DECIMAL(5, 2)) AS Unemployment_Pct_timely
INTO Unemployment_timeliness
FROM cleansed_data
WHERE Unemployment_Rate_y_valid IS NOT NULL
GROUP BY Unemployment_Rate_y_valid
ORDER BY Unemployment_Rate_y_valid DESC;

-- Timeliness Check for GDPa_pct_y_valid
SELECT 
    GDPa_pct_y_valid,
    COUNT(*) AS GDPa_pct_y_valid_Count,
    CAST(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM derived_data) AS DECIMAL(5, 2)) AS GDPa_Pct_timely
INTO GDPa_pct_timeliness
FROM cleansed_data
WHERE GDPa_pct_y_valid IS NOT NULL
GROUP BY GDPa_pct_y_valid
ORDER BY GDPa_pct_y_valid DESC;

-- Timeliness Check for GDPi_pct_y_valid
SELECT 
    GDPi_pct_y_valid,
    COUNT(*) AS GDPi_pct_y_valid_Count,
    CAST(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM derived_data) AS DECIMAL(5, 2)) AS GDPi_Pct_timely
INTO GDPi_pct_timeliness
FROM cleansed_data
WHERE GDPi_pct_y_valid IS NOT NULL
GROUP BY GDPi_pct_y_valid
ORDER BY GDPi_pct_y_valid DESC;

-- Timeliness Check for GDPs_pct_y_valid
SELECT 
    GDPs_pct_y_valid,
    COUNT(*) AS GDPs_pct_y_valid_Count,
    CAST(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM derived_data) AS DECIMAL(5, 2)) AS GDPs_Pct_timely
INTO GDPs_pct_timeliness
FROM cleansed_data
WHERE GDPs_pct_y_valid IS NOT NULL
GROUP BY GDPs_pct_y_valid
ORDER BY GDPs_pct_y_valid DESC;

-- Timeliness Check for Int_Users_y_valid
SELECT 
    Int_Users_y_valid,
    COUNT(*) AS Int_Users_y_valid_Count,
    CAST(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM derived_data) AS DECIMAL(5, 2)) AS Int_Users_Pct_timely
INTO Int_Users_timeliness
FROM cleansed_data
WHERE Int_Users_y_valid IS NOT NULL
GROUP BY Int_Users_y_valid
ORDER BY Int_Users_y_valid DESC;

-- Timeliness Check for Edu_Budget_y_valid
SELECT 
    Edu_Budget_y_valid,
    COUNT(*) AS Edu_Budget_y_valid_Count,
    CAST(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM derived_data) AS DECIMAL(5, 2)) AS Edu_Budget_Pct_timely
INTO Edu_Budget_timeliness
FROM cleansed_data
WHERE Edu_Budget_y_valid IS NOT NULL
GROUP BY Edu_Budget_y_valid
ORDER BY Edu_Budget_y_valid DESC;

-- Timeliness Check for Mil_Budget_y_valid
SELECT 
    Mil_Budget_y_valid,
    COUNT(*) AS Mil_Budget_y_valid_Count,
    CAST(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM derived_data) AS DECIMAL(5, 2)) AS Mil_Budget_Pct_timely
INTO Mil_Budget_timeliness
FROM cleansed_data
WHERE Mil_Budget_y_valid IS NOT NULL
GROUP BY Mil_Budget_y_valid
ORDER BY Mil_Budget_y_valid DESC;

-- Timeliness Check for Coal_Rev_y_valid
SELECT 
    Coal_Rev_y_valid,
    COUNT(*) AS Coal_Rev_y_valid_Count,
    CAST(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM derived_data) AS DECIMAL(5, 2)) AS Coal_Rev_Pct_timely
INTO Coal_Rev_timeliness
FROM cleansed_data
WHERE Coal_Rev_y_valid IS NOT NULL
GROUP BY Coal_Rev_y_valid
ORDER BY Coal_Rev_y_valid DESC;

-- Timeliness Check for Emissions_y_valid
SELECT 
    Emissions_y_valid,
    COUNT(*) AS Emissions_y_valid_Count,
    CAST(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM derived_data) AS DECIMAL(5, 2)) AS Emissions_Pct_timely
INTO Emissions_timeliness
FROM cleansed_data
WHERE Emissions_y_valid IS NOT NULL
GROUP BY Emissions_y_valid
ORDER BY Emissions_y_valid DESC;

-- Timeliness Check for enCap_y_valid
SELECT 
    GenCap_y_valid,
    COUNT(*) AS GenCap_y_valid_Count,
    CAST(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM derived_data) AS DECIMAL(5, 2)) AS GenCap_Pct_timely
INTO GenCap_timeliness
FROM cleansed_data
WHERE GenCap_y_valid IS NOT NULL
GROUP BY GenCap_y_valid
ORDER BY GenCap_y_valid DESC;

-- Timeliness Check for Consumption_y_valid
SELECT 
    Consumption_y_valid,
    COUNT(*) AS Consumption_y_valid_Count,
    CAST(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM derived_data) AS DECIMAL(5, 2)) AS Consumption_Pct_timely
INTO Consumption_timeliness
FROM cleansed_data
WHERE Consumption_y_valid IS NOT NULL
GROUP BY Consumption_y_valid
ORDER BY Consumption_y_valid DESC;

-- Final join query
SELECT *
INTO DQ_timeliness_raw
FROM RGDP_timeliness
    LEFT JOIN RGDP_G_timeliness ON RGDP_timeliness.RGDP_y_valid = RGDP_G_timeliness.RGDP_G_y_valid
    LEFT JOIN PopG_timeliness ON RGDP_timeliness.RGDP_y_valid = PopG_timeliness.PopG_y_valid
    LEFT JOIN Inflation_timeliness ON RGDP_timeliness.RGDP_y_valid = Inflation_timeliness.Inflation_Rate_y_valid
    LEFT JOIN Labor_Force_timeliness ON RGDP_timeliness.RGDP_y_valid = Labor_Force_timeliness.Labor_Force_y_valid
    LEFT JOIN Unemployment_timeliness ON RGDP_timeliness.RGDP_y_valid = Unemployment_timeliness.Unemployment_Rate_y_valid
    LEFT JOIN GDPa_pct_timeliness ON RGDP_timeliness.RGDP_y_valid = GDPa_pct_timeliness.GDPa_pct_y_valid
    LEFT JOIN GDPi_pct_timeliness ON RGDP_timeliness.RGDP_y_valid = GDPi_pct_timeliness.GDPi_pct_y_valid
    LEFT JOIN GDPs_pct_timeliness ON RGDP_timeliness.RGDP_y_valid = GDPs_pct_timeliness.GDPs_pct_y_valid
    LEFT JOIN Int_Users_timeliness ON RGDP_timeliness.RGDP_y_valid = Int_Users_timeliness.Int_Users_y_valid
    LEFT JOIN Edu_Budget_timeliness ON RGDP_timeliness.RGDP_y_valid = Edu_Budget_timeliness.Edu_Budget_y_valid
    LEFT JOIN Mil_Budget_timeliness ON RGDP_timeliness.RGDP_y_valid = Mil_Budget_timeliness.Mil_Budget_y_valid
    LEFT JOIN Coal_Rev_timeliness ON RGDP_timeliness.RGDP_y_valid = Coal_Rev_timeliness.Coal_Rev_y_valid
    LEFT JOIN Emissions_timeliness on RGDP_timeliness.RGDP_y_valid = Emissions_timeliness.Emissions_y_valid
    LEFT JOIN GenCap_timeliness on RGDP_timeliness.RGDP_y_valid = GenCap_timeliness.GenCap_y_valid
    LEFT JOIN Consumption_timeliness on RGDP_timeliness.RGDP_y_valid = Consumption_timeliness.Consumption_y_valid
ORDER BY RGDP_timeliness.RGDP_y_valid DESC;

--- Consistency Check

SELECT 
    m.Name,
    (CASE WHEN m.Region IS NOT NULL AND d.Region IS NOT NULL THEN 1 ELSE 0 END +
    CASE WHEN m.Name IS NOT NULL AND d.Name IS NOT NULL THEN 1 ELSE 0 END +
    CASE WHEN m.Population IS NOT NULL AND d.Clean_Pop IS NOT NULL THEN 1 ELSE 0 END +
    CASE WHEN m.Population_Rank IS NOT NULL AND d.Population_Rank IS NOT NULL THEN 1 ELSE 0 END +
    CASE WHEN m.RGDP IS NOT NULL AND d.RGDP IS NOT NULL THEN 1 ELSE 0 END +
    CASE WHEN m.RGDP_Year IS NOT NULL AND d.RGDP_Year IS NOT NULL THEN 1 ELSE 0 END +
    CASE WHEN m.RGDP_Rank IS NOT NULL AND d.RGDP_Rank IS NOT NULL THEN 1 ELSE 0 END +
    CASE WHEN m.RGDP_Per_Capita IS NOT NULL AND d.RGDP_Per_Capita IS NOT NULL THEN 1 ELSE 0 END +
    CASE WHEN m.RGDP_Per_Capita_Rank IS NOT NULL AND d.RGDP_Per_Capita_Rank IS NOT NULL THEN 1 ELSE 0 END +
    CASE WHEN m.RGDP_Growth_Rate IS NOT NULL AND d.RGDP_Growth_Rate IS NOT NULL THEN 1 ELSE 0 END +
    CASE WHEN m.RGDP_G_Year IS NOT NULL AND d.RGDP_G_Year IS NOT NULL THEN 1 ELSE 0 END +
    CASE WHEN m.RGDP_Growth_Rank IS NOT NULL AND d.RGDP_Growth_Rank IS NOT NULL THEN 1 ELSE 0 END +
    CASE WHEN m.Population_Growth_Rate IS NOT NULL AND d.Population_Growth_Rate IS NOT NULL THEN 1 ELSE 0 END +
    CASE WHEN m.PopG_Year IS NOT NULL AND d.PopG_Year IS NOT NULL THEN 1 ELSE 0 END +
    CASE WHEN m.Gini_Index_Coefficient IS NOT NULL AND d.Gini_Index_Coefficient IS NOT NULL THEN 1 ELSE 0 END +
    CASE WHEN m.Gini_Rank IS NOT NULL AND d.Gini_Rank IS NOT NULL THEN 1 ELSE 0 END +
    CASE WHEN m.Gini_Year IS NOT NULL AND d.Gini_Year IS NOT NULL THEN 1 ELSE 0 END +
    CASE WHEN m.Inflation_Rate_YoY_Consumer_Prices IS NOT NULL AND d.Inflation_Rate_YoY_Consumer_Prices IS NOT NULL THEN 1 ELSE 0 END +
    CASE WHEN m.Inflation_Rate_Year IS NOT NULL AND d.Inflation_Rate_Year IS NOT NULL THEN 1 ELSE 0 END +
    CASE WHEN m.Inflation_rank IS NOT NULL AND d.Inflation_rank IS NOT NULL THEN 1 ELSE 0 END +
    CASE WHEN m.Labor_Force IS NOT NULL AND d.Clean_LF IS NOT NULL THEN 1 ELSE 0 END +
    CASE WHEN m.Labor_Force_Year IS NOT NULL AND d.Labor_Force_Year IS NOT NULL THEN 1 ELSE 0 END +
    CASE WHEN m.Labor_Force_rank IS NOT NULL AND d.Labor_Force_rank IS NOT NULL THEN 1 ELSE 0 END +
    CASE WHEN m.Unemployment_Rate IS NOT NULL AND d.Unemployment_Rate IS NOT NULL THEN 1 ELSE 0 END +
    CASE WHEN m.Unemployment_Rate_Year IS NOT NULL AND d.Unemployment_Rate_Year IS NOT NULL THEN 1 ELSE 0 END +
    CASE WHEN m.GDP_Pct_Agricultural IS NOT NULL AND d.GDP_Pct_Agricultural IS NOT NULL THEN 1 ELSE 0 END +
    CASE WHEN m.GDPa_pct_Year IS NOT NULL AND d.GDPa_pct_Year IS NOT NULL THEN 1 ELSE 0 END +
    CASE WHEN m.GDP_Pct_Industrial IS NOT NULL AND d.GDP_Pct_Industrial IS NOT NULL THEN 1 ELSE 0 END +
    CASE WHEN m.GDPi_pct_Year IS NOT NULL AND d.GDPi_pct_Year IS NOT NULL THEN 1 ELSE 0 END +
    CASE WHEN m.GDP_Pct_Services IS NOT NULL AND d.GDP_Pct_Services IS NOT NULL THEN 1 ELSE 0 END +
    CASE WHEN m.GDPs_pct_Year IS NOT NULL AND d.GDPs_pct_Year IS NOT NULL THEN 1 ELSE 0 END +
    CASE WHEN m.Internet_Users IS NOT NULL AND d.Clean_IU IS NOT NULL THEN 1 ELSE 0 END +
    CASE WHEN m.Int_Users_Year IS NOT NULL AND d.Int_Users_Year IS NOT NULL THEN 1 ELSE 0 END +
    CASE WHEN m.Internet_Users_rank IS NOT NULL AND d.Internet_Users_rank IS NOT NULL THEN 1 ELSE 0 END +
    CASE WHEN m.Education_Budget_Pct IS NOT NULL AND d.Education_Budget_Pct IS NOT NULL THEN 1 ELSE 0 END +
    CASE WHEN m.Edu_Budget_Year IS NOT NULL AND d.Edu_Budget_Year IS NOT NULL THEN 1 ELSE 0 END +
    CASE WHEN m.Military_budget_pct IS NOT NULL AND d.Military_budget_pct IS NOT NULL THEN 1 ELSE 0 END +
    CASE WHEN m.Mil_Budget_Year IS NOT NULL AND d.Mil_Budget_Year IS NOT NULL THEN 1 ELSE 0 END +
    CASE WHEN m.Coal_Revenue_Pct_GDP IS NOT NULL AND d.Coal_Revenue_Pct_GDP IS NOT NULL THEN 1 ELSE 0 END +
    CASE WHEN m.Coal_Rev_Year IS NOT NULL AND d.Coal_Rev_Year IS NOT NULL THEN 1 ELSE 0 END +
    CASE WHEN m.CO2_Emissions_mTonnes IS NOT NULL AND d.CO2_Emissions_mTonnes IS NOT NULL THEN 1 ELSE 0 END +
    CASE WHEN m.Emissions_Year IS NOT NULL AND d.Emissions_Year IS NOT NULL THEN 1 ELSE 0 END +
    CASE WHEN m.Emissions_Rank IS NOT NULL AND d.Emissions_Rank IS NOT NULL THEN 1 ELSE 0 END +
    CASE WHEN m.Installed_Generating_Capacity_kW IS NOT NULL AND d.Installed_Generating_Capacity_kW IS NOT NULL THEN 1 ELSE 0 END +
    CASE WHEN m.GenCap_Year IS NOT NULL AND d.GenCap_Year IS NOT NULL THEN 1 ELSE 0 END +
    CASE WHEN m.GenCap_Rank IS NOT NULL AND d.GenCap_Rank IS NOT NULL THEN 1 ELSE 0 END +
    CASE WHEN m.Energy_Consumption_Per_Capita_btu IS NOT NULL AND d.Energy_Consumption_Per_Capita_btu IS NOT NULL THEN 1 ELSE 0 END +
    CASE WHEN m.Consumption_Year IS NOT NULL AND d.Consumption_Year IS NOT NULL THEN 1 ELSE 0 END +
    CASE WHEN m.Energy_Consumption_pc_Rank IS NOT NULL AND d.Energy_Consumption_pc_Rank IS NOT NULL THEN 1 ELSE 0 END
    ) AS Congruence_Score
INTO dq_consistency
FROM 
    master_reference AS m
JOIN 
    derived_data AS d ON m.Name = d.Name
WHERE 
    m.Name IS NOT NULL;


select * from dq_completeness;
select * from dq_validity;
select * from DQ_timeliness_raw;
select * from dq_consistency;