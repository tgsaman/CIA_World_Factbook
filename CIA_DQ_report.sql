---=== Data Quality Metrics ===---
drop table if exists data_quality;

drop table if exists dq_completeness;

DROP TABLE IF EXISTS dq_validity;

drop table if exists dq_timeliness;
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
    CHARACTER_MAXIMUM_LENGTH AS MaxLength
INTO dq_validity
FROM 
    INFORMATION_SCHEMA.COLUMNS
WHERE 
    TABLE_NAME = 'derived_data'
ORDER BY 
    ORDINAL_POSITION;

select * from dq_validity;

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
INTO DQ_timeliness
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

select * from DQ_timeliness