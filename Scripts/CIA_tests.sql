-- Create the Unit_Test_Results logging table if it doesn't exist
IF NOT EXISTS (
    SELECT * FROM INFORMATION_SCHEMA.TABLES 
    WHERE TABLE_NAME = 'Unit_Test_Results'
)
BEGIN
    CREATE TABLE Unit_Test_Results (
        Test_ID INT IDENTITY(1,1) PRIMARY KEY,
        Test_Name NVARCHAR(100) NOT NULL,
        Result NVARCHAR(10) CHECK (Result IN ('Pass', 'Fail')) NOT NULL,
        Failure_Reason NVARCHAR(MAX) NULL,
        Timestamp DATETIME DEFAULT GETDATE()
    );
END;

--- === Unit Test 1: Data Join Validation in master_reference === ---
DECLARE @test_passed BIT = 1;
DECLARE @expectedRegion NVARCHAR(50) = 'Sample Region';
DECLARE @expectedPopulation INT = 500000;
DECLARE @expectedRGDP DECIMAL(18, 2) = 300000000;
DECLARE @failure_reason NVARCHAR(MAX) = NULL;

-- Check if the expected data exists in master_reference
IF NOT EXISTS (
    SELECT 1
    FROM master_reference
    WHERE Region = @expectedRegion
      AND Population = @expectedPopulation
      AND RGDP = @expectedRGDP
)
BEGIN
    SET @test_passed = 0;
    SET @failure_reason = 'Expected data not found in master_reference.';
END

-- Insert the test result into Unit_Test_Results
INSERT INTO Unit_Test_Results (Test_Name, Result, Failure_Reason, Timestamp)
VALUES (
    'Data_Join_Validation_in_master_reference',
    CASE WHEN @test_passed = 1 THEN 'Pass' ELSE 'Fail' END,
    @failure_reason,
    GETDATE()
);

--- === Unit Test 2: Derived Data Validations in derived_data === ---
DECLARE @test_passed_2 BIT = 1;
DECLARE @failure_reason_2 NVARCHAR(MAX) = NULL;

-- Check for non-matching dates

-- Check for negative percentages
IF EXISTS (
    SELECT 1
    FROM derived_data
    WHERE Education_Budget_Pct < 0 OR Military_budget_pct < 0
)
BEGIN
    SET @test_passed_2 = 0;
    SET @failure_reason_2 = COALESCE(@failure_reason_2 + '; ', '') + 'Negative percentage in Education_Budget_Pct or Military_budget_pct.';
END

-- Insert the test result into Unit_Test_Results
INSERT INTO Unit_Test_Results (Test_Name, Result, Failure_Reason, Timestamp)
VALUES (
    'Derived_Data_Validations_in_derived_data',
    CASE WHEN @test_passed_2 = 1 THEN 'Pass' ELSE 'Fail' END,
    @failure_reason_2,
    GETDATE()
);

-- View all test results
SELECT * FROM Unit_Test_Results ORDER BY Timestamp DESC;
