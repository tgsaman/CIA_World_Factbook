# CIA_World_Factbook

DEVELOPER NOTES: 

Below is a SQL script that transforms CSV files from the CIA World Factbook website into a comprehensive
database. This SQL database was intended to transform a large set of data for efficient use in Tableau.
The public Tableau dashboard is available at this link:


This is an example of a multi-stage program that can ingest, test, and export raw CSV data into 
a relational, clean format for use in visualization - a data pipeline.

SETUP & ETL:

SQL Queries were run on a contained server using Docker. (I'm running MS SQL on a Mac.)

To automate the process of storing & querying flat files provided by the CIA, I have provided a bash script 
wrapper capable of staging CSV files, manually saved in a folder, and prepping tables in a SQLedge container env.
This wrapper can be modified to run this script on your own server with minimal changes to queries. 