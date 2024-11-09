# CIA_World_Factbook

This repository contains open-source code for relating & transforming flat files obtained from the CIA World Factbook website and prepping them for data visualization in Tableau. 

Specifically, I work with three SQL scripts to join CSV files on a consistent slug, and then transform them into a comprehensive
SQL database. This database is intended to cleanse, analyze, and transform otherwise unweildy data for efficient use in Tableau.
The public Tableau dashboard is available at this link:

This is an example of a multi-stage program that can ingest, test, and export raw CSV data into 
a relational, clean format for use in visualization; ie. a data pipeline.

As a stretch goal, I intend to build a bash script wrapper that allows users to automatically run these programs on their own SQL server. However, given the relatively low-quality data (see DQ report) I figured this wasn't the best use of my time right now.

SETUP & ETL:

SQL Queries were run on a contained server using Docker. (I'm running MSSQL on a Mac.)

You'll need to download and stage the appropriate files from the CIA World Factbook website in order to run this script. 
As it stands, this repository is currently only useful as a review of the work I completed to make the visualization. 