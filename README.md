# PROJECT: END-TO-END DATA PIPELINE ON MICROSOFT FABRIC
This is the final project for MindX Data Engineering Level 3, implementing a scalable ETL pipeline using the Medallion Architecture (Bronze, Silver, Gold).

# Overview

The pipeline automates the ingestion, processing, and analysis of retail sales data for MINDX-Mart. The process includes:

Bronze Layer: Extract raw sales data and exchange rates from Google Drive and load them as Parquet files into OneLake.

Silver Layer: Cleanse data anomalies (filter failed transactions), parse nested JSON structures, and normalize the data into Delta Tables.

Gold Layer: Model data into a Star Schema, calculate monthly VND revenue, and analyze discount usage impact by geographic location.

# Project Structure

mindx_data_pipeline/
├── nb_silver_gold_processing.py      # PySpark source code for Silver & Gold transformations
├── pl_ingest_bronze.png              # Screenshot of the Data Factory ingestion pipeline
└── ERD_Gold_Layer.png                # Entity-Relationship Diagram modeling the Gold layer


# Tech Stack

Platform: Microsoft Fabric (Data Factory, Synapse Notebooks)

Programming Languages: PySpark, Spark SQL

Storage: OneLake, Delta Lake (Delta Tables), Parquet

Architecture: Medallion Data Architecture

# How to run

Set up the Environment
Create a new Lakehouse named mindx_retail_lakehouse in your Microsoft Fabric workspace.

Ingest Data (Bronze Layer)
Recreate the Data Factory Pipeline based on the structure shown in 'pl_ingest_bronze.png' to pull raw files into the Files/Bronze/directory.

Process Data (Silver & Gold Layers)
Import nb_silver_gold_processing.py into your workspace.
Attach the Lakehouse to the notebook and click "Run All" to build the finalized Delta Tables in the Tables/ directory.
