# Scaling an E-Commerce Analytics Platform in Databricks: An End-to-End Data + AI Project

## Project Overview - Link to Medium Article and Story of the Project: https://medium.com/@alijrizvi/scaling-an-e-commerce-analytics-platform-an-end-to-end-data-and-ai-project-875afe5d18db

This project demonstrates how a rapidly growing e-commerce company can migrate from fragile, single-machine Python ETL pipelines to a scalable, cloud-native analytics platform using Databricks and Apache Spark.

As data volume and velocity increased, existing workflows became slow, unreliable, and difficult to maintain. This project was designed as a pilot implementation to validate whether Databricks could support scalable ingestion, transformation, analytics, BI, and statistical modeling on a unified platform.

## Business Problem

* Python-based ETL pipelines failed to scale
* Dashboards lagged or froze
* Business teams could not access fresh data
* Trust in analytics eroded

The goal was not a quick fix, but a future-proof analytics foundation.

## Objectives:
* Improve performance and scalability using distributed computing
* Simplify analytics workflows for engineers, analysts, and business users
* Enable BI, experimentation, and statistical analysis on the same platform
* Automate daily incremental updates

## Architecture:

### This project follows a Medallion Architecture:

* Bronze Layer:
-- Raw CSV ingestion
-- Explicit schemas
-- Audit metadata (source path, ingestion timestamp)

* Silver Layer:
-- Cleaned and standardized data
-- Schema evolution using Delta Lake
-- Business-ready transformations

* Gold Layer:
-- Aggregated, analytics-optimized tables
-- Designed for BI dashboards and decision support

All layers are implemented as Delta Tables.

## Technologies Used:
-- Databricks (Managed Spark platform)
-- Apache Spark / PySpark
-- Delta Lake
-- Databricks Jobs & Workflows
-- Databricks Genie (Generative AI for BI & SQL)
-- Python (EDA, Feature Engineering, ANOVA)
-- SQL
-- Excel (Pivot Tables, Advanced Functions, Lookup Analysis)
-- Generative AI Integration

## The project leverages Databricks Genie, a GenAI-powered analytics assistant, to:
* Generate complex SQL queries via natural language
* Build interactive BI dashboards from Gold-layer tables
* Accelerate insight discovery while maintaining analytical control
* This demonstrates how GenAI can augment analytics, not replace analytical thinking.

## Statistical & Analytical Work

### Beyond data engineering and BI, the project includes:
-- AI-Augmented Exploratory Data Analysis (EDA)
-- Feature engineering (time-based features)
-- ANOVA testing to evaluate seasonal differences in customer spending

Overall, the approach and work bridge data engineering and decision/data science.

## Orchestration:
-- Automated daily incremental pipelines
-- Parameterized Databricks Jobs
-- End-of-day scheduling to ensure fresh data for dashboards

## Key Outcomes:
-- Scalable distributed processing
-- Reliable daily analytics refresh
-- Unified platform for engineering, analytics, BI, and statistics
-- Improved trust in data and insights

## Highlights:

<img width="789" height="497" alt="total_customerexpend_quarter" src="https://github.com/user-attachments/assets/bf695431-3589-437a-8fba-736dee9e14ea" />
