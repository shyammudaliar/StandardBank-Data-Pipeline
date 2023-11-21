# StandardBank-Data-Pipeline
Standard Bank Data Engineering Pipeline
 Data Engineering Pipeline for Customer Behavior Modeling

## Overview

This project involves building a data engineering pipeline for a bank to create customer behavior modeling profiles. The bank provides access to various financial data types, such as credit card transactions, savings account transactions, loan payment history, fixed deposit transactions, and mutual fund transactions. Machine learning models, provided by a data science team in pickle format, generate scores based on customer financial behavior. These scores are used by the bank for cross-selling products.

## Pipeline Steps (Simplified)

1. **Environment Setup:**
   - Set up necessary environments, AWS resources, and required libraries.

2. **MySQL Setup:**
   - Configure MySQL database for handling high-frequency financial data.

3. **Sample Data Generation:**
   - Write scripts to create sample data tables in the MySQL RDS.

4. **Lambda Setup (High-Frequency Data):**
   - Create AWS Lambda functions to ingest high-frequency data from RDS.
   - Add logic to accept data only after the last processed entry.
   - Integrate data into the machine learning model pickle files.

5. **Glue Setup (Low-Frequency Data):**
   - Configure AWS Glue jobs to ingest low-frequency data from S3 (CSV files).
   - Implement data processing logic and integrate with machine learning models.

6. **Machine Learning Models:**
   - Utilize pickle files containing pre-trained machine learning models for scoring.
   - Update and store scores for each customer.

