# Event-Driven Customer Data Platform on Snowflake

# 🚀 Project Overview

This project demonstrates the **design and implementation of an end-to-end customer data platform built entirely on Snowflake using event-driven architecture principles**.

The objective was to simulate a real-world, production-grade data engineering environment where:

- Data is ingested automatically from cloud storage  
- Change Data Capture (CDC) is handled incrementally  
- Transformations are fully automated  
- Governance is enforced through role-based security  
- Observability and logging are implemented  
- A user-facing dashboard is built inside Snowflake  
- Batch machine learning scoring is integrated  

This project reflects how modern cloud-native data platforms are architected and operated in enterprise environments.

# Architecture Summary

## High-Level Flow

Customer CSV files are uploaded to Azure Blob Storage.

Azure Event Grid captures the file upload event.

Event Grid sends a notification to Azure Queue.

Snowpipe listens to the queue and automatically ingests data.

Data lands in a RAW table.

A Stream tracks newly inserted rows.

A Task runs a Stored Procedure for incremental processing.

Data is merged into a CLEAN table.

A logging table tracks execution status.

A Streamlit dashboard visualizes platform health and customer data.

An ML model is trained and deployed using a Python UDF.

Predictions are stored in a scoring table.

This entire flow runs automatically without manual intervention.

# Tech Stack

## Cloud & Storage

- Azure Blob Storage  
- Azure Event Grid  
- Azure Storage Queue  

## Snowflake Components

- External Stage  
- Storage Integration  
- Notification Integration  
- Snowpipe (AUTO_INGEST)  
- Streams  
- Tasks  
- Stored Procedures (SQL + JavaScript)  
- Masking Policies  
- Row Access Policies  
- Snowpark  
- Python UDF  
- Streamlit in Snowflake  

## ML Stack

- Python  
- Pandas  
- Scikit-Learn (Logistic Regression)

# Detailed Implementation

---
## 1. Automated Ingestion (Event-Driven Architecture)

### Objective

Load customer files into Snowflake automatically when uploaded to cloud storage.

### Implementation Steps

- Created a Storage Integration to securely connect Snowflake to Azure Blob.  
- Created an External Stage pointing to the Blob container.  
- Created a Notification Integration linked to Azure Queue.  
- Configured Snowpipe with `AUTO_INGEST = TRUE`.  
- Configured Azure Event Grid to push blob creation events to the queue.  

### Result

When a new CSV file is uploaded:

- Snowpipe automatically loads the file into the RAW table.  
- No manual `COPY` commands are required.  
- Fully event-driven ingestion.

---
## 2. Raw Layer

**Table:** `RAW_SCHEMA.CUSTOMER_RAW`

### Purpose

- Store ingested data without transformation.  
- Preserve original structure.  
- Act as a landing zone.  

This layer ensures separation between ingestion and transformation logic.

---

## 3. Change Data Capture (CDC)

### Objective

Process only newly ingested data instead of reprocessing the entire dataset.

### Implementation

- Created a Stream on the RAW table:
  - Tracks inserted rows.  
  - Maintains offset automatically.  

- Created a Stored Procedure:
  - Performs `MERGE` into the CLEAN table.  
  - Handles updates and inserts.  
  - Logs execution details.  

- Created a Task:
  - Runs every minute.  
  - Condition: `SYSTEM$STREAM_HAS_DATA`  
  - Executes the stored procedure only when new data exists.  

### Result

- Incremental processing  
- No duplicate processing  
- Efficient compute usage  
- Near real-time transformation  

---

## 4. Clean Layer

**Table:** `PROCESSED_SCHEMA.CUSTOMER_CLEAN`

### Purpose

- Store cleaned and structured customer data.  
- Apply transformations:
  - Trim fields  
  - Cast data types  
  - Normalize columns  

Acts as the analytics-ready layer.

## 5. Observability & Logging

**Created table:** `LOG_SCHEMA.PIPELINE_LOG`

### Tracks

- Pipeline start time  
- End time  
- Rows processed  
- Status (`SUCCESS` / `FAILED`)  
- Error messages  

The stored procedure writes logs after every execution.

This simulates production-grade monitoring.

---

## 6. Data Governance

### Column-Level Security (Masking Policy)

Applied a masking policy to the `EMAIL` column.

**Behavior:**

- Developer role → sees actual email  
- Public role → sees masked value  

Demonstrates secure data sharing.

---

### Row-Level Security (Row Access Policy)

Restricted `REGION` visibility by role.

**Result:**

- Developer role sees full dataset  
- Public role sees limited subset  

Shows enterprise RBAC implementation.

---

## 7. Streamlit Dashboard

Built an interactive dashboard inside Snowflake.

### Features

- Current role display  
- Customer count metrics  
- Region distribution chart  
- Stream backlog indicator  
- Task execution history  
- Warehouse credit usage  
- Manual pipeline trigger  
- Data filtering by region  
- Email search  
- CSV download  
- ML prediction display  

The dashboard respects:

- Masking policies  
- Row access policies  
- Role-based permissions  

---

## 8. Machine Learning Integration

### Objective

Integrate batch ML scoring inside Snowflake.

### Feature Engineering

**Created table:** `CUSTOMER_ML_FEATURES`

**Features:**

- `AGE`  
- `GENDER`  
- `REGION`  

**Target:**

- `STATUS` (binary classification based on age threshold)  
