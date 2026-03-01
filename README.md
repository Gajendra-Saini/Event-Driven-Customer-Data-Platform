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
