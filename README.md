# ELT Pipeline for NE Cars

An ELT pipeline using dbt, Snowflake and Apache Airflow, for a local used car dealership.

## 🌐 Overview

NE Cars (name changed) is a car dealership based in the North of England. They buy nearly new used cars at auction and recondition them for sale at a profit, as well as paying for repairs on warranties down the line. Their accountants want to perform data analysis on these reconditioning and repair costs, gaining insight into changes in expenditure and profitability over time, correlations between cost and supplier or between cost and car make/model, and so on. However, the company's present systems only make certain fairly rigid queries available. Variability of data input style on the part of the sales personnel represents another limitation on present querying power.

The goal is to build an ELT pipeline according to good data warehousing principles, in order both to answer the accountants' questions now (the MVP) and to automate the loading of new data into the system in future, with an API and dashboard facilitating future queries by data analysts. Data transformation will be handled by **dbt (data build tool)**; **Snowflake** will provide the data platform; and orchestration will be managed using **Apache Airflow**.

## 🗃️ Data Source

The results of two queries in the existing system, one focused on vehicle purchases and the other on reconditioning costs, are output as CSVs with the following schema. These are loaded into Snowflake.

<p align="center"><img src="./docs/images/NECars_CSV_output.svg" alt="Schema of CSVs output from the existing NE Cars system" title="Input Schema" /></p>

These CSVs contain tens of thousands of lines. At the discovery stage, I analysed this data using **Jupyter Notebook and pandas** (notebook not included in this repository for confidentiality reasons) and various **SQL queries in Snowflake** (viewable [here](./discovery/costs_discovery_queries.sql), identifying the following pain points:

1. **Imperfect correspondence of datasets**. The CSVs are filtered by time period. Costs can be incurred some time after purchase, so there are vehicles referenced in the costs data set for which we lack purchase data (and therefore lack information about make, model, original supplier, etc.).
2. **Imperfect correlation of nominal code to cost description**. Most cost codes stably correspond to a given cost type, without exceptions, but one code, `5302`, is used in practice as a 'Miscellaneous' category. In a number of cases costs that should belong to other cost codes have been associated with it.
3. **Free-text / incomplete vehicle descriptions**. Vehicle descriptions are output in the rough format, `[vehicle model] [no. of doors] [transmission type] [fuel type]`. Sometimes, transmission type or fuel type may be missing, or the number of doors may be given as 0.
   
