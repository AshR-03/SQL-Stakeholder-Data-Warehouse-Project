# SQL-Stakeholder-ETL-Data-Warehouse-Project

In this database project, I create an industry standard **`ETL (extract, transform and load)`** [data warehouse](https://en.wikipedia.org/wiki/Data_warehouse) in [SQL server](https://learn.microsoft.com/en-us/sql/sql-server/what-is-sql-server?view=sql-server-ver16). The data warehouse is built upon 3 layers. The **`bronze layer`** (initial layer) contains the raw CSV file data. The **`silver layer`** contains the data from the bronze layer applied to multiple data transformations. Some examples include: 

- **data normalisation**
- **data enrichment**
- **data standardisation**. 

The final layer represents the most appropriate for **stakeholder view** and **data analysis** which is the **`gold layer`**. This layer includes the "`Data Marts`", which are categories of SQL views modelled in a [**Star Schema**](https://www.geeksforgeeks.org/star-schema-in-data-warehouse-modeling/). The datasets and project specifications have been provided by [Baraa Khatib Salkini](https://www.datawithbaraa.com/), and are used with many thanks.

## Project Architecture

The Figure below illustrates the **high level architecture** of the project in SQL Server.

![HighLevelArchitecture3 (1)](https://github.com/user-attachments/assets/6c9bb542-3115-4547-b90d-e35c8926b8dc)

The raw data section represents the [source datasets](Datasets) used in the projects, which are fully accessible and are of CSV format. The data warehouse section outlines how the 3 layers will be **processed** and how data will be **stored and transformed** in each. The **analytics** section represents different techniques that can be applied to the transformed and modelled gold layer including:

- Data `report writing`
- Machine learning models to `predict trends`
- Data `analysis queries`

## Data Modelling

### Data Flow Diagram

When building a **data warehouse**, it is useful to keep track of the `data flow` and `table relations` using a **`data flow diagram`**. The figure below shows how the data flows throughout the layers in the architecture.

![DataFlowDiagram4](https://github.com/user-attachments/assets/e403e800-1bab-4b01-a53c-900b9792c2cc)

Each arrow from each csv node / table represents the `data dependency` that the corresponding connected data source has. For example in the `bronze layer`, `erp_cust_info` depends on data from `cust_info.csv`.

### Data Integration Diagram

Along with recording the flow of the data, it is useful to break down each CSV file in the raw dataset and record the `relationships` between numerous `tables`. This will aid in creating the `Star Schema` in the gold layer, as we can separate out facts and dimentions based on this diagram. The figure below shows the data integration diagram which shows the relationships between entities and their `related columns`.

![DataIntegrationDiagram2](https://github.com/user-attachments/assets/3e44a6f1-879f-4941-a52a-a8366909e60f)

As shown in the integration diagram, the categories found were relations between `products`, `customers` and `sales`. This information will be useful in crafting the `Star Schema` in the **gold layer**. This diagram is also useful as we now understand which attributes correspond in different raw files. For example, the `prd_key` in `crm_prd_info` relates to the `id` in `erp_px_cat_g1v2`. This will also help us understand which `data transformations` we may need to apply to the attributes in the **silver layer**, ensuring that `LEFT JOINS` can be applied in the gold layer.

### Entity Relationship Diagram

The final modelling diagram that will be useful in creating the data warehouse is understanding the relationships between the `primary` keys and `foreign` keys in the gold layer. We can model this by using an `Entity Relationship Diagram` which can be seen in the figure below:

![EntityRelationshipDiagram2](https://github.com/user-attachments/assets/732bd135-6169-4ff1-acb5-3f113cc5ead2)

The `ER diagram` above illustrates a `strictly one to optional many` relationship between the `customer` and `product` dimention and the `sales` fact. The translation can be read as `There is only 1 customer with customer_key X in gold.dim_customers, but that same customer_key may appear 0 or more times in gold.fact_sales`, with a similar statement for `gold.dim_products`. This diagram can help us understand how many records with the same `surrogate_key` should appear in any given table.

## Data Catalog

To help `Data Analysts` in understanding the structure of the **gold layer**, a data catalog of the data warehouse **gold layer** has been constructed to explain each `fact` and `dimention` and what their attributes represent.
