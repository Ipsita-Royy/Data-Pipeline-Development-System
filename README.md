# Data-Pipeline-Development-System

Designed and implemented an end-to-end real time ETL data pipeline pipeline that integrates and processes real-time and batch data from multiple heterogeneous sources: local CSV files, Azure Blob Storage, a public API, and MySQL. I applied the medallion architecture (bronze → silver → gold) to structure transformations, making the ETL pipeline modular, scalable, and easy to maintain. Data ingestion was done from multiple sources—local files, Azure Blob Storage, MySQL, and a public API—while handling schema mismatches, deduplication, joins, and filters as part of preprocessing. For real-time processing, I used Apache Kafka and Kafka Connect, along with the Watchdog Connector to monitor changes in local CSVs. Transformed data was stored in PostgreSQL, and validated via pgAdmin. I automated all workflows using Apache Airflow DAGs, scheduling transformation scripts ensuring smooth and scheduled pipeline runs. To conclude, I built an interactive Power BI dashboard linked to PostgreSQL to visualize key business insights—like sales trends, product performance, and employee and customer metrics.
## Tools and Technologies Uesd
Apache Spark (PySpark) – for distributed data processing and transformation

Apache Kafka – for real-time data streaming and integration

Kafka Connect + Watchdog Connector – for automated CSV ingestion and CDC

Apache Airflow – for pipeline orchestration and scheduling

PostgreSQL / pgAdmin – as your centralized data warehouse

Power BI – for business intelligence and dashboarding

Azure Blob Storage – for cloud-based file ingestion

MySQL – as a relational source system
## Conclusion

This project enabled me to build a complete, production-grade ETL pipeline capable of handling both unstructured and structured big data from diverse sources like local CSVs, Azure Blob Storage, REST APIs, and MySQL. By leveraging Apache Kafka for real-time streaming and Watchdog for monitoring local files, I ensured dynamic ingestion as soon as new data arrives. All transformation logic was implemented using PySpark, following the Medallion architecture to ensure modularity, reusability, and clean data layering. The pipeline was orchestrated using Apache Airflow which managed scheduling, monitoring and automation of all ETL jobs through DAGs. Transformed data was stored in PostgreSQL, providing a unified, clean, and query-ready data warehouse. The final layer connected Power BI to PostgreSQL for generating interactive dashboards to visualize key insights such as sales trends, employee metrics, customer distribution, and more. Overall, this end-to-end pipeline combines batch and streaming data, automation, and analytics, providing hands-on exposure to real-world data engineering challenges and solutions. It is scalable, maintainable, and ready for production environments where continuous data integration and business intelligence are critical.
