# Stock Ticker ETL Pipeline
An ETL pipeline constructed to ingest ticker data from Yahoo! Finance's API into a data lake and then transform the data before loading it into a data warehouse for querying.
### Tick Data
The tick data is being ingested from Yahoo! Finance's API through a Python library called yfinance.

https://ranaroussi.github.io/yfinance/#download-market-data-from-yahoo-finance-s-api
https://ranaroussi.github.io/yfinance/reference/index.html
### Data Lake
The ingested data will be stored in a centralised repository in raw Parquet format. The service used for the data lake will be Amazon S3 due to its scalability, durability, versioning and security. The data lake will be designed with a multi-tier architecture with a raw zone and a cleaned zone for the data. Upon ingestion, the data will be stored in the raw zone of the data lake.

As for each zone, the data will be split up into partitions through the event date in order to optimise the retrieval of data.

https://docs.aws.amazon.com/AmazonS3/latest/userguide/Welcome.html
### Metadata Repository
The data from the raw zone will be scanned for information on the metadata of the dataset and update the inferred schema using a catalog. This catalog simplifies the extraction of data from S3 as well as utilises partitions effectively. For this project, the repository that has been used is Glue Data Catalog and the dataset is scanned using Glue crawlers for data discovery.

https://docs.aws.amazon.com/glue/latest/dg/catalog-and-crawler.html
### ETL pipelines
##### Raw to Cleaned
This ETL pipeline will extract data from the raw zone of the data lake, clean it and load the resultant data into the cleaned zone of the data lake. Glue jobs are used for this with PySpark for the logic. Logs and metrics are sent to CloudWatch.
##### Cleaned to Data Warehouse
This ETL pipeline will extract data from the cleaned zone of the data lake, perform transformations to provide further information on the data collected. These are: 
* Lee-Ready Trade Direction Classifiction -> to infer whether a buyer or seller initiated the trade
* VWAP (Volume-Weighted Average Price) -> to benchmark the price of which the asset is being bought at
* Trade Imbalance -> to infer the current momentum of the market

The resultant data is loaded into the data warehouse for querying.

https://docs.aws.amazon.com/glue/latest/dg/what-is-glue.html

### Data Warehouse
The data warehouse used for this project is Amazon Redshift due to its performance, scalability and efficiency. The schema design for the data has been optimised for SQL queries using Athena.

https://docs.aws.amazon.com/redshift/latest/dg/welcome.html

## Getting Started

1. **Create and activate virtual environment**

On Windows, please use source .venv/Scripts/activate

```bash
python -m venv .venv
source .venv/bin/activate
```

2. **Install pip-tools**:

```bash
pip install pip-tools
```

3. **Install project dependencies**

On Windows, use WSL or Git Bash to run make commands.

```bash
make install
```