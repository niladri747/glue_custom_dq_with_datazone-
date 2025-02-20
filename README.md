# Automated Custom Data Quality Checks using AWS Glue and DataZone

This solution implements automated data quality checks for data assets using AWS Glue and Datazone. It uses DynamoDB as the rules engine. It provides a flexible, rule-based approach to validate data quality across different datasets.
It provides garnualar results at a row level and also ingests summary level information to Amazon DataZone.

## Solution Overview

The solution consists of three main components:
1. A DynamoDB table storing configurable data quality rules
2. A parameterized AWS Glue script that executes these rules against specified datasets.
3. Integration to Amazon DataZone to ingest and associate the DQ evaluation metrics withe Datazone Asset (table).

### Architecture

```ascii
┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│   DynamoDB   │     │  AWS Glue    │     │   Source     │
│  Rules Table │────>│   Script     │────>│   Tables     │
└──────────────┘     └──────────────┘     └──────────────┘

```
### Setup Instructions
1. Create DynamoDB Table - First, create the DynamoDB table to store data quality rules using the AWS CLI:
```ascii  
aws dynamodb create-table \
    --table-name ctrl_data_quality_rules_tbl \ [[1]](https://repost.aws/articles/ARBTATztMgQOeYyMZ9IAmxDw/configuring-codecatalyst-dev-environments)
    --attribute-definitions \
        AttributeName=id,AttributeType=S \
    --key-schema \
        AttributeName=id,KeyType=HASH \
    --provisioned-throughput \
        ReadCapacityUnits=5,WriteCapacityUnits=5
```

2. Load Data Quality Rules - Create a JSON file (items.json) with your data quality rules and load the rules. Example structure:
```ascii
{
    "items": [
        {
            "id": {"S": "1"},
            "dq_column": {"S": "driverrating"},
            "dq_id": {"S": "rule_1"},
            "dq_level": {"S": "column"},
            "dq_name": {"S": "check_valid_insuredzip"},
            "dq_type": {"S": "accuracy"},
            "name": {"S": "domaindb.policy"},
            "query": {"S": "select * from (SELECT *,CASE WHEN length(insuredzip) > 4 THEN 'passed' ELSE 'fail' END AS result FROM domaindb.policy);"}
        }
    ]
}
Load the rules into DynamoDB:
aws dynamodb batch-write-item --request-items file://items.json
```
3. Configure the Glue Script:- The parameterized_dq_glue.py script accepts the following parameters:
```ascii
'JOB_NAME',   -- Name of the Job as this template Job will be run for any tables e.g j_dq_raw_<table_name>
'source_database', -- source database name for the table in Glue catalog
'source_table',  -- table name in Glue catalog <table_name>
'target_database',  -- database name for the target table storing the failed dq records 
'target_table', -- target table storing the failed dq records e.g. dq_failed_raw_<table_nane>
'datazone_domain_id',  -- datazone domain id. You can get this from the datazone page in AWS management console
'dq_rules_tbl_name', -- DynamoDB table name storing the the dq rules
'region',  -- region where the Glue job is running
'target_s3_bucket_name'  -- S3 bucket where the dq_failed_raw_<table_nane> table is stored.
```

#### Rule Configuration
Rule Structure
Each DQ rule in DynamoDB should contain:
```ascii
id: Unique identifier for the rule
dq_column: Column to be validated
dq_id: Rule identifier
dq_level: Level of check (column/table)
dq_name: Name of the check
dq_type: Type of check (accuracy/completeness)
name: Database.table name
query: SQL query implementing the check
```
Example Rules
ZIP Code Length Check:
```ascii
SELECT *,
CASE WHEN length(insuredzip) > 4 THEN 'passed' ELSE 'fail' END AS result 
FROM domaindb.policy
```

### Running the Solution
1. Create AWS Glue Job
Create a new AWS Glue job using the parameterized_dq_glue.py script:
Navigate to AWS Glue Console
Create a new Spark job
Upload or paste the script content
Configure job parameters:

2. Execute the Job
Run the Glue job either:
Manually through AWS Console Via AWS CLI:





