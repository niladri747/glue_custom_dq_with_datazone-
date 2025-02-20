import sys
import json
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *
import boto3
from datetime import datetime
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as F
from pyspark.sql.types import *
  
# Get job parameters
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'source_database',
    'source_table',
    'target_database',
    'target_table',
    'datazone_domain_id',
    'dq_rules_tbl_name',
    'region',
    'target_s3_bucket_name'
])

# Initialize Spark and Glue contexts
#sc = SparkContext()
glueContext = GlueContext(sc)
#spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
def get_datazone_asset_id(datazone, dzDomain, tableName, schemaName, maxResults: int) -> str:
    """
    Search for an asset in Amazon DataZone.

    Args:
        datazone (boto3.client): The Amazon DataZone client.
        dzDomain (str): The domain identifier.
        tableName (str): The name of the table.
        schemaName (str): The name of the schema.
        maxResults (int): The maximum number of results to return.

    Returns:
        list: The list of entity identifiers for the asset, or None if not found.
    """
    print(f'starting search ... ')
        
    entity_identifier_list=[]
        
    try:
        response = datazone.search_listings(
            additionalAttributes=['FORMS'],
            domainIdentifier=dzDomain,
            maxResults=maxResults,
            searchText=tableName
        )

        for item in response['items']:
            forms_dict = json.loads(item['assetListing']['additionalAttributes']['forms'])
            if ('RedshiftTableForm' in forms_dict and
                forms_dict['RedshiftTableForm']['schemaName'] == schemaName and
                forms_dict['RedshiftTableForm']['tableName'] == tableName) or \
                ('GlueTableForm' in forms_dict and
                f"table/{schemaName}/{tableName}" in forms_dict['GlueTableForm']['tableArn']):
                entity_identifier=item['assetListing']['entityId']
                print(f"DZ Asset Id: {entity_identifier_list}")
                entity_identifier_list.append(entity_identifier)
            else:
                print(f'No matching asset found in this iteration')
            
        print(f"DZ Asset Id list: {entity_identifier_list}")
        return entity_identifier_list
    except Exception as e:
        print(f"Error searching for asset ID: {e}")
        raise DataQualityJobError(f"Error searching for asset ID: {e}")
def post_to_datazone(rule, result_df, datazone_client, domain_id, asset_id):
    """Post data quality results to DataZone"""
    if 'result' not in result_df.columns:
        raise ValueError("DataFrame does not contain 'result' column")
    
    total_count = result_df.count()
    pass_count = result_df.filter(F.col("result") == "passed").count()
    passing_percentage = (pass_count / total_count * 100) if total_count > 0 else 0

    # Prepare the evaluation object
    evaluation = {
        'description': f"Data quality check: {rule['dq_name']}",
        'applicableFields': [rule['dq_column']],
        'types': [rule['dq_type']],
        'status': 'PASS' if passing_percentage == 100 else 'FAIL',
        'details': {
            'EVALUATION_DETAILS': f"Rule Type: {rule['dq_type']}, Level: {rule['dq_level']}, Column: {rule['dq_column']}"
        }
    }

    try:
        datazone_client.post_time_series_data_points(
            domainIdentifier=domain_id,
            entityIdentifier=asset_id,
            entityType='ASSET',
            forms=[
                {
                    "content": json.dumps({
                        "evaluationsCount": total_count,
                        "evaluations": [evaluation],
                        "passingPercentage": passing_percentage
                    }),
                    "formName": f"DQ_Rule_{rule['dq_id']}",
                    "typeIdentifier": "amazon.datazone.DataQualityResultFormType",
                    "timestamp": datetime.now().isoformat()
                }
            ]
        )
        print(f"Successfully posted results to DataZone for rule {rule['dq_id']}")
    except Exception as e:
        print(f"Error posting to DataZone for rule {rule['dq_id']}: {str(e)}")

def get_sql_rules_from_dynamodb(table_name):
    """Fetch SQL rules from DynamoDB based on table name"""
    try:
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table(args['dq_rules_tbl_name'])
        
        # Using scan with filter expression
        response = table.scan(
            FilterExpression='#n = :table_name',
            ExpressionAttributeNames={
                '#n': 'name'  # Using expression attribute name as 'name' is a reserved word
            },
            ExpressionAttributeValues={
                ':table_name': table_name
            }
        )
        
        items = response['Items']
        
        # Handle pagination if there are more items
        while 'LastEvaluatedKey' in response:
            response = table.scan(
                FilterExpression='#n = :table_name',
                ExpressionAttributeNames={
                    '#n': 'name'
                },
                ExpressionAttributeValues={
                    ':table_name': table_name
                },
                ExclusiveStartKey=response['LastEvaluatedKey']
            )
            items.extend(response['Items'])
            
        print(f"Found {len(items)} items for table {table_name}")
        return items
        
    except Exception as e:
        print(f"Error scanning DynamoDB table: {str(e)}")
        return []
def execute_sql_check(spark, df, rule):
    """Execute a SQL check and return results"""
    df.createOrReplaceTempView(rule['name'].split('.')[-1])
    
    try:
        result = spark.sql(rule['query'])
        
        # Add metadata columns
        result = result.withColumn("dq_id", lit(rule['dq_id']))
        result = result.withColumn("dq_type", lit(rule['dq_type']))
        result = result.withColumn("dq_level", lit(rule['dq_level']))
        result = result.withColumn("dq_column", lit(rule['dq_column']))
        result = result.withColumn("dq_name", lit(rule['dq_name']))
        result = result.withColumn("execution_timestamp", lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
        return result
    except Exception as e:
        print(f"Error executing rule {rule['dq_name']}: {str(e)}")
        return None
# Initialize DataZone client
datazone_client = boto3.client(
    service_name='datazone',
    region_name=args['region']
)
# Get asset ID from DataZone
asset_id = get_datazone_asset_id(
    datazone_client,
    args['datazone_domain_id'],
    args['source_table'],
    args['source_database'],
    10
    )
print(asset_id)
# Create full table name as it appears in DynamoDB
full_table_name = f"{args['source_database']}.{args['source_table']}"
# Read source table
source_dyf = glueContext.create_dynamic_frame.from_catalog(
    database=args['source_database'],
    table_name=args['source_table']
)
source_df = source_dyf.toDF()
sql_rules = get_sql_rules_from_dynamodb(full_table_name)
print(sql_rules)
from pyspark.sql import functions as F
from pyspark.sql.types import *
import json
from datetime import datetime

asset_id = ''.join(asset_id)
all_results = None
    
for rule in sql_rules:
    result = execute_sql_check(spark, source_df, rule)
        
    if result is not None:
        # Post results to DataZone
        post_to_datazone(
            rule, 
            result, 
            datazone_client, 
            args['datazone_domain_id'], 
            asset_id
        )
            
        if all_results is None:
            all_results = result
        else:
            all_results = all_results.union(result)
# Add additional metadata

failed_results = all_results.filter(F.col("result") == "fail")

final_results = failed_results.withColumn("source_table", lit(full_table_name))
final_results = final_results.withColumn("check_date", current_date())

target_database_name = args['target_database']
target_table_name = args['target_table']
target_s3_name = args['target_s3_bucket_name']


final_results.createOrReplaceTempView("temp_dq_results")
sql_query = f"""CREATE TABLE IF NOT EXISTS {target_database_name}.{target_table_name}
            LOCATION 's3://{target_s3_name}/{target_table_name}/'
            AS SELECT * FROM temp_dq_results WHERE 1=3"""

spark.sql(sql_query)
# Convert back to DynamicFrame
results_dyf = DynamicFrame.fromDF(final_results, glueContext, "results_dyf")

# Write results to target table
glueContext.write_dynamic_frame.from_catalog(
    frame=results_dyf,
    database=args['target_database'],
    table_name=args['target_table'],
    transformation_ctx="write_results"
)
job.commit()