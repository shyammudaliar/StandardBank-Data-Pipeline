from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.transforms import *
from pyspark.sql.functions import *
import pickle
import boto3

# Initializing Spark and Glue contexts
sc = SparkContext()
glueContext = GlueContext(sc)

# Extracting data from database created by Crawler
input_data = glueContext.create_dynamic_frame.from_catalog(database="your_database", table_name="your_csv_table")

# Define data processing logic
def process_data(dynamic_frame):
    # Convert the DynamicFrame to a DataFrame
    df = dynamic_frame.toDF()

    # Data Cleaning: Remove rows with missing values
    df = df.dropna()

    # Feature Engineering: Extract features from the transaction_date
    df = df.withColumn('transaction_month', month(df['transaction_date']))
    df = df.withColumn('transaction_day', dayofmonth(df['transaction_date']))
    df = df.withColumn('transaction_hour', hour(df['transaction_date']))

    # Data Transformation: Log-transform the amount
    df = df.withColumn('log_amount', when(col('amount') <= 0, 0).otherwise(log(col('amount'))))

    # Data Aggregation: Aggregate transaction amount by merchant
    merchant_aggregation = df.groupBy('merchant') \
        .agg(sum('amount').alias('total_amount'),
             avg('amount').alias('avg_amount_per_transaction'),
             count('amount').alias('transaction_count'))

    # Data Filtering: Keep only transactions above a certain threshold amount
    df_filtered = df.filter(col('amount') > 10)

    # Combine aggregated data with the original DataFrame
    df_processed = df.join(merchant_aggregation, 'merchant', 'left_outer')

    # Convert the DataFrame back to a DynamicFrame
    dynamic_frame_processed = DynamicFrame.fromDF(df_processed, glueContext, 'dynamic_frame_processed')

    return dynamic_frame_processed

# Perform data processing
processed_data = process_data(input_data)

# Download the Pickle file locally
s3 = boto3.client('s3')
s3.download_file('your_model_s3_bucket', 'your_model_s3_path/model.pkl', '/tmp/model.pkl')

# Load the machine learning model from the Pickle file
with open('/tmp/model.pkl', 'rb') as model_file:
    ml_model = pickle.load(model_file)

# Prepare data for machine learning model (replace 'your_model_s3_path' with the actual S3 path)
ml_model_input = glueContext.create_dynamic_frame.from_catalog(database="your_database", table_name="your_model_table")

# Run machine learning model
# Example: Assuming ml_model.predict is the method to make predictions
ml_model_output = ml_model.predict(ml_model_input)

# Save the machine learning model output to S3 (replace 'your_output_s3_bucket' and 'your_output_s3_path' with the actual S3 path)
glueContext.write_dynamic_frame.from_catalog(frame=ml_model_output, database="your_database", table_name="your_output_table")

# Stop the Spark and Glue contexts
sc.stop()
