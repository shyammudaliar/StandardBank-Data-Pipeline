import os
import boto3
import pymysql
import pickle
import pandas as pd
import numpy as np

def lambda_handler(event, context):
    # Connecting to RDS MySQL database
    rds_host = 'your-rds-endpoint'
    username = 'your-username'
    password = 'your-password'
    database_name = 'your-database'

    connection = pymysql.connect(host=rds_host,
                                user=username,
                                password=password,
                                database=database_name)
    
    # Getting the last processed entry from an environment variable
    last_processed_entry = get_last_processed_entry()

    # Querying new data entries after the last processed entry
    new_data_query = f"SELECT * FROM your_table WHERE entry_id > {last_processed_entry}"
    cursor = connection.cursor()
    cursor.execute(new_data_query)
    new_data = cursor.fetchall()

    # If new data is available, process data
    if new_data:
        processed_data = process_data(new_data)

        # Feeding the processed data into the machine learning model
        ml_model_input = prepare_model_input(processed_data)
        ml_model_output = run_machine_learning_model(ml_model_input)

        # Saving the machine learning model output 
        save_model_output_to_s3(ml_model_output, 'my-s3-bucket', 'output_folder', 'mlmodel_output.pkl')

        # Updating the environment variable with the latest processed entry
        update_last_processed_entry(max(entry['entry_id'] for entry in new_data))

    # Closing the database connection
    connection.close()

def get_last_processed_entry():
    # Retrieving the last processed entry from an environment variable
    return int(os.environ.get('LAST_PROCESSED_ENTRY', 0))

def process_data(data):
    # Converting the data to a Pandas DataFrame
    df = pd.DataFrame(data, columns=['entry_id', 'transaction_date', 'amount', 'merchant', 'credit_card_number'])

    # Data Cleaning: Removing rows with missing values
    df = df.dropna()

    # Feature Engineering: Extracting features from the transaction_date
    df['transaction_month'] = pd.to_datetime(df['transaction_date']).dt.month
    df['transaction_day'] = pd.to_datetime(df['transaction_date']).dt.day
    df['transaction_hour'] = pd.to_datetime(df['transaction_date']).dt.hour

    # Data Transformation: Log-transform the amount
    df['log_amount'] = df['amount'].apply(lambda x: 0 if x <= 0 else np.log(x))

    # Data Aggregation: Aggregating transaction amount by merchant
    merchant_aggregation = df.groupby('merchant')['amount'].agg(['sum', 'mean', 'count']).reset_index()
    merchant_aggregation.columns = ['merchant', 'total_amount', 'avg_amount_per_transaction', 'transaction_count']

    # Data Filtering: Keeping only transactions above a certain threshold amount
    df_filtered = df[df['amount'] > 10]

    # Combining aggregated data with the original DataFrame
    df_processed = pd.merge(df, merchant_aggregation, on='merchant', how='left')

    return df_processed

def prepare_model_input(processed_data):
    # Extracting features for model input
    features = processed_data[['transaction_month', 'transaction_day', 'transaction_hour', 'log_amount',
                               'total_amount', 'avg_amount_per_transaction', 'transaction_count']]

    # One-hot encoding categorical variables (if any)
    categorical_columns = ['merchant']
    features = pd.get_dummies(features, columns=categorical_columns, drop_first=True)

    # Reshaping the data
    model_input = features.values.reshape(-1, features.shape[1])

    return model_input


def run_machine_learning_model(model_input):
    #Loading machine learning model from a pickle file
    with open('your_model.pkl', 'rb') as model_file:
        machine_learning_model = pickle.load(model_file)

    #Performing prediction using the loaded model
    model_output = machine_learning_model.predict(model_input)
    return model_output

def update_last_processed_entry(last_processed_entry):
    # Updating the environment variable with the latest processed entry
    os.environ['LAST_PROCESSED_ENTRY'] = str(last_processed_entry)

def save_model_output_to_s3(model_output, bucket, folder, filename):
    # Saving the model output to S3
    s3 = boto3.client('s3')
    key = f"{folder}/{filename}"

    with open(filename, 'wb') as model_output_file:
        pickle.dump(model_output, model_output_file)

    s3.upload_file(filename, bucket, key)