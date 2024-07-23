
import pandas as pd
import numpy as np
import json
import os
from kafka import KafkaProducer

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Function to load and preprocess CSV data chunk
def preprocess_csv_chunk(chunk):
    # Strip leading/trailing spaces from column names
    chunk.columns = chunk.columns.str.strip()

    # Drop duplicates and NaN values
    chunk.drop_duplicates(inplace=True)
    chunk.dropna(inplace=True)

    # Drop the 'Label' column if it exists
    if 'Label' in chunk.columns:
        chunk.drop(['Label'], axis=1, inplace=True)

    # Drop the 'Timestamp' column if it exists
    if 'Timestamp' in chunk.columns:
        chunk.drop(['Timestamp'], axis=1, inplace=True)

    # Convert object types to numeric where possible
    for col in chunk.columns:
        chunk[col] = pd.to_numeric(chunk[col], errors='coerce')

    # One-hot encode the 'Protocol' column if it exists
    if 'Protocol' in chunk.columns:
        chunk = pd.get_dummies(chunk, columns=['Protocol'])

    # Replace inf/-inf with NaN, then fill NaN values with the mean of numeric columns
    chunk.replace([np.inf, -np.inf], np.nan, inplace=True)
    numeric_cols = chunk.select_dtypes(include=[np.number]).columns
    chunk[numeric_cols] = chunk[numeric_cols].fillna(chunk[numeric_cols].mean())

    return chunk

# Function to load data from all CSV files in a directory
# def load_data(directory):
#     df_main = pd.DataFrame()
#     chunksize = 7  # Adjust the chunk size as needed
#     for dirname, _, filenames in os.walk(directory):
#         for filename in filenames:
#             file_path = os.path.join(dirname, filename)
#             if file_path.endswith('.csv'):
#                 print(f"Loading file: {file_path}")
#                 for chunk in pd.read_csv(file_path, chunksize=chunksize, low_memory=False):
#                     df_main = pd.concat([df_main, preprocess_csv_chunk(chunk)], ignore_index=True)
#     return df_main

# # Example usage
# dataset_directory = 'C://Users//Admin//Documents//Final//HIDSProject//signatures//testing1.csv' 
# df = load_data(dataset_directory)
# print('loaded')

# # Send data to Kafka
# for _, row in df.iterrows():
#     message = row.to_dict()
#     producer.send('network_data', value=message)
#     count += 1
#     if count % 10 == 0:  
#         print(f"Sent {count} messages to Kafka")
    

# # Ensure all messages are sent before exiting
# producer.flush()
# print("All messages sent to Kafka.")


# Load and preprocess the CSV file in chunks
csv_file_path = 'C://Users//Admin//Documents//Final//HIDSProject//signatures//testing1.csv'  # Update this path to your CSV file

chunk_size = 10 # Adjust the chunk size based on your system's memory capacity
count = 0

for chunk in pd.read_csv(csv_file_path, chunksize=chunk_size, low_memory=False):
    chunk = preprocess_csv_chunk(chunk)

    # Send each row to Kafka
    for _, row in chunk.iterrows():
        message = row.to_dict()
        producer.send('network_data', value=message)
        count += 1
        if count % 10 == 0:  # Print progress every 100 messages
            print(f"Sent {count} messages to Kafka")

# Ensure all messages are sent before exiting
producer.flush()
print("All messages sent to Kafka.")

