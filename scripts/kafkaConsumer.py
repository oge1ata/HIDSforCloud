# import pandas as pd
# import numpy as np
# import json
# import joblib
# from minio import Minio
# from io import BytesIO
# from sklearn.preprocessing import MinMaxScaler
# from kafka import KafkaConsumer

# # Load the models
# model2017 = joblib.load('C://Users//Admin//Documents//Final//HIDSProject//models//cicids2017_random_forest_model.pkl')
# model2018 = joblib.load('C://Users//Admin//Documents//Final//HIDSProject//models//cicids2018_random_forest_model.pkl')

# # MinIO client setup
# minio_client = Minio(
#     '127.0.0.1:9000',
#     access_key='minioadmin',
#     secret_key='minioadmin',
#     secure=False
# )
# bucket_name = 'hids-results2'

# # Ensure the bucket exists
# if not minio_client.bucket_exists(bucket_name):
#     minio_client.make_bucket(bucket_name)

# # Function to preprocess data for models
# def preprocess_data(data, model):
#     df = pd.DataFrame([data])

#     # Ensure numeric columns are correctly converted
#     numeric_cols = df.columns.difference(['Dst Port', 'Protocol', 'Src IP Addr', 'Dst IP Addr', 'TCP Flags', 'Service', 'DNS Queries', 'HTTP Headers', 'Payload'])
#     for col in numeric_cols:
#         try:
#             df[col] = pd.to_numeric(df[col], errors='coerce')
#         except:
#             pass

#     # Check for presence of categorical columns before applying pd.get_dummies()
#     if 'Protocol' in df.columns and 'TCP Flags' in df.columns and 'Service' in df.columns:
#         df = pd.get_dummies(df, columns=['Protocol', 'TCP Flags', 'Service'])

#     # Ensure all expected columns from the model are present
#     expected_columns = model.feature_names_in_
#     for col in expected_columns:
#         if col not in df.columns:
#             df[col] = 0

#     # Scale numeric columns
#     scaler = MinMaxScaler()
#     df[numeric_cols] = scaler.fit_transform(df[numeric_cols])

#     return df[expected_columns]

# # Function to predict using anomaly detection models
# def predict_anomaly_ensemble17(data):
#     df = preprocess_data(data, model2017)
#     prediction2017 = model2017.predict(df)
#     return prediction2017

# def predict_anomaly_ensemble18(data):
#     df = preprocess_data(data, model2018)
#     prediction2018 = model2018.predict(df)
#     return prediction2018

# # Load signature rules
# with open('C://Users//Admin//Documents//Final//HIDSProject//signatures//signatures.json') as f:
#     signature_rules = json.load(f)

# # Function to check against signature rules
# def check_signature(data):
#     for rule in signature_rules:
#         if all(data.get(k) == v for k, v in rule.items()):
#             return True
#     return False

# # Function to save data to MinIO
# def save_to_minio(data, filename):
#     csv_buffer = BytesIO()
#     df = pd.DataFrame([data])
#     df.to_csv(csv_buffer, index=False)
#     csv_buffer.seek(0)
#     minio_client.put_object(bucket_name, filename, data=csv_buffer, length=len(csv_buffer.getvalue()), content_type='application/csv')

# # Kafka Consumer setup
# consumer = KafkaConsumer(
#     'network_data',
#     bootstrap_servers='localhost:9092',
#     value_deserializer=lambda x: json.loads(x.decode('utf-8'))
# )

# # Loop to process incoming messages
# for message in consumer:
#     data = message.value

#     # Perform anomaly detection using ensemble learning (voting mechanism)
#     prediction2017 = predict_anomaly_ensemble17(data)
#     prediction2018 = predict_anomaly_ensemble18(data)
#     anomaly_prediction2017 = prediction2017[0]
#     anomaly_prediction2018 = prediction2018[0]

#     # Perform signature-based detection
#     signature_detected = check_signature(data)

#     # Combine results using voting mechanism
#     votes = [anomaly_prediction2017, anomaly_prediction2018, signature_detected]
#     final_prediction = 1 if sum(votes) >= 2 else 0  # Majority voting

#     # Prepare data for saving
#     data['Signature Detected'] = signature_detected
#     data['Anomaly Prediction 2017'] = int(anomaly_prediction2017)
#     data['Anomaly Prediction 2018'] = int(anomaly_prediction2018)
#     data['Final Prediction'] = final_prediction

#     # Save to MinIO
#     filename = f"detection_results_{message.offset}.csv"
#     save_to_minio(data, filename)
#     print(f"Saved to MinIO: {data}")




# import json
# import joblib
# from kafka import KafkaConsumer, KafkaProducer
# import pandas as pd
# from minio import Minio
# from io import BytesIO
# from sklearn.preprocessing import MinMaxScaler

# # Load the models
# model2017 = joblib.load('C://Users//Admin//Documents//Final//HIDSProject//models//cicids2017_random_forest_model.pkl')
# model2018 = joblib.load('C://Users//Admin//Documents//Final//HIDSProject//models//cicids2018_random_forest_model.pkl')

# # Load signature-based detection rules
# signature_file_path = 'C://Users//Admin//Documents//Final//HIDSProject//signatures//signatures.json'
# with open(signature_file_path, 'r') as f:
#     signature_rules = json.load(f)

# # MinIO client setup
# minio_client = Minio(
#     '127.0.0.1:9000',
#     access_key='minioadmin',
#     secret_key='minioadmin',
#     secure=False
# )
# bucket_name = 'hids-results1'

# # Ensure the bucket exists
# if not minio_client.bucket_exists(bucket_name):
#     minio_client.make_bucket(bucket_name)

# # Function to preprocess data for models
# def preprocess_data(data, model):
#     df = pd.DataFrame([data])

#     # Ensure numeric columns are correctly converted
#     numeric_cols = df.columns.difference(['Dst Port', 'Protocol', 'Src IP Addr', 'Dst IP Addr', 'TCP Flags', 'Service', 'DNS Queries', 'HTTP Headers', 'Payload'])
#     for col in numeric_cols:
#         try:
#             df[col] = pd.to_numeric(df[col], errors='coerce')
#         except:
#             pass

#     # Check for presence of categorical columns before applying pd.get_dummies()
#     if 'Protocol' in df.columns and 'TCP Flags' in df.columns and 'Service' in df.columns:
#         df = pd.get_dummies(df, columns=['Protocol', 'TCP Flags', 'Service'])

#     # Ensure all expected columns from the model are present
#     expected_columns = model.feature_names_in_
#     for col in expected_columns:
#         if col not in df.columns:
#             df[col] = 0

#     # Scale numeric columns
#     scaler = MinMaxScaler()
#     df[numeric_cols] = scaler.fit_transform(df[numeric_cols])

#     return df[expected_columns]

# # Function to predict using anomaly detection models
# def predict_anomaly_ensemble(data):
#     df_2017 = preprocess_data(data, model2017)
#     df_2018 = preprocess_data(data, model2018)
#     prediction_2017 = model2017.predict(df_2017)[0]
#     prediction_2018 = model2018.predict(df_2018)[0]
    
#     # Simple majority voting
#     final_prediction = 1 if prediction_2017 + prediction_2018 >= 1 else 0
#     return final_prediction, prediction_2017, prediction_2018

# # Function to check if a packet matches any signature rule
# def check_signature(packet):
#     for rule in signature_rules:
#         match = True
#         for key, value in rule.items():
#             if packet.get(key) != value:
#                 match = False
#                 break
#         if match:
#             return True
#     return False

# # Function to add a new malicious packet to the signature rules
# def add_to_signature(packet):
#     signature_rules.append(packet)
#     with open(signature_file_path, 'w') as f:
#         json.dump(signature_rules, f, indent=4)

# # Function to save data to MinIO
# def save_to_minio(data, filename):
#     csv_buffer = BytesIO()
#     df = pd.DataFrame([data])
#     df.to_csv(csv_buffer, index=False)
#     csv_buffer.seek(0)
#     minio_client.put_object(bucket_name, filename, data=csv_buffer, length=len(csv_buffer.getvalue()), content_type='application/csv')

# # Initialize Kafka consumer
# consumer = KafkaConsumer(
#     'network_data',
#     bootstrap_servers='localhost:9092',
#     value_deserializer=lambda x: json.loads(x.decode('utf-8'))
# )

# # Process messages from Kafka
# for message in consumer:
#     packet = message.value
    
#     # Perform signature-based detection
#     signature_detected = check_signature(packet)
#     if signature_detected:
#         packet['Prediction'] = 'Malicious'
#         print(f"Signature-based detection: {packet}")
#     else:
#         # Perform anomaly detection using ensemble method
#         final_prediction, prediction_2017, prediction_2018 = predict_anomaly_ensemble(packet)
#         packet['Anomaly Prediction 2017'] = prediction_2017
#         packet['Anomaly Prediction 2018'] = prediction_2018
#         packet['Final Prediction'] = final_prediction

#         if final_prediction == 1:
#             packet['Prediction'] = 'Malicious'
#             print(f"Anomaly-based detection: {packet}")
#             add_to_signature(packet)  # Add to signature rules
#         else:
#             packet['Prediction'] = 'Benign'
#             print(f"Benign packet: {packet}")

#     # Save to MinIO
#     filename = f"detection_results_{message.offset}.csv"
#     save_to_minio(packet, filename)
#     print(f"Saved to MinIO: {packet}")


# import json
# import joblib
# import pandas as pd
# import numpy as np
# from kafka import KafkaConsumer, KafkaProducer
# from minio import Minio
# from io import BytesIO
# from sklearn.preprocessing import MinMaxScaler

# # Load the models
# # # Load the models
# model2017 = joblib.load('C://Users//Admin//Documents//Final//HIDSProject//models//cicids2017_random_forest_model.pkl')
# model2018 = joblib.load('C://Users//Admin//Documents//Final//HIDSProject//models//cicids2018_random_forest_model.pkl')

# # Load signature-based detection rules
# signature_file_path = 'C://Users//Admin//Documents//Final//HIDSProject//signatures//signatures.json'
# with open(signature_file_path, 'r') as f:
#     signature_rules = json.load(f)

# # MinIO client setup
# minio_client = Minio(
#     '127.0.0.1:9000',
#     access_key='minioadmin',
#     secret_key='minioadmin',
#     secure=False
# )
# bucket_name = 'hids-bucket2'

# # Ensure the bucket exists
# if not minio_client.bucket_exists(bucket_name):
#     minio_client.make_bucket(bucket_name)

# # Function to preprocess data for models
# def preprocess_data(data, model):
#     df = pd.DataFrame([data])

#     # Ensure numeric columns are correctly converted
#     numeric_cols = df.columns.difference(['Dst Port', 'Protocol', 'Src IP Addr', 'Dst IP Addr', 'TCP Flags', 'Service', 'DNS Queries', 'HTTP Headers', 'Payload'])
#     for col in numeric_cols:
#         try:
#             df[col] = pd.to_numeric(df[col], errors='coerce')
#         except:
#             pass

#     # Check for presence of categorical columns before applying pd.get_dummies()
#     if 'Protocol' in df.columns and 'TCP Flags' in df.columns and 'Service' in df.columns:
#         df = pd.get_dummies(df, columns=['Protocol', 'TCP Flags', 'Service'])

#     # Ensure all expected columns from the model are present
#     expected_columns = model.feature_names_in_
#     for col in expected_columns:
#         if col not in df.columns:
#             df[col] = 0

#     # Scale numeric columns
#     scaler = MinMaxScaler()
#     df[numeric_cols] = scaler.fit_transform(df[numeric_cols])

#     return df[expected_columns]

# # Function to predict using anomaly detection models
# def predict_anomaly_ensemble(data):
#     df_2017 = preprocess_data(data, model2017)
#     df_2018 = preprocess_data(data, model2018)
#     prediction_2017 = model2017.predict(df_2017)[0]
#     prediction_2018 = model2018.predict(df_2018)[0]
    
#     # Simple majority voting
#     final_prediction = 1 if prediction_2017 + prediction_2018 >= 1 else 0
#     return final_prediction, prediction_2017, prediction_2018

# # Function to check if a packet matches any signature rule
# def check_signature(packet):
#     for rule in signature_rules:
#         match = True
#         for key, value in rule.items():
#             if packet.get(key) != value:
#                 match = False
#                 break
#         if match:
#             return True
#     return False

# # Function to add a new malicious packet to the signature rules
# def add_to_signature(packet):
#     signature_rules.append(packet)
#     with open(signature_file_path, 'w') as f:
#         json.dump(signature_rules, f, indent=4)

# # Function to save data to MinIO
# def save_to_minio(data, filename):
#     csv_buffer = BytesIO()
#     df = pd.DataFrame([data])
#     df.to_csv(csv_buffer, index=False)
#     csv_buffer.seek(0)
#     minio_client.put_object(bucket_name, filename, data=csv_buffer, length=len(csv_buffer.getvalue()), content_type='application/csv')

# # Initialize Kafka consumer
# consumer = KafkaConsumer(
#     'network_data',
#     bootstrap_servers='localhost:9092',
#     value_deserializer=lambda x: json.loads(x.decode('utf-8'))
# )

# # Process messages from Kafka
# for message in consumer:
#     packet = message.value
    
#     # Perform signature-based detection
#     signature_detected = check_signature(packet)
#     if signature_detected:
#         packet['Prediction'] = 'Malicious'
#         print(f"Signature-based detection: {packet}")
#     else:
#         # Perform anomaly detection using ensemble method
#         final_prediction, prediction_2017, prediction_2018 = predict_anomaly_ensemble(packet)
#         packet['Anomaly Prediction 2017'] = prediction_2017
#         packet['Anomaly Prediction 2018'] = prediction_2018
#         packet['Final Prediction'] = final_prediction

#         if final_prediction == 1:
#             packet['Prediction'] = 'Malicious'
#             print(f"Anomaly-based detection: {packet}")
#             add_to_signature(packet)  # Add to signature rules
#         else:
#             packet['Prediction'] = 'Benign'
#             print(f"Benign packet: {packet}")

#     # Save to MinIO
#     filename = f"detection_results_{message.offset}.csv"
#     save_to_minio(packet, filename)
#     print(f"Saved to MinIO: {packet}")




# import json
# import pandas as pd
# import numpy as np
# from kafka import KafkaConsumer, KafkaProducer
# from minio import Minio
# from io import BytesIO
# from sklearn.preprocessing import MinMaxScaler
# import tensorflow as tf
# import joblib
# import os
# import keras
# os.environ['TF_ENABLE_ONEDNN_OPTS'] = '0'

# # MinIO client setup
# minio_client = Minio(
#     '127.0.0.1:9000',
#     access_key='minioadmin',
#     secret_key='minioadmin',
#     secure=False
# )
# bucket_name = 'hids-results2'

# # Ensure the bucket exists
# if not minio_client.bucket_exists(bucket_name):
#     minio_client.make_bucket(bucket_name)

# # Load signature-based detection rules
# signature_file_path = 'C://Users//Admin//Documents//Final//HIDSProject//signatures//signatures.json'
# with open(signature_file_path, 'r') as f:
#     signature_rules = json.load(f)

# saved_model_path = 'C://Users//Admin//Documents//Final//HIDSProject//models//newmodel'
# # Load the SavedModel
# try:
#     inference_layer = keras.layers.TFSMLayer(saved_model_path, call_endpoint='serving_default')
#     print("Model loaded successfully.")
# except Exception as e:
#     print(f"Error loading the model: {e}")

# scaler = joblib.load('C://Users//Admin//Documents//Final//HIDSProject//models//newmodel//scaler.pkl')

# # Function to preprocess data for TensorFlow model
# def preprocess_data(data):
#     df = pd.DataFrame([data])

#     df.columns = df.columns.str.strip()
   
#     df_numeric = convert_to_numeric(df.copy())
#     if 'Protocol' in df_numeric.columns and 'TCP Flags' in df.columns and 'Service' in df.columns:
#         df_numeric = pd.get_dummies(df, columns=['Protocol', 'TCP Flags', 'Service'])

#     if 'Dst Port' in df_numeric.columns:
#         df_numeric['Dst Port'] = pd.to_numeric(df_numeric['Dst Port'], errors='coerce')
#         df_numeric['Dst Port'].fillna(0, inplace=True)
#         df_numeric['Dst Port'] = df_numeric['Dst Port'].astype('int64')
    
#     df_numeric.replace([np.inf, -np.inf], np.nan, inplace=True)
#     df_numeric.fillna(df_numeric.mean(), inplace=True)

#     numeric_cols = df_numeric.columns.difference(['Label'])

#     expected_columns = ['Dst Port', 'Flow Duration', 'Tot Fwd Packets', 'Flow Pkts/s', 'Flow IAT Mean', 'Fwd IAT Tot', 'Fwd IAT Mean', 'Fwd IAT Std', 'Bwd IAT Tot', 'Fwd Header Len', 'Bwd Header Len', 'Fwd Pkts/s', 'PSH Flag Count', 'Down/Up Ratio', 'Average Packet Size', 'Subflow Fwd Packets', 'Subflow Bwd Packets', 'Init_Win_bytes_forward', 'min_seg_size_forward', 'Idle Mean', 'Idle Min', 'Idle Max']
#     for col in expected_columns:
#         if col not in df_numeric.columns:
#             df[col] = 0

#     df_numeric[numeric_cols] = scaler.transform(df_numeric[numeric_cols])

#     if 'Label' in df_numeric.columns:
#         df_numeric.insert(len(df_numeric.columns) - 1, 'Label', df_numeric.pop('Label'))

#     return df_numeric[expected_columns]

# def convert_to_numeric(df):
#     """Convert object-type features (except 'Label') to numeric."""
#     numeric_cols = df.columns.difference(['Dst Port', 'Protocol', 'Src IP Addr', 'Dst IP Addr', 'TCP Flags', 'Service', 'DNS Queries', 'HTTP Headers', 'Payload'])
#     for col in numeric_cols:
#         try:
#             df[col] = pd.to_numeric(df[col], errors='coerce')
#         except:
#             pass
#     return df

# # Function to predict using TensorFlow SavedModel
# def predict(data):
#     processed_data = preprocess_data(data)
#     print(f"Processed data: {processed_data}")
#     predictions = inference_layer(processed_data)
#     print(f"Predictions: {predictions}")

#     try:
#         # Assuming your model outputs a dictionary with a single key and a tensor value
#         prediction_tensor = list(predictions.values())[0]
#         # Convert predictions to numpy if it is a tensor
#         if isinstance(prediction_tensor, tf.Tensor):
#             predictions = prediction_tensor.numpy()
#         # Assuming your model outputs a single probability value per sample
#         print('The Prediction is:')
#         print(predictions[0])
#         final_prediction = 1 if predictions[0] > 0.5 else 0
#     except KeyError as e:
#         print(f"KeyError: {e}")
#         print(f"Predictions: {predictions}")
#         return 0
#     except Exception as e:
#         print(f"Unexpected error: {e}")
#         return 0

#     return final_prediction

# # Function to check if a packet matches any signature rule
# def check_signature(packet):
#     for rule in signature_rules:
#         match = True
#         for key, value in rule.items():
#             if packet.get(key) != value:
#                 match = False
#                 break
#         if match:
#             return True
#     return False

# # Function to add a new malicious packet to the signature rules
# def add_to_signature(packet):
#     signature_rules.append(packet)
#     with open(signature_file_path, 'w') as f:
#         json.dump(signature_rules, f, indent=4)

# # Function to save data to MinIO
# def save_to_minio(data, filename):
#     csv_buffer = BytesIO()
#     df = pd.DataFrame([data])
#     df.to_csv(csv_buffer, index=False)
#     csv_buffer.seek(0)
#     minio_client.put_object(bucket_name, filename, data=csv_buffer, length=len(csv_buffer.getvalue()), content_type='application/csv')

# # Initialize Kafka consumer
# consumer = KafkaConsumer(
#     'network_data',
#     bootstrap_servers='localhost:9092',
#     value_deserializer=lambda x: json.loads(x.decode('utf-8'))
# )

# # Process messages from Kafka
# for message in consumer:
#     packet = message.value
    
#     # Perform signature-based detection
#     signature_detected = check_signature(packet)
#     if signature_detected:
#         packet['Prediction'] = 'Malicious'
#         print(f"Signature-based detection: {packet}")
#     else:
#         # Perform anomaly detection using TensorFlow SavedModel
#         final_prediction = predict(packet)
#         if final_prediction == 1:
#             packet['Prediction'] = 'Malicious'
#             print(f"Anomaly-based detection: {packet}")
#             add_to_signature(packet)  # Add to signature rules
#         else:
#             packet['Prediction'] = 'Benign'
#             print(f"Benign packet: {packet}")

#     # Save to MinIO
#     filename = f"detection_results_{message.offset}.csv"
#     save_to_minio(packet, filename)
#     print(f"Saved to MinIO: {packet}")



import json
import pandas as pd
import numpy as np
from kafka import KafkaConsumer, KafkaProducer
from minio import Minio
from io import BytesIO
from sklearn.preprocessing import MinMaxScaler
import tensorflow as tf
import joblib
import os
import keras
os.environ['TF_ENABLE_ONEDNN_OPTS'] = '0'

# MinIO client setup
minio_client = Minio(
    '127.0.0.1:9000',
    access_key='minioadmin',
    secret_key='minioadmin',
    secure=False
)
bucket_name = 'hids-results2'

# Ensure the bucket exists
if not minio_client.bucket_exists(bucket_name):
    minio_client.make_bucket(bucket_name)

# Load signature-based detection rules
signature_file_path = 'C://Users//Admin//Documents//Final//HIDSProject//signatures//signatures.json'
with open(signature_file_path, 'r') as f:
    signature_rules = json.load(f)

# saved_model_path = 'C://Users//Admin//Documents//Final//HIDSProject//models//newmodel'
# # Load the SavedModel
# try:
#     inference_layer = keras.layers.TFSMLayer(saved_model_path, call_endpoint='serving_default')
#     print("Model loaded successfully.")
# except Exception as e:
#     print(f"Error loading the model: {e}")

# scaler = joblib.load('C://Users//Admin//Documents//Final//HIDSProject//models//newmodel//scaler.pkl')

saved_model_path = 'C://Users//Admin//Documents//Final//HIDSProject//models//newmodel'
# Load the SavedModel
try:
    inference_layer = keras.layers.TFSMLayer(saved_model_path, call_endpoint='serving_default')
    print("Model loaded successfully.")
except Exception as e:
    print(f"Error loading the model: {e}")

scalered = joblib.load('C://Users//Admin//Documents//Final//HIDSProject//models//newmodel//scaler.pkl')

# Function to preprocess data for TensorFlow model
def preprocess_data(data, scaler):
    df = pd.DataFrame([data])
    
    # Strip column names
    df.columns = df.columns.str.strip()
    
    # Convert columns to numeric where possible
    df_numeric = convert_to_numeric(df.copy())
    
    # Handle Dst Port separately
    # if 'Dst Port' in df_numeric.columns:
    #     df_numeric['Dst Port'] = pd.to_numeric(df_numeric['Dst Port'], errors='coerce')
    #     df_numeric['Dst Port'].fillna(0, inplace=True)
    #     df_numeric['Dst Port'] = df_numeric['Dst Port'].astype('int64')
    
    # Replace infinite values and fill NaNs in numeric columns
    df_numeric.replace([np.inf, -np.inf], np.nan, inplace=True)
    
    # Select numeric columns
    numeric_cols = df_numeric.select_dtypes(include=[np.number]).columns
    df_numeric[numeric_cols] = df_numeric[numeric_cols].fillna(df_numeric[numeric_cols].mean())
    
    # Ensure all expected columns are present
    expected_columns = ['Dst Port', 'Flow Duration', 'Tot Fwd Packets', 'Flow Pkts/s', 'Flow IAT Mean', 'Fwd IAT Tot', 'Fwd IAT Mean', 'Fwd IAT Std', 'Bwd IAT Tot', 'Fwd Header Len', 'Bwd Header Len', 'Fwd Pkts/s', 'PSH Flag Count', 'Down/Up Ratio', 'Average Packet Size', 'Subflow Fwd Packets', 'Subflow Bwd Packets', 'Init_Win_bytes_forward', 'min_seg_size_forward', 'Idle Mean', 'Idle Min', 'Idle Max']
    for col in expected_columns:
        if col not in df_numeric.columns:
            df_numeric[col] = 0 
    
    scaler_feature_names = scaler.feature_names_in_
    for col in scaler_feature_names:
        if col not in df_numeric.columns:
            df_numeric[col] = 0  # or an appropriate default value
    
    # Align df_numeric columns with the scaler's features
    aligned_df = df_numeric[expected_columns]
    
    # Transform using the scaler
    # aligned_df[numeric_cols] = scaler.transform(aligned_df[numeric_cols])
    
    return aligned_df

def convert_to_numeric(df):
    """Convert object-type features (except 'Label') to numeric."""
    numeric_cols = df.columns.difference(['Dst Port', 'Protocol', 'Src IP Addr', 'Dst IP Addr', 'TCP Flags', 'Service', 'DNS Queries', 'HTTP Headers', 'Payload'])
    for col in numeric_cols:
        try:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        except:
            pass
    return df


# Function to predict using TensorFlow SavedModel
def predict(data):
    processed_data = preprocess_data(data, scalered)
    print(f"Processed data: {processed_data}")
    predictions = inference_layer(processed_data)
    print(f"Predictions: {predictions}")

    try:
        # Assuming your model outputs a dictionary with a single key and a tensor value
        prediction_tensor = list(predictions.values())[0]
        # Convert predictions to numpy if it is a tensor
        if isinstance(prediction_tensor, tf.Tensor):
            predictions = prediction_tensor.numpy()
        # Assuming your model outputs a single probability value per sample
        print('The Prediction is:')
        print(predictions[0])
        final_prediction = 1 if predictions[0] > 0.5 else 0
    except KeyError as e:
        print(f"KeyError: {e}")
        print(f"Predictions: {predictions}")
        return 0
    except Exception as e:
        print(f"Unexpected error: {e}")
        return 0

    return final_prediction

# Function to check if a packet matches any signature rule
def check_signature(packet):
    for rule in signature_rules:
        match = True
        for key, value in rule.items():
            if packet.get(key) != value:
                match = False
                break
        if match:
            return True
    return False

# Function to add a new malicious packet to the signature rules
def add_to_signature(packet):
    signature_rules.append(packet)
    with open(signature_file_path, 'w') as f:
        json.dump(signature_rules, f, indent=4)

# Function to save data to MinIO
def save_to_minio(data, filename):
    csv_buffer = BytesIO()
    df = pd.DataFrame([data])
    df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)
    minio_client.put_object(bucket_name, filename, data=csv_buffer, length=len(csv_buffer.getvalue()), content_type='application/csv')

# Initialize Kafka consumer
consumer = KafkaConsumer(
    'network_data',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Process messages from Kafka
for message in consumer:
    packet = message.value
    
    # Perform signature-based detection
    signature_detected = check_signature(packet)
    if signature_detected:
        packet['Prediction'] = 'Malicious'
        print(f"Signature-based detection: {packet}")
    else:
        # Perform anomaly detection using TensorFlow SavedModel
        final_prediction = predict(packet)
        if final_prediction == 1:
            packet['Prediction'] = 'Malicious'
            print(f"Anomaly-based detection: {packet}")
            add_to_signature(packet)  # Add to signature rules
        else:
            packet['Prediction'] = 'Benign'
            print(f"Benign packet: {packet}")

    # Save to MinIO
    filename = f"detection_results_{message.offset}.csv"
    save_to_minio(packet, filename)
    print(f"Saved to MinIO: {packet}")
