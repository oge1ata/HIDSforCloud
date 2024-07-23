import gradio as gr
import pandas as pd
import plotly.express as px
from minio import Minio
from io import BytesIO
import threading
import time

# MinIO client setup
minio_client = Minio(
    '127.0.0.1:9000',
    access_key='minioadmin',
    secret_key='minioadmin',
    secure=False
)
bucket_name = 'hids-results2'

def load_data():
    try:
        objects = minio_client.list_objects(bucket_name, recursive=True)
        dfs = []
        for obj in objects:
            response = minio_client.get_object(bucket_name, obj.object_name)
            data = response.read()
            df = pd.read_csv(BytesIO(data))
            dfs.append(df)
        if dfs:
            return pd.concat(dfs, ignore_index=True)
        else:
            return pd.DataFrame()
    except Exception as e:
        print(f"Error loading data from MinIO: {e}")
        return pd.DataFrame()

# Initial loading of data with a limit
def initial_load_data():
    try:
        objects = minio_client.list_objects(bucket_name, recursive=True)
        dfs = []
        count = 0
        for obj in objects:
            if count >= 500:
                break
            response = minio_client.get_object(bucket_name, obj.object_name)
            data = response.read()
            df = pd.read_csv(BytesIO(data))
            dfs.append(df)
            count += len(df)
        if dfs:
            return pd.concat(dfs, ignore_index=True)
        else:
            return pd.DataFrame()
    except Exception as e:
        print(f"Error loading initial data from MinIO: {e}")
        return pd.DataFrame()

# Load initial data
data = initial_load_data()

def visualize_line_chart(prediction_type):
    if data.empty:
        return px.line(title="No Data Available")
    fig = px.line(data, x=data.index, y=prediction_type, title=f"{prediction_type} Over Time")
    return fig

def visualize_pie_chart():
    if data.empty:
        return px.pie(title="No Data Available")
    fig = px.pie(data, names='Prediction', title='Distribution of Anomaly Predictions')
    return fig

def display_data():
    return data

def update_data():
    global data
    data = load_data()

# Update data every minute
def scheduled_update():
    while True:
        update_data()
        time.sleep(60)  # Update every 60 seconds

# Start a separate thread for scheduled updates
update_thread = threading.Thread(target=scheduled_update)
update_thread.start()

def update_metrics():
    global data
    if data.empty:
        return "No Data Available"
    
    # Ensure columns exist before accessing
    anomaly_count_2017 = data['Anomaly Prediction 2017'].sum() if 'Anomaly Prediction 2017' in data.columns else 0
    anomaly_count_2018 = data['Anomaly Prediction 2018'].sum() if 'Anomaly Prediction 2018' in data.columns else 0
    signature_count = data['Signature Detected'].sum() if 'Signature Detected' in data.columns else 0
    total_packets = len(data)
    
    metrics_content = f"""
    - **Total Packets Processed**: {total_packets}
    - **Total Anomalies Detected (2017)**: {anomaly_count_2017}
    - **Total Anomalies Detected (2018)**: {anomaly_count_2018}
    - **Total Signatures Matched**: {signature_count}
    """
    return metrics_content

# Define Gradio interface with custom styling using inline CSS
with gr.Blocks() as demo:
    # gr.set_config(header_show=False)  # Hide default Gradio header

    gr.HTML('<style>.gr_block { background-color: black; color: pink; }</style>')  # Apply custom CSS

    gr.Markdown("# Hybrid Intrusion Detection System Dashboard")
    gr.Markdown("### Made by Ogechukwu Ata 20120612017 Set of 2024")
    
    with gr.Tab("Data Table"):
        data_table = gr.DataFrame()
        load_button = gr.Button("Load Data")
        load_button.click(display_data, outputs=data_table)
    
    with gr.Tab("Line Chart"):
        prediction_type = gr.Dropdown(choices=["Prediction"], value="Prediction")
        line_chart = gr.Plot()
        visualize_line_button = gr.Button("Visualize Line Chart")
        visualize_line_button.click(visualize_line_chart, inputs=prediction_type, outputs=line_chart)
    
    with gr.Tab("Pie Chart"):
        pie_chart = gr.Plot()
        visualize_pie_button = gr.Button("Visualize Pie Chart")
        visualize_pie_button.click(visualize_pie_chart, outputs=pie_chart)
    
    gr.Markdown("### Auto-refresh every 30 seconds")

demo.launch()
