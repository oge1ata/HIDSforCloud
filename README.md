
# Hybrid Intrusion Detection System (HIDS) for Storage as a Service (StaaS) Environments

## A brief description of what this project does and who it's for


This project implements a Hybrid Intrusion Detection System (HIDS) designed specifically for Storage as a Service (StaaS) environments. With the growing complexity of cyber threats and the rise in cloud-based storage solutions, traditional Intrusion Detection Systems (IDS) often fall short. This HIDS combines anomaly-based and signature-based detection methods, leveraging machine learning and predefined attack signatures to enhance security and reduce false positives in StaaS environments.

## Features
1. Hybrid Detection Approach: Combines anomaly-based detection (machine learning) with signature-based detection to capture both known and unknown threats.
2. Real-time Threat Detection: Utilizes Apache Kafka and Apache Storm to ingest and process data in real-time.
3. Scalable Storage: Uses MinIO for high-performance object storage, ideal for handling large datasets typical of StaaS systems.
4. User-Friendly Dashboard: The Gradio interface provides a comprehensive view of detected threats, metrics, and system performance.

## Technology Stack
1. Apache Kafka & Storm: For real-time data ingestion and processing.
2. Python (Scikit-Learn): For implementing machine learning models in anomaly detection.
3. MinIO: Provides scalable storage for threat data.
4. Gradio: A dashboard for visualizing metrics and alerting on suspicious activity.

## Getting Started: Prerequisites
Python 3.x
Apache Kafka
MinIO


## Installation

Install my-project with npm

```bash
  npm install my-project
  cd my-project
```
Installation: clone the repository:

```bash
Copy code
git clone https://github.com/oge1ata/HIDSforCloud.git
```

Install dependencies:

```bash
Copy code
pip install -r requirements.txt
Start Kafka: Follow Kafka documentation to set up and start Kafka.

Start MinIO: Follow MinIO documentation for setting up the object storage.
```

Run the system:

```bash
Copy code
python producer.py  # Start data producer
python consumer.py  # Start data consumer
python dashboard.py # Launch the Gradio dashboard
```

## Usage
Once all components are running, access the Gradio dashboard to monitor network traffic and detect intrusions in real time. The dashboard refreshes every 30 seconds, providing updated metrics and visualizations.


## Authors

- [@oge1ata](https://www.github.com/oge1ata)

