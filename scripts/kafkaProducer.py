# # import subprocess
# # from scapy.all import sniff, IP, TCP, UDP
# # from kafka import KafkaProducer
# # import json
# # import time

# # def on_send_success(record_metadata):
# #     print(f'Message delivered to {record_metadata.topic} partition {record_metadata.partition} offset {record_metadata.offset}')

# # def on_send_error(excp):
# #     print(f'I am an errback {excp}')

# # producer = KafkaProducer(
# #     bootstrap_servers='localhost:9092',
# #     value_serializer=lambda v: json.dumps(v).encode('utf-8')
# # )

# # data = []
# # for _ in range(200):
# #     sample = {
# #         "Dst Port": random.choice([80, 443, 8080]),
# #         "Flow Duration": random.randint(500, 5000),
# #         "Tot Fwd Pkts": random.randint(10, 50),
# #         "Tot Bwd Pkts": random.randint(5, 25),
# #         "TotLen Fwd Pkts": random.randint(500, 2000),
# #         "TotLen Bwd Pkts": random.randint(500, 2000),
# #         "Protocol": random.choice([0, 6, 17]),
# #         "Src IP Addr": f"192.168.{random.randint(0, 255)}.{random.randint(0, 255)}",
# #         "Dst IP Addr": f"10.0.{random.randint(0, 255)}.{random.randint(0, 255)}",
# #         "Fwd Pkt/Byte Count": random.randint(1, 100),
# #         "Bwd Pkt/Byte Count": random.randint(1, 100),
# #         "TCP Flags": random.choice(["SYN", "ACK", "FIN", "RST"]),
# #         "Service": random.choice(["HTTP", "DNS", "FTP"]),
# #         "Time": int(time.time()),
# #         "DNS Queries": random.choice([True, False]),
# #         "HTTP Headers": {"User-Agent": "Mozilla/5.0", "Accept-Language": "en-US"},
# #         "Payload": "Lorem ipsum dolor sit amet, consectetur adipiscing elit."
# #     }
# #     data.append(sample)

# # for record in data:
# #     producer.send('network_data', record).add_callback(on_send_success).add_errback(on_send_error)
# #     time.sleep(1)

# # producer.flush()

# from scapy.all import sniff, IP, TCP, UDP
# from kafka import KafkaProducer
# import json
# import time

# # Kafka producer setup
# producer = KafkaProducer(
#     bootstrap_servers='localhost:9092',
#     value_serializer=lambda v: json.dumps(v).encode('utf-8')
# )

# # Callback function for successful message delivery
# def on_send_success(record_metadata):
#     print(f'Message delivered to {record_metadata.topic} partition {record_metadata.partition} offset {record_metadata.offset}')

# # Callback function for message delivery failure
# def on_send_error(excp):
#     print(f'I am an errback {excp}')

# # Function to extract packet features
# def extract_features(packet):
#     features = {
#         "Src IP Addr": packet[IP].src,
#         "Dst IP Addr": packet[IP].dst,
#         "Protocol": packet[IP].proto,
#         "Flow Duration": 0,  # Placeholder, real duration calculation requires packet timing info
#         "Tot Fwd Pkts": 1,  # Placeholder, would need more packet context
#         "Tot Bwd Pkts": 0,  # Placeholder, would need more packet context
#         "TotLen Fwd Pkts": len(packet[IP]),  # Example feature
#         "TotLen Bwd Pkts": 0,  # Placeholder, would need more packet context
#         "Dst Port": packet[TCP].dport if TCP in packet else (packet[UDP].dport if UDP in packet else None),
#         "Fwd Pkt/Byte Count": 1,  # Example feature
#         "Bwd Pkt/Byte Count": 0,  # Placeholder, would need more packet context
#         "TCP Flags": packet.sprintf("%TCP.flags%") if TCP in packet else "",
#         "Service": "",  # Placeholder, determine based on port or other heuristics
#         "Time": int(packet.time),
#         "DNS Queries": False,  # Placeholder, need DNS packet parsing
#         "HTTP Headers": {},  # Placeholder, need HTTP packet parsing
#         "Payload": str(packet[IP].payload),
#         "Flow Pkts/s": 1,
#         "Flow IAT Mean": 1,
#         "Fwd IAT Tot": 0,
#         "Fwd IAT Mean": 1,
#         "Fwd IAT Std": 1,
#         "Bwd IAT Tot": 0,
#         "Fwd Header Len": 1,
#         "Bwd Header Len": 0,       
#         "Fwd Pkts/s": 0,
#         "PSH Flag Count": 1,
#         "Down/Up Ratio": 0,
#         "Average Packet Size": 1,
#         "Subflow Fwd Pkts": 1,
#         "Subflow Bwd Pkts": 1,
#         "Init_Win_bytes_forward": 2,
#         "min_seg_size_forward": 2,
#         "Idle Mean": 1,
#         "Idle Min": 0,
#         "Idle Max": 0, 
#     }

#     return features

# # Packet handler function
# def packet_handler(packet):
#     if IP in packet:
#         features = extract_features(packet)
#         producer.send('network_data', features).add_callback(on_send_success).add_errback(on_send_error)
#         print(f"Packet captured and sent to Kafka: {features}")

# # Function to capture packets on the loopback interface using Scapy
# def capture_packets(interface, capture_filter):
#     print(f"Starting packet capture on interface '{interface}' with filter '{capture_filter}'...")
#     sniff(iface=interface, filter=capture_filter, prn=packet_handler)

# # Replace 'lo' with your loopback interface name and 'loopback' with appropriate filter
# loopback_interface = '\\Device\\NPF_Loopback'
# capture_packets(loopback_interface, 'tcp or udp')

# # Ensure all messages are sent before exiting
# producer.flush()
from scapy.all import sniff, IP, TCP, UDP
from kafka import KafkaProducer
import json
import time
import statistics

# Kafka producer setup
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Callback function for successful message delivery
def on_send_success(record_metadata):
    print(f'Message delivered to {record_metadata.topic} partition {record_metadata.partition} offset {record_metadata.offset}')

# Callback function for message delivery failure
def on_send_error(excp):
    print(f'I am an errback {excp}')

# Function to calculate flow-based features
class FlowFeatures:
    def __init__(self):
        self.fwd_packets = []
        self.bwd_packets = []
        self.flow_start_time = None
        self.idle_times = []

    def update(self, packet):
        current_time = packet.time
        if self.flow_start_time is None:
            self.flow_start_time = current_time

        flow_duration = current_time - self.flow_start_time

        if packet[IP].src in ('127.0.0.1', 'localhost'):
            self.fwd_packets.append(packet)
        else:
            self.bwd_packets.append(packet)

        if len(self.fwd_packets) > 1:
            self.idle_times.append(current_time - self.fwd_packets[-2].time)

        features = {
            "Flow Duration": flow_duration,
            "Tot Fwd Packets": len(self.fwd_packets),
            # "Tot Bwd Packets": len(self.bwd_packets),
            # "TotLen Fwd Packets": sum(len(p) for p in self.fwd_packets),
            # "TotLen Bwd Packets": sum(len(p) for p in self.bwd_packets),
            "Flow Pkts/s": (len(self.fwd_packets) + len(self.bwd_packets)) / flow_duration if flow_duration > 0 else 0,
            "Flow IAT Mean": statistics.mean(self._inter_arrival_times(self.fwd_packets + self.bwd_packets)) if len(self.fwd_packets) + len(self.bwd_packets) > 1 else 0,
            "Fwd IAT Tot": sum(self._inter_arrival_times(self.fwd_packets)) if len(self.fwd_packets) > 1 else 0,
            "Fwd IAT Mean": statistics.mean(self._inter_arrival_times(self.fwd_packets)) if len(self.fwd_packets) > 1 else 0,
            "Fwd IAT Std": statistics.stdev(self._inter_arrival_times(self.fwd_packets)) if len(self._inter_arrival_times(self.fwd_packets)) > 1 else 0,
            "Bwd IAT Tot": sum(self._inter_arrival_times(self.bwd_packets)) if len(self.bwd_packets) > 1 else 0,
            "Fwd Header Len": sum(p[IP].ihl for p in self.fwd_packets) * 4,
            "Bwd Header Len": sum(p[IP].ihl for p in self.bwd_packets) * 4,
            "Fwd Pkts/s": len(self.fwd_packets) / flow_duration if flow_duration > 0 else 0,
            "PSH Flag Count": sum(1 for p in self.fwd_packets if p.sprintf("%TCP.flags%") == 'P'),
            "Down/Up Ratio": len(self.bwd_packets) / len(self.fwd_packets) if len(self.fwd_packets) > 0 else 0,
            "Average Packet Size": (sum(len(p) for p in self.fwd_packets + self.bwd_packets) / (len(self.fwd_packets) + len(self.bwd_packets))) if len(self.fwd_packets) + len(self.bwd_packets) > 0 else 0,
            "Subflow Fwd Packets": len(self.fwd_packets),
            "Subflow Bwd Packets": len(self.bwd_packets),
            "Init_Win_bytes_forward": self.fwd_packets[0][TCP].window if len(self.fwd_packets) > 0 and TCP in self.fwd_packets[0] else 0,
            "min_seg_size_forward": min(len(p) for p in self.fwd_packets) if len(self.fwd_packets) > 0 else 0,
            "Idle Mean": statistics.mean(self.idle_times) if len(self.idle_times) > 0 else 0,
            "Idle Min": min(self.idle_times) if len(self.idle_times) > 0 else 0,
            "Idle Max": max(self.idle_times) if len(self.idle_times) > 0 else 0,
        }

        return features

    def _inter_arrival_times(self, packets):
        return [packets[i].time - packets[i-1].time for i in range(1, len(packets))]

flow_features = FlowFeatures()

# Function to extract packet features
def extract_features(packet):
    base_features = {
        "Src IP Addr": packet[IP].src,
        "Dst IP Addr": packet[IP].dst,
        # "Protocol": packet[IP].proto,
        "Dst Port": packet[TCP].dport if TCP in packet else (packet[UDP].dport if UDP in packet else None),
        "TCP Flags": packet.sprintf("%TCP.flags%") if TCP in packet else "",
        # "Time": int(packet.time),
        "Payload": str(packet[IP].payload)
    }

    flow_based_features = flow_features.update(packet)

    features = {**base_features, **flow_based_features}
    return features

# Packet handler function
def packet_handler(packet):
    if IP in packet:
        features = extract_features(packet)
        producer.send('network_data', features).add_callback(on_send_success).add_errback(on_send_error)
        print(f"Packet captured and sent to Kafka: {features}")

# Function to capture packets on the loopback interface using Scapy
def capture_packets(interface, capture_filter):
    print(f"Starting packet capture on interface '{interface}' with filter '{capture_filter}'...")
    sniff(iface=interface, filter=capture_filter, prn=packet_handler)

# Replace 'lo' with your loopback interface name and 'loopback' with appropriate filter
loopback_interface = '\\Device\\NPF_Loopback'
capture_packets(loopback_interface, 'tcp or udp')

# Ensure all messages are sent before exiting
producer.flush()
