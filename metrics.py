from prometheus_client import Gauge
from prometheus_client.core import CollectorRegistry

subsystem = 'impala_query'
base_labels = ['query_id', 'query_state', 'user', 'database']

REGISTRY = CollectorRegistry(auto_describe=False)


rows_produced = Gauge(
    'rows_produced',
    'Number of rows_produced',
    base_labels,
    registry=REGISTRY,
    subsystem=subsystem)


duration_millis = Gauge(
    'duration_millis',
    'Number of duration_millis',
    base_labels,
    registry=REGISTRY,
    subsystem=subsystem)

thread_cpu_time_percentage = Gauge(
    'thread_cpu_time_percentage',
    'Number of thread_cpu_time_percentage',
    base_labels,
    registry=REGISTRY,
    subsystem=subsystem)

thread_network_receive_wait_time = Gauge(
    'thread_network_receive_wait_time',
    'Number of thread_network_receive_wait_time',
    base_labels,
    registry=REGISTRY,
    subsystem=subsystem)


hdfs_average_scan_range = Gauge(
    'hdfs_average_scan_range',
    'Number of hdfs_average_scan_range',
    base_labels,
    registry=REGISTRY,
    subsystem=subsystem)
