import json
import logging
from typing import List, Dict, Tuple

import requests
from flask import Flask, jsonify, Response, render_template, url_for, request
from prometheus_client import generate_latest

import redis
from metrics import *


app = Flask(__name__)

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

CM_URL = 'http://127.0.0.1:7180/api/v17'

redis_client = redis.StrictRedis(host='127.0.0.1', port=6379)


class ImpalaQueryAttrs:
    def __init__(self, **kwargs):
        self.pool = kwargs.get('pool')
        self.thread_cpu_time_percentage = kwargs.get(
            'thread_cpu_time_percentage')
        self.thread_network_receive_wait_time = kwargs.get(
            'thread_network_receive_wait_time')
        self.hdfs_average_scan_range = kwargs.get('hdfs_average_scan_range')
        self.bytes_streamed = kwargs.get('bytes_streamed')
        self.memory_spilled = kwargs.get('memory_spilled')
        self.hdfs_bytes_read = kwargs.get('hdfs_bytes_read')
        self.hdfs_scanner_average_bytes_read_per_second = kwargs.get(
            'hdfs_scanner_average_bytes_read_per_second')
        self.estimated_per_node_peak_memory = kwargs.get(
            'estimated_per_node_peak_memory')


class ImpalaQuery:
    def __init__(self, **kwargs):
        self.query_id = kwargs.get('queryId')
        self.statement = kwargs.get('statement')
        self.query_state = kwargs.get('queryState')
        self.rows_produced = kwargs.get('rowsProduced')
        self.user = kwargs.get('user')
        self.database = kwargs.get('database')
        self.duration_millis = kwargs.get('durationMillis')

        self.attributes = ImpalaQueryAttrs(**kwargs.get('attributes'))

    def read_raw():
        pass

    @staticmethod
    def _do_request(url):
        resp = requests.post(url)
        return resp.json()

    @staticmethod
    def cancel(query_id):
        logging.debug(f'Cancel query {query_id}')
        cancel_url = CM_URL + \
            f'/clusters/cluster/services/impala/impalaQueries/{query_id}/cancel'
        return ImpalaQuery._do_request(cancel_url)

    def __repr__(self):
        return f'ImpalaQuery<query_id={self.query_id}>'


def download_impala_running_queris() -> List[Dict]:
    query_url = CM_URL + \
        '/clusters/cluster/services/impala/impalaQueries?filter=(query_state=RUNNING)&limit=1000'
    redis_cache_key = 'impala:queries:raw'

    resp = redis_client.get(redis_cache_key)

    if not resp:
        logging.info('Downloading impala running queris...')
        try:
            response = requests.get(query_url)
        except requests.exceptions.ConnectionError as e:
            logging.error(f'Connection {query_url} timeout!')
            return []

        if response.status_code != 200:
            logging.error(f'Error: {response.text}')
            return

        redis_client.set(redis_cache_key, response.text, ex=120)

        return response.json()['queries']

    return json.loads(resp)['queries']


def from_json() -> Tuple[List[ImpalaQuery], List[Dict]]:
    running_queris = download_impala_running_queris()

    running_query_objs = []
    for running_query in running_queris:
        impala_query = ImpalaQuery(**running_query)
        running_query_objs.append(impala_query)

    return running_query_objs, running_queris


def collect(running_query_objs: List[ImpalaQuery]):
    for running_query in running_query_objs:

        rows_produced.clear()
        duration_millis.clear()
        thread_cpu_time_percentage.clear()
        thread_network_receive_wait_time.clear()
        hdfs_average_scan_range.clear()

        rows_produced.labels(query_id=running_query.query_id, query_state=running_query.query_state,
                             user=running_query.user, database=running_query.database).set(running_query.rows_produced or 0)

        duration_millis.labels(query_id=running_query.query_id, query_state=running_query.query_state,
                               user=running_query.user, database=running_query.database).set(running_query.duration_millis or 0)

        thread_cpu_time_percentage.labels(query_id=running_query.query_id, query_state=running_query.query_state,
                                          user=running_query.user, database=running_query.database).set(running_query.attributes.thread_cpu_time_percentage or 0)

        thread_network_receive_wait_time.labels(query_id=running_query.query_id, query_state=running_query.query_state,
                                                user=running_query.user, database=running_query.database).set(running_query.attributes.thread_network_receive_wait_time or 0)

        hdfs_average_scan_range.labels(query_id=running_query.query_id, query_state=running_query.query_state,
                                       user=running_query.user, database=running_query.database).set(running_query.attributes.hdfs_average_scan_range or 0)


@app.route('/metrics', methods=['GET', 'OPTIONS'])
def metrics():
    running_query_objs, _ = from_json()
    collect(running_query_objs)

    return Response(generate_latest(REGISTRY),
                    mimetype="text/plain")


@app.route('/proxy/raw_query', methods=['GET'])
def raw_query():
    _, running_queris = from_json()
    return jsonify(running_queris)


@app.route('/cancel_query', methods=['POST'])
def cancel_query():
    query_id = request.get_json().get('query_id')
    if not query_id:
        return jsonify({'error': 'query_id is required'})

    impala_query = ImpalaQuery.cancel(query_id=query_id)
    if impala_query == {}:
        msg = 'ok'
    else:
        msg = 'error: {}'.format(impala_query)

    return jsonify({'code': 0, 'msg': msg})


@app.route('/', methods=['OPTIONS', 'GET'])
def index():
    endpoints = [
        url_for('metrics'),
        url_for('raw_query'),
        url_for('cancel_query'),
    ]
    return render_template('index.html', endpoints=endpoints)


app.run(host='0.0.0.0', port=9999, debug=True)
