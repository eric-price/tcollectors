#!/opt/monitoring/env-py3/bin/python3.6

'''
Retrieve json metric dump from Kudu masters and tablet servers.
'''

import json
import logging
import requests
import sys
import yaml
import time

logging.basicConfig(level=logging.INFO)

metric_name = 'kudu'

tablet_stats = [
    'on_disk_data_size',
    'on_disk_size',
    'rows_inserted',
    'rows_upserted',
    'rows_delete',
    'rows_updated',
    'write_transactions_inflight',
    'all_transactions_inflight',
    'leader_memory_pressure_rejections',
    'follower_memory_pressure_rejections',
    'transaction_memory_pressure_rejections',
]

server_stats = [
    'tablets_num_running',
    'tablets_num_failed',
    'tablets_num_stopped',
    'generic_heap_size',
    'data_dirs_full',
    'data_dirs_failed',
    'block_cache_inserts',
    'block_cache_lookups',
    'block_cache_evictions',
    'block_cache_misses',
    'block_cache_misses_caching',
    'block_cache_hits',
    'block_cache_hits_caching',
    'block_cache_usage',
]

server_stats_avg = [
    'handler_latency_kudu_tserver_TabletServerService_Write',
    'handler_latency_kudu_tserver_TabletCopyService_FetchData',
    'handler_latency_kudu_tserver_TabletServerService_Scan',
]


def get_metrics():
    config_path = '/etc/tcollector/kudu.yaml'

    try:
        with open(config_path) as f:
            config = yaml.safe_load(f)
            host = config['host']
            port = config['port']
            url = 'http://{host}:{port}/metrics'.format(host=host, port=port)
    except (KeyError, IOError, yaml.YAMLError):
        logging.exception('Failed to get config file at {}'.format(
            config_path))
        raise

    r = requests.get(url)
    j = json.loads(r.text)
    return j


def main():
    try:
        stats = get_metrics()
        ts = int(time.time())

        for i in range(len(stats)):
            node = stats[i]

            if node['type'] == 'tablet':
                table_name = node['attributes']['table_name']
                for metric in node['metrics']:
                    if metric['name'] in tablet_stats:
                        print('{}.tablet.{} {} {} table={}'.format(
                            metric_name,
                            metric['name'],
                            ts,
                            metric['value'],
                            table_name.replace("impala::", ""),
                        ))

            if node['type'] == 'server':
                for metric in node['metrics']:
                    if metric['name'] in server_stats:
                        print('{}.server.{} {} {}'.format(
                            metric_name,
                            metric['name'],
                            ts,
                            metric['value']
                        ))
                    if metric['name'] in server_stats_avg:
                        print('{}.server.{} {} {}'.format(
                            metric_name,
                            metric['name'],
                            ts,
                            metric['mean']
                        ))

    except Exception as e:
        print(e)

    sys.stdout.flush()


if __name__ == '__main__':
    main()
