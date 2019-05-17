#!/opt/monitoring/env-py3/bin/python3.6

'''
Nutcracker stats for TSDB
'''

import socket
import sys
import time
import json

service_metrics = [
    'curr_connections',
    'total_connections',
]

cluster_metrics = [
    'client_err',
    'server_ejects',
    'client_connections',
    'forward_error',
    'fragments',
]


def get_metrics():

    host = socket.gethostname()
    port = 2013
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((host, port))

    file = s.makefile('r')
    data = file.readline()
    s.close()

    j = json.loads(data)

    return j


def main():

    try:
        data = get_metrics()
        ts = int(time.time())
        for metric in service_metrics:
            print("nutcracker.{} {} {}".format(
                metric,
                ts,
                data[metric]
            ))
        for cluster in ['cluster-localhost', 'cluster-localnetwork']:
            for metric in cluster_metrics:
                print("nutcracker.{} {} {} cluster={}".format(
                    metric,
                    ts,
                    data['cluster-localhost'][metric],
                    cluster,
                ))

    except Exception as e:
        print(e)

    sys.stdout.flush()


if __name__ == '__main__':
    main()
