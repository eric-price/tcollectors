#!/opt/monitoring/env-py3/bin/python3.6
"""Jaeger stats for TSDB"""

import requests
import socket
import time

KEYS = frozenset([
    "jaeger_collector_spans_saved_by_svc_total",
    "jaeger_collector_traces_saved_by_svc_total"
    ])


def get_metric():
    ip = socket.gethostname()
    port = 14269
    r = requests.get("http://{0}:{1}/metrics".format(ip, port))
    lines = r.text

    return lines


def main():
    while True:
        try:
            metrics = get_metric()
            for stat in metrics.splitlines():
                metric = stat.split()[0].split('{')
                value = stat.split()[1]
                ts = int(time.time())
                if metric[0] in KEYS:
                    tag = metric[1].split("svc=")
                    tag = str(tag[1]).strip("}").strip('"')
                    print("jaeger.{0} {1} {2} component={3}".format(
                        metric[0], ts, value, tag))

        except Exception as e:
            print(e)

        time.sleep(15)


if __name__ == "__main__":
    main()
