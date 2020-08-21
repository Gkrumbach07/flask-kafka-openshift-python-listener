import argparse
import copy
import logging
import os
import threading
from datetime import datetime, timedelta

import flask
from flask import views
from flask import json
import kafka

from prometheus_client import Histogram, make_wsgi_app
from werkzeug.middleware.dispatcher import DispatcherMiddleware


h = Histogram('predictions', 'Description of histogram', ["id", "day"], buckets=(0.3, 0.5, 0.6, 0.7, 0.8, 0.9, 1, float("inf")))
access_lock = threading.Lock()
exit_event = threading.Event()


def index():
    return "Root"


def server(args):
    logging.info('starting flask server')
    # create the flask app object
    app = flask.Flask(__name__)
    # change this value for production environments
    app.config['SECRET_KEY'] = 'secret!'

    app.add_url_rule('/', 'index', index)

    # Add prometheus wsgi middleware to route /metrics requests
    app.wsgi_app = DispatcherMiddleware(app.wsgi_app, {
        '/metrics': make_wsgi_app()
    })

    app.run(host='0.0.0.0', port=8080)
    logging.info('exiting flask server')


def consumer(args):
    logging.info('starting kafka consumer')
    consumer = kafka.KafkaConsumer(args.topic, bootstrap_servers=args.brokers)
    for msg in consumer:
        print(msg)
        if exit_event.is_set():
            break
        try:
            for i, pred in enumerate(json.loads(msg.value.decode('utf8'))['solar']):
                pred_day = datetime.strptime(json.loads(msg.value.decode('utf8'))['start_day'], '%Y-%m-%d')
                pred_day = pred_day + timedelta(days=1)
                h.labels(id=json.loads(msg.value.decode('utf8'))['id'], day=str(pred_day)).observe(pred)
        except Exception as e:
            logging.error(e.message)
    logging.info('exiting kafka consumer')


def main(args):
    logging.basicConfig(level=logging.INFO)
    logging.info('starting flask-kafka-listener')
    exit_event.clear()
    cons = threading.Thread(group=None, target=consumer, args=(args,))
    cons.start()
    server(args)
    exit_event.set()
    cons.join()
    logging.info('exiting flask-kafka-listener')


def get_arg(env, default):
    return os.getenv(env) if os.getenv(env, '') is not '' else default


def parse_args(parser):
    args = parser.parse_args()
    args.brokers = get_arg('KAFKA_BROKERS', args.brokers)
    args.topic = get_arg('KAFKA_TOPIC', args.topic)
    return args


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    logging.info('starting flask-kafka-python-listener')
    parser = argparse.ArgumentParser(
            description='listen for some stuff on kafka')
    parser.add_argument(
            '--brokers',
            help='The bootstrap servers, env variable KAFKA_BROKERS',
            default='localhost:9092')
    parser.add_argument(
            '--topic',
            help='Topic to publish to, env variable KAFKA_TOPIC',
            default='bones-brigade')
    args = parse_args(parser)
    main(args)
