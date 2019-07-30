import argparse
import copy
import logging
import os
import threading

import flask
from flask import views
from flask import json
import kafka


access_lock = threading.Lock()
exit_event = threading.Event()
_last_data = {}


def last_data(update=None):
    access_lock.acquire()
    if update is not None:
        global _last_data
        _last_data = copy.deepcopy(update)
    retval = copy.deepcopy(_last_data)
    access_lock.release()
    return retval


class RootView(views.MethodView):
    def get(self):
        return json.jsonify(last_data())


def server(args):
    logging.info('starting flask server')
    # create the flask app object
    app = flask.Flask(__name__)
    # change this value for production environments
    app.config['SECRET_KEY'] = 'secret!'

    app.add_url_rule('/', view_func=RootView.as_view('index'))

    app.run(host='0.0.0.0', port=8080)
    logging.info('exiting flask server')


def consumer(args):
    logging.info('starting kafka consumer')
    consumer = kafka.KafkaConsumer(args.topic, bootstrap_servers=args.brokers)
    for msg in consumer:
        if exit_event.is_set():
            break
        try:
            last_data(json.loads(str(msg.value, 'utf-8')))
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
