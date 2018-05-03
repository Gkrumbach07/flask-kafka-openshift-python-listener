# flask-kafka-openshift-python-listener

This application is designed to listen for JSON formatted messages broadcast
on Apache Kafka. It also provides an HTTP server to access the last received
message.

It is designed for use with the [OpenShift](https://openshift.org)
[Python source-to-image](https://docs.openshift.org/latest/using_images/s2i_images/python.html)
workflow.

## Quickstart on OpenShift

```
oc new-app centos/python-36-centos7~https://github.com/bones-brigade/flask-kafka-openshift-python-listener.git \
  -e KAFKA_BROKERS=kafka:9092 \
  -e KAFKA_TOPIC=bones-brigade \
  --name=listener
```

You will need to adjust the `KAFKA_BROKERS` and `KAFKA_TOPICS` variables to
match your configured Kafka deployment and desired topic.

After launching the application, you will want to expose a route to its
service.

```
oc expose svc/listener
```

You can see the output from the HTTP server by using the following command:

```
curl http://`oc get routes/sparkpi --template='{{.spec.host}}'`/sparkpi?scale=5
```
