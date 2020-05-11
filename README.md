# Korona - Kafka Metrics Publisher Plugin

Apache Kafka has large number of built-in metrics that are available in the Kafka broker (cluster) as well as in the Java Producer, Consumer and Streaming client libraries. 
However the metrics are "trapped" inside JMX and one needs mechanisms to extract and publish them 
to a monitoring and alerting system. 

Korona however changes that, allowing one to publish metrics to InfluxDB. 
There are several monitoring and alerting systems out there and choosing 
a destination system for the metrics was a tough decision. 
However, InfluxDB was chosen based on:
- easy install
- speed and performance
- ability to emulate and thus be plug-in replacement for OpenTSDB, Graphite, StatsD
- the TICK ecosystem and Influx language
- ability to auto-purge old data
- integrated query, data export/import and aggregation
- simple line-protocol mechanism to push metrics

# How-To Use
## Requirements
The project was developed and compiled against Kafka versions 2.4.1 (March 12, 2020)
and 2.5.0 (April 15) and thus requires the following as enumerated in the
gradle build file:

- Kafka 2.2+
- Java 1.9+
- Gradle 5.4.1+

## Build And Deploy
Clone or download the project from Github and build the project:
```$xslt
git clone 
cd korona
git build
```
The above steps will create a jar `korona-0.1.0.jar` under the build directory.

Copy this jar in the lib directory for the Kafka broker, producer or client.

## Configuration Properties
InfluxDbMetricsReporter implements the Kafka MetricsReporter interface to send metrics 
from Kafka (broker, producer or consumer) to an InfluxDB instance. The reporter is configured 
by setting required properties (described below) in the appropriate Kafka configuration 
file (or properties in the code) - e.g. server.properties for broker.

Property | Description | Default Value | Applicable to (Broker, Producer, Consumer) |
---------|-------------|---------------|--------------------------------------------|
metrics.influxdb.host | Hostname or IP address of influxdb instance | localhost | All |
metrics.influxdb.port | Port on which influxdb instance listens for HTTP API requests | 8086 | All |
metrics.influxdb.db | Database name to store the metrics | kafka_metrics | All |
metrics_influxdb.user | Optional username to connect to influxdb | None | All |
metrics.influxdb.password | Optional password to connect to influxdb | None | All |
metrics.influxdb.kafkacluster | Optional name of Kafka cluster to which the broker belongs to or to which the producer or consumer connect to. It is highly recommended to provide this for grouping the metrics by the appropriate Kafka cluster. | None | All |
metrics.influxdb.publish.period | Interval period in seconds at which the metrics will be published to influxdb | 10 | All |
broker.id | Broker-id if the metrics are being emitted from a Kafka broker. | None | Broker |
client.id | Producer-id if metrics are being emitted from a Kafka producer | None | Producer | 
group.id | Consumer-group-id if metrics are being emitted from a Kafka consumer | None | Consumer |
group.instance.id | Consumer-group-instance-id if metrics are being emitted from a Kafka consumer in a consumer group| None | Consumer |
metric.reporters | Comma seperated list of classes/reporters to be used for processing metrics | None | korona.metrics.influxdb.Reporter |

## InfluxDB Setup
InfluxDB is currently available in two forms - InfluxDB 1.x (production stable) and InfluxDB 2.x (beta).
Version 1.x (e.g. 1.8) is a suite of 4 products - "TICK": 
Telegraf, InfluxDB (the actual timeseries datastore and a CLI client), Chronograf and Kapacitor.
Version 2.x combines all the products into a single binary. See [InfluxDB documentation](https://www.influxdata.com/products/influxdb-overview/) for details.
Korona has been tested with Influx 1.8

## Ready-Set-Go
That's it folks! You are ready to emit your metrics to InfluxDB by restarting the broker, producer or consumer.
