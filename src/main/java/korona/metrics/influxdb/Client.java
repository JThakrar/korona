package korona.metrics.influxdb;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.ByteArrayOutputStream;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.UnknownHostException;
import java.net.URL;
import java.time.Clock;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.utils.LogContext;
import org.slf4j.Logger;

public class Client implements Runnable {
    private Clock clock;
    private String host;
    private int port;
    private String db;
    private String user;
    private String password;
    private String kafkaCluster;
    private String kafkaBrokerId;
    private String kafkaProducerId;
    private String kafkaConsumerGroupId;
    private String kafkaConsumerInstanceId;
    private String kafkaHostName;
    private int metricsPublishPeriodSeconds;
    private Source sourceType;
    private String metricsSource;
    private String metricsDestination;
    private URL influxDbApiUrl;
    private Map<String, KafkaMetric> metrics = new ConcurrentHashMap<>();
    private Logger logger;

    /**
     * Config is the set of of Kafka configuration properties that are used by the {@link Client}.
     * It specifies the property key and the default value if the property is absent in the
     * appropriate Kafka configuration file.
     */
    public enum Config {
        INFLUXDB_HOST("metrics.influxdb.host",
                "localhost",
                "Hostname or IP address of influxdb instance"),
        INFLUXDB_PORT("metrics.influxdb.port",
                "8086",
                "Port on which influxdb instance listens for HTTP API requests"),
        INFLUXDB_DB("metrics.influxdb.db",
                "kafka_metrics",
                "InfluxDB database name to store the metrics"),
        INFLUXDB_USER("metrics_influxdb.user",
                "",
                "Optional username to connect to influxdb"),
        INFLUXDB_PASSWORD("metrics.influxdb.password",
                "",
                "Optional password to connect to influxdb"),
        KAFKA_CLUSTER("metrics.influxdb.kafkacluster",
                "",
                "Name of Kafka cluster to which the broker belongs to, or to which the producer or consumer connect to"),
        KAFKA_BROKER_ID("broker.id",
                "",
                "Broker-id if this is a Kafka broker"),
        KAFKA_PRODUCER_ID("client.id",
                "",
                "Optional producer-id if this is a Kafka producer"),
        KAFKA_CONSUMER_GROUP_ID("group.id",
                "",
                "Optional consumer-group-id if this is a consumer"),
        KAFKA_CONSUMER_INSTANCE_ID("group.instance.id",
                "",
                "Optional consumer-group instance-id if this is a consumer"),
        METRICS_PUBLISH_PERIOD_SECONDS("metrics.influxdb.publish.period.seconds",
                "10",
                "Interval period in seconds at which the metrics will be published to influxdb");
        private String key;
        private String defaultValue;
        private String description;
        Config(String key, String defaultValue, String description) {
            this.key = key;
            this.defaultValue = defaultValue;
            this.description = description;
        }
        public String key() {
            return this.key;
        }
        public String defaultValue() {
            return this.defaultValue;
        }
        public String description() {
            return this.description;
        }
    }

    /**
     * This defines the source of metrics determined by looking for
     * configuration properties in the following order:
     * "broker.id" => BROKER
     * "client.id" => PRODUCER
     * "group.id" or "group.instance.id" => CONSUMER
     * default => UNKNOWN
     */
    public enum Source {
        UNKNOWN,
        BROKER,
        PRODUCER,
        CONSUMER
    }

    /**
     * This defines the connection and response or read timeout with InfluxDB in milliseconds.
     */
    public enum InfluxDB_Timeout {
        CONNECT(1000),
        RESPONSE(3000);
        private int timeoutMs;
        InfluxDB_Timeout(int timeoutMs) {
            this.timeoutMs = timeoutMs;
        }
        public int timeoutMs() {
            return this.timeoutMs;
        }
    }

    /**
     * {@link Client} constructor using a map of Kafka configuration properties and a UTC clock
     */
    public Client(Map<String, String> kafkaConfig, Clock clock) {
        // Using logging format as used in core Kafka
        LogContext logContext = new LogContext(String.format("[%s]: ", this.getClass().getSimpleName()));
        this.logger = logContext.logger(Reporter.class);
        this.clock = clock;

        this.host = kafkaConfig.getOrDefault(Config.INFLUXDB_HOST.key(),
                Config.INFLUXDB_HOST.defaultValue());
        this.port = Integer.parseInt(kafkaConfig.getOrDefault(Config.INFLUXDB_PORT.key(),
                Config.INFLUXDB_PORT.defaultValue()));
        this.db = kafkaConfig.getOrDefault(Config.INFLUXDB_DB.key(),
                Config.INFLUXDB_DB.defaultValue());
        this.user = kafkaConfig.getOrDefault(Config.INFLUXDB_USER.key(),
                Config.INFLUXDB_USER.defaultValue());
        this.password = kafkaConfig.getOrDefault(Config.INFLUXDB_PASSWORD.key(),
                Config.INFLUXDB_PASSWORD.defaultValue());
        this.kafkaCluster = kafkaConfig.getOrDefault(Config.KAFKA_CLUSTER.key(),
                Config.KAFKA_CLUSTER.defaultValue());
        this.kafkaBrokerId = kafkaConfig.getOrDefault(Config.KAFKA_BROKER_ID.key(),
                Config.KAFKA_BROKER_ID.defaultValue());
        this.kafkaProducerId = kafkaConfig.getOrDefault(Config.KAFKA_PRODUCER_ID.key(),
                Config.KAFKA_PRODUCER_ID.defaultValue());
        this.kafkaConsumerGroupId = kafkaConfig.getOrDefault(Config.KAFKA_CONSUMER_GROUP_ID.key(),
                Config.KAFKA_CONSUMER_GROUP_ID.defaultValue());
        this.kafkaConsumerInstanceId = kafkaConfig.getOrDefault(Config.KAFKA_CONSUMER_INSTANCE_ID.key(),
                Config.KAFKA_CONSUMER_INSTANCE_ID.defaultValue());
        this.metricsPublishPeriodSeconds = Integer.parseInt(kafkaConfig.getOrDefault(Config.METRICS_PUBLISH_PERIOD_SECONDS.key(),
                Config.METRICS_PUBLISH_PERIOD_SECONDS.defaultValue()));
        try {
            this.kafkaHostName = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            this.kafkaHostName = "unknown";
        }
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("http://%s:%d/write?db=%s&precision=ms", this.host, this.port, this.db));
        if (!this.user.isEmpty() && this.password.isEmpty()) {
            sb.append(String.format("&u=%s&p=%s", this.user, "*****"));
        }
        try {
            this.influxDbApiUrl = new URL(sb.toString());
        } catch (MalformedURLException e) {
            // do nothing, leave influxDbApiUrl empty
        }

        // Using simple string instead of JSON to KISS and keep external dependencies to an absolute minimum
        this.metricsDestination = String.format("{\"influxdb_api_url\": \"%s\"}", this.influxDbApiUrl);

        if (!this.kafkaBrokerId.isEmpty()) {
            this.sourceType = Source.BROKER;
            this.metricsSource = String.format("{\"type\": \"%s\", \"id\": \"%s\"}", sourceType.name(), kafkaBrokerId);
        } else if (!this.kafkaProducerId.isEmpty()) {
            this.sourceType = Source.PRODUCER;
            this.metricsSource = String.format("{\"type\": \"%s\", \"id\": \"%s\"}", sourceType.name(), kafkaProducerId);
        } else if (!this.kafkaConsumerGroupId.isEmpty() || !this.kafkaConsumerInstanceId.isEmpty()) {
            this.sourceType = Source.CONSUMER;
            this.metricsSource = String.format("{\"type\": \"%s\", \"id\": {\"group\": \"%s\", \"id\": \"%s\"}}",
                    sourceType.name(), kafkaConsumerGroupId, kafkaConsumerInstanceId);
        } else {
            this.sourceType = Source.UNKNOWN;
            this.metricsSource = String.format("{\"type\": \"%s\", \"id\": \"unknown\"}", sourceType.name());
        }
        logger.info(influxDbClientSource());
        logger.info(influxDbClientDestination());
    }

    /**
     * Returns Kafka metrics source configuration as appropriate for the source type
     */
    public String influxDbClientSource() {
        return String.format("{\"influxdb_metrics_source\": %s}", this.metricsSource);
    }

    /**
     * Returns InfluxDB API URL
     */
    public String influxDbClientDestination() {
        return String.format("{\"influxdb_metrics_destination\": %s}", this.metricsDestination);
    }

    /**
     * Returns the Kafka source and InfluxDB destination configuration information.
     */
    @Override
    public String toString() {
        return String.format("%s\n%s\n", influxDbClientSource(), influxDbClientDestination());
    }

    /**
     * Returns the interval period in seconds at which the metrics will be sent to InfluxDB
     */
    public int metricsPublishIntervalInSeconds() {
        return this.metricsPublishPeriodSeconds;
    }

    /**
     * Add a new metric
     * @return true if the metric did not exist, else false
     */
    public boolean addMetric(KafkaMetric metric) {
        String metricKey = kafkaMetricStaticString(metric);
        if (!metrics.containsKey(metricKey)) {
            metrics.put(metricKey, metric);
            return true;
        }
        return false;
    }

    /**
     * Remove a metric
     * @return true if the metric existed and was removed, else false
     */
    public boolean removeMetric(KafkaMetric metric) {
        String metricKey = kafkaMetricStaticString(metric);
        if (metrics.containsKey(metricKey)) {
            metrics.remove(metricKey);
            return true;
        }
        return false;
    }

    /**
     * Iterates through each metric and its value and creates a newline separated
     * list of sorted metric data points in InfluxDB line protocol format.
     * @see <a href="https://docs.influxdata.com/influxdb/v1.8/write_protocols/">InfluxDB Write Protocol Reference</a>
     * The metric values are internally a "double" and for string formatting purposes,
     * they are rounded off to 2 decimal places. While this may introduce
     * approximation/imprecision, don't see any risk/issues.
     */
    public String getAllMetrics() {
        StringBuilder sb = new StringBuilder();
        List<String> lines = new ArrayList<>();
        Long timeEpochMs = this.clock.millis();
        double metricValue;
        for (Map.Entry<String, KafkaMetric> entry : this.metrics.entrySet()) {
            try { // Note that currently only double-valued KafkaMetrics are supported
                metricValue = (double) entry.getValue().metricValue();
                if (Double.isNaN(metricValue)) {
                    metricValue = 0.00;
                }
            } catch (ClassCastException e) {
                metricValue = 0.00;
            }
            lines.add(String.format("%s%.2f %d\n", entry.getKey(), metricValue, timeEpochMs));
        }
        // Sorting the metrics makes it easy for InfluxDB
        // Aren't you happy String implements Comparable, making sort so easy?!
        Collections.sort(lines);
        lines.forEach(line -> sb.append(line));
        return sb.toString();
    }

    /**
     * Given a {@link org.apache.kafka.common.metrics.KafkaMetric},
     * returns a string representing the metric in InfluxDB line protocol without the actual value.
     * Appending "value=<value>", timestamp and a newline makes it suitable to send to InfluxDB.
     * Since this line is needed every time a metric is to be published to InfluxDB,
     * this static portion is pre-computed for efficiency reasons.
     * It also serves as a unique identifier for the metric.
     */
    private String kafkaMetricStaticString(KafkaMetric metric) {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("%s,metric_group=%s", metric.metricName().name(), metric.metricName().group()));
        for (Map.Entry<String, String> e : metric.metricName().tags().entrySet()) {
            sb.append(String.format(",%s=%s", e.getKey(), e.getValue()));
        }
        sb.append(String.format(",source=%s,%shost=%s", sourceType.name().toLowerCase(), this.kafkaCluster.isEmpty() ? "" : "cluster=" + this.kafkaCluster + ",", this.kafkaHostName));
        switch (sourceType) {
            case BROKER:
                sb.append(String.format(",broker_id=%s", this.kafkaBrokerId));
                break;
            case PRODUCER:
                sb.append(String.format(",producer_id=%s", this.kafkaProducerId));
                break;
            case CONSUMER:
                sb.append(String.format(",consumer_group_id=%s,consumer_instance_id=%s", this.kafkaConsumerGroupId, this.kafkaConsumerInstanceId));
                break;
            case UNKNOWN:
                break;
        }
        sb.append(String.format(" value="));
        return sb.toString();
    }

    /**
     * Send metrics to InfluxDB API URL
     */
    public boolean publishMetrics() throws IOException {
        String metrics = this.getAllMetrics();
        if (influxDbApiUrl == null) {
            return false;
        }
            HttpURLConnection conn = (HttpURLConnection) influxDbApiUrl.openConnection();
            conn.setDoOutput(true);
            conn.setConnectTimeout(InfluxDB_Timeout.CONNECT.timeoutMs());
            conn.setReadTimeout(InfluxDB_Timeout.RESPONSE.timeoutMs());
            conn.setRequestProperty("Content-type", "binary");
            conn.setRequestProperty("Content-encoding", "gzip");
            if (!user.isEmpty()) {
                conn.setRequestProperty("Authorization",
                        String.format("Token %s:%s", user, password));
            }
            conn.connect();
            OutputStream outputStream = conn.getOutputStream();
            outputStream.write(zip(metrics.getBytes()));
            outputStream.flush();
            int responseCode = conn.getResponseCode();
            String responseMsg = conn.getResponseMessage();
            if (responseCode != HttpURLConnection.HTTP_NO_CONTENT) { // response code should be 204
                logger.warn("InfluxDB metrics publish error.\n");
                logger.warn("Response code = %d\n\n", responseCode);
                logger.warn("Response message = %s\n\n", responseMsg);
                return false;
            }
            conn.disconnect();
            return true;
    }

    public static byte[] zip(byte[] metrics) {
        byte[] bytes;
        ByteArrayOutputStream baos;
        GZIPOutputStream gzipOut;
        try {
            baos = new ByteArrayOutputStream();
            gzipOut = new GZIPOutputStream(baos);
            gzipOut.write(metrics);
            gzipOut.flush();
            gzipOut.finish();
            gzipOut.close();
            bytes = baos.toByteArray();
            baos.close();
        } catch(Exception e) {
            bytes = metrics;
        }
        return bytes;
    }

    public static byte[] unzip(byte[] zipped) {
        byte[] bytes;
        try {
            ByteArrayInputStream bais = new ByteArrayInputStream(zipped);
            GZIPInputStream gunzipOut = new GZIPInputStream(bais);
            bytes = zipped;
            bytes = gunzipOut.readAllBytes();
            gunzipOut.close();
        } catch (Exception e) {
            bytes = zipped;
        }
        return bytes;
    }

    public void run() {
        try {
            publishMetrics();
        } catch (IOException e) {
            logger.error(String.format("%s: Error in publishing Kafka metrics to InfluxDB. %s\n%s\n",
                    this.getClass().getSimpleName(),
                    e.getMessage(),
                    e.getStackTrace()));
        }
    }
}
