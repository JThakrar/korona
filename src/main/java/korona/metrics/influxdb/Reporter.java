package korona.metrics.influxdb;

import java.time.Clock;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.utils.LogContext;
import org.slf4j.Logger;

/**
 * Reporter implements the Kafka MetricsReporter interface
 * to send metrics from a Kafka broker, producer or consumer to an InfluxDB instance.
 */
public class Reporter implements MetricsReporter {
    private Client client;
    private LogContext logContext;
    private Logger logger;

    @Override
    public void init(List<KafkaMetric> metrics) {
        metrics.forEach(m -> client.addMetric(m));
    }

    @Override
    public void metricChange(KafkaMetric metric) {
        client.removeMetric(metric);
        client.addMetric(metric);
    }

    @Override
    public void metricRemoval(KafkaMetric metric) {
        client.removeMetric(metric);
    }

    @Override
    public void close() {
        // No action needed
    }

    /**
     * Setup the InfluxDbMetricsReporter.
     * This is the first thing to be called after instance creation.
     * It sets up the InfluxDbClient and configures it to be run at
     * periodic intervals as specified in the configuration property
     * "metrics.influxdb.publish.period".
     */
    @Override
    public void configure(Map<String, ?> configs) {
        ScheduledExecutorService scheduler;
        // Using logging format as used in core Kafka
        logContext = new LogContext(String.format("[%s]: ", this.getClass().getSimpleName()));
        logger = logContext.logger(Reporter.class);
        Map<String, String> kafkaConf = new HashMap<>();
        for (Map.Entry<String, ?> e : configs.entrySet()) {
            kafkaConf.put(e.getKey(), e.getValue().toString());
        }
        Clock clock = Clock.systemUTC();
        client = new Client(kafkaConf, clock);
        scheduler = Executors.newSingleThreadScheduledExecutor();
        int metricsPublishIntervalInSeconds = client.metricsPublishIntervalInSeconds();
        scheduler.scheduleAtFixedRate(client,
                metricsPublishIntervalInSeconds,
                metricsPublishIntervalInSeconds,
                TimeUnit.SECONDS);
        logger.info("Initialization complete");
    }
}
