package korona.metrics.influxdb;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.utils.Time;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ClientTest {

    private Instant currentInstant = Instant.now();
    private Clock clock = Clock.fixed(currentInstant, ZoneId.of("UTC"));

    @Test
    @DisplayName("Test with empty Kafka config")
    void testEmptyConfig() {
        Map<String, String> conf = new HashMap<>();
        Client testClient = new Client(conf, clock);
        String expectedSrc = "{\"influxdb_metrics_source\": {\"type\": \"UNKNOWN\", \"id\": \"unknown\"}}";
        assertEquals(expectedSrc, testClient.influxDbClientSource());
        String expectedDest = "{\"influxdb_metrics_destination\": {\"influxdb_api_url\": \"http://localhost:8086/write?db=kafka_metrics&precision=ms\"}}";
        assertEquals(expectedDest, testClient.influxDbClientDestination());
        assertEquals(String.format("%s\n%s\n", expectedSrc, expectedDest), testClient.toString());
        int metricsPublishIntervalInSeconds = 10;
        assertEquals(metricsPublishIntervalInSeconds, testClient.metricsPublishIntervalInSeconds());
    }

    @Test
    @DisplayName("Test with broker.id in Kafka config")
    void testWithBrokerIdConfig() {
        Map<String, String> conf = new HashMap<>();
        conf.put("broker.id", "1");
        Client testClient = new Client(conf, clock);
        String expectedSrc = "{\"influxdb_metrics_source\": {\"type\": \"BROKER\", \"id\": \"1\"}}";
        assertEquals(expectedSrc, testClient.influxDbClientSource());
        String expectedDest = "{\"influxdb_metrics_destination\": {\"influxdb_api_url\": \"http://localhost:8086/write?db=kafka_metrics&precision=ms\"}}";
        assertEquals(expectedDest, testClient.influxDbClientDestination());
    }

    @Test
    @DisplayName("Test with producer client.id in Kafka config")
    void testWithProducerIdConfig() {
        Map<String, String> conf = new HashMap<>();
        conf.put("client.id", "sample-producer");
        Client testClient = new Client(conf, clock);
        String expectedSrc = "{\"influxdb_metrics_source\": {\"type\": \"PRODUCER\", \"id\": \"sample-producer\"}}";
        assertEquals(expectedSrc, testClient.influxDbClientSource());
        String expectedDest = "{\"influxdb_metrics_destination\": {\"influxdb_api_url\": \"http://localhost:8086/write?db=kafka_metrics&precision=ms\"}}";
        assertEquals(expectedDest, testClient.influxDbClientDestination());
    }

    @Test
    @DisplayName("Test with consumer group id and consumer instance in Kafka config")
    void testWithConsumerIdConfig() {
        Map<String, String> conf = new HashMap<>();
        conf.put("group.id", "sample-group");
        conf.put("group.instance.id", "instance-1");
        Client testClient = new Client(conf, clock);
        String expectedSrc = "{\"influxdb_metrics_source\": {\"type\": \"CONSUMER\", \"id\": {\"group\": \"sample-group\", \"id\": \"instance-1\"}}}";
        assertEquals(expectedSrc, testClient.influxDbClientSource());
        String expectedDest = "{\"influxdb_metrics_destination\": {\"influxdb_api_url\": \"http://localhost:8086/write?db=kafka_metrics&precision=ms\"}}";
        assertEquals(expectedDest, testClient.influxDbClientDestination());
    }

    @Test
    @DisplayName("Test with InfluxDB info in Kafka config")
    void testWithInfluxDbConfig() {
        Map<String, String> conf = new HashMap<>();
        conf.put("metrics.influxdb.host", "host");
        conf.put("metrics.influxdb.port", "1234");
        conf.put("metrics.influxdb.user", "admin");
        conf.put("metrics.influxdb.db", "mydb");
        conf.put("metrics.influxdb.password", "password");
        Client testClient = new Client(conf, clock);
        String expectedSrc = "{\"influxdb_metrics_source\": {\"type\": \"UNKNOWN\", \"id\": \"unknown\"}}";
        assertEquals(expectedSrc, testClient.influxDbClientSource());
        String expectedDest = "{\"influxdb_metrics_destination\": {\"influxdb_api_url\": \"http://host:1234/write?db=mydb&precision=ms\"}}";
        assertEquals(expectedDest, testClient.influxDbClientDestination());
    }

//    @Test
//    @DisplayName("Test generic zip/unzip methods")
//    void testZipUnzip() {
//        String inputStr = "This is a string";
//        String outputStr = new String(Client.unzip(Client.zip(inputStr.getBytes())));
//        assertTrue(inputStr.equals(outputStr));
//    }

    /**
     * Test getAllMetrics() without any metrics and after adding/removing several metrics, one at a time.
     */
    @Test
    @DisplayName("Test getAllMetrics() string with 0,1,2,3 metrics")
    void testGetAllMetrics() {
        Client testClient = new Client(new HashMap<>(), clock);
        assertEquals("", testClient.getAllMetrics()); // no metrics = empty string
        Map<String, KafkaMetric> metrics = generateSampleMetrics(3);
        List<String> metricStrings = new ArrayList<>();
        // Add metrics one at a time and test the metrics output after each addition
        metrics.forEach((k, v) -> {
                    metricStrings.add(k);
                    assertTrue(testClient.addMetric(v)); // ensure that the metric is actually added
                    StringBuilder sb = new StringBuilder();
                    Collections.sort(metricStrings);
                    metricStrings.forEach((s) -> {
                        sb.append(s);
                        sb.append("\n");
                    });
                    assertEquals(sb.toString(), testClient.getAllMetrics());
                }
        );
        // Now remove metrics one at a time and test the output after each removal
        metrics.forEach((k, v) -> {
                    metricStrings.remove(k);
                    testClient.removeMetric(v);
                    StringBuilder sb = new StringBuilder();
                    Collections.sort(metricStrings);
                    metricStrings.forEach((s) -> {
                        sb.append(s);
                        sb.append("\n");
                    });
                    assertEquals(sb.toString(), testClient.getAllMetrics());
                }
        );
    }

    @Test
    @DisplayName("Test publishMetrics() by sending metrics to InfluxDB (proxy)")
    void testPublishMetrics() throws IOException, InterruptedException {
        Executor executor = Executors.newSingleThreadExecutor();
        HttpServer server;
        server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
        int port = server.getAddress().getPort();
        Map<String, String> conf = new HashMap<>();
        conf.put("metrics.influxdb.host", "localhost");
        conf.put("metrics.influxdb.port", String.valueOf(port));

        Map<String, String> expectedHttpRequestHeaders = new HashMap<>();
        expectedHttpRequestHeaders.put("Content-type", "binary");
        expectedHttpRequestHeaders.put("Content-encoding", "gzip");

        Client testClient = new Client(conf, clock);
        server.setExecutor(executor);
        // No metrics
        server.createContext("/", httpExchange -> {
            processHttpExchange(httpExchange,
                    expectedHttpRequestHeaders,
                    "");
        });
        server.start();
        assertTrue(testClient.publishMetrics());

        // 3 metrics
        Map<String, KafkaMetric> metrics = generateSampleMetrics(3);
        List<String> metricStrings = new ArrayList<>();
        metrics.forEach((k, v) -> { metricStrings.add(k); testClient.addMetric(v); });
        Collections.sort(metricStrings);
        String result = metricStrings.get(0) + "\n" + metricStrings.get(1) + "\n" + metricStrings.get(2) + "\n";
        server.removeContext("/");
        server.createContext("/", httpExchange -> {
            processHttpExchange(httpExchange,
                    expectedHttpRequestHeaders,
                    result);
        });
        assertTrue(testClient.publishMetrics());
        server.stop(1);
    }

    /**
     * Given an integer N, creates a map of N KafkaMetrics and their InfluxDB line protocol string values.
     * Here's an example with 3 artificial metrics:
     * metric_1,metric_group=group_1,metricNameTagKey_1=metricNameTagValue_1,source=unknown,host=server value=1.000000 1587420886656
     * metric_2,metric_group=group_2,metricNameTagKey_1=metricNameTagValue_1,metricNameTagKey_2=metricNameTagValue_2,source=unknown,host=server value=2.000000 1587420886656
     * metric_3,metric_group=group_3,metricNameTagKey_1=metricNameTagValue_1,metricNameTagKey_2=metricNameTagValue_2,metricNameTagKey_3=metricNameTagValue_3,source=unknown,host=server value=3.000000 1587420886656
     *
     * @param count
     * @return
     */
    private Map<String, KafkaMetric> generateSampleMetrics(int count) {
        Map<String, KafkaMetric> metrics = new HashMap<>();
        for (int i = 1; i <= count; i++) {
            StringBuilder sb = new StringBuilder();
            sb = sb.append("metric_name_" + i); // metric_name_#
            sb = sb.append(",metric_group=group_" + i); // metric_group_#=group_#
            final double value = i;
            Measurable measurable = (MetricConfig mc, long now) -> value;
            MetricConfig metricConfig = new MetricConfig();
            Map<String, String> metricConfigTags = new HashMap<>();
            for (int j = 1; j <= i; j++) {
                metricConfigTags.put("metricConfigTagKey_" + String.valueOf(j), "metricConfigTagValue_" + String.valueOf(j));
            }
            metricConfig.tags(metricConfigTags);

            Map<String, String> metricNameTags = new HashMap<>();
            for (int k = 1; k <= i; k++) {
                metricNameTags.put("metricNameTagKey_" + String.valueOf(k), "metricNameTagValue_" + String.valueOf(k));
                sb.append(",metricNameTagKey_" + String.valueOf(k) + "=metricNameTagValue_" + String.valueOf(k));
            }
            MetricName metricName = new MetricName("metric_name_" + String.valueOf(i),
                    "group_" + String.valueOf(i),
                    "This is metric # " + String.valueOf(i),
                    metricNameTags);
            KafkaMetric kafkaMetric = new KafkaMetric(new Object(), metricName, measurable, metricConfig, Time.SYSTEM);
            try {
                sb.append(",source=unknown,host=" + InetAddress.getLocalHost().getHostName());
                sb.append(" value=" + i + ".00 " + clock.millis());
            } catch (Exception e) {

            }
            metrics.put(sb.toString(), kafkaMetric);
        }
        return metrics;
    }

    /**
     * HTTP request processor that simulates InfluxDB
     * @param httpExchange
     * @param expectedRequestedHeaders
     * @param expectedRequestBody
     * @throws IOException
     */
    private void processHttpExchange(HttpExchange httpExchange,
                                     Map<String, String> expectedRequestedHeaders,
                                     String expectedRequestBody) throws IOException {
        boolean pathOk = true, headersOk = true, bodyOk = true;
        Headers actualRequestHeaders = httpExchange.getRequestHeaders();
        if (!httpExchange.getRequestURI().getPath().equals("/write")) {
            pathOk = false;
        }

        for (Map.Entry<String, String> expected : expectedRequestedHeaders.entrySet()) {
            if (!actualRequestHeaders.containsKey(expected.getKey())) {
                headersOk = false;
            } else {
                String requestHeaderKey = expected.getKey();
                String requestHeaderValue = actualRequestHeaders.get(requestHeaderKey).get(0);
                if (!requestHeaderValue.equals(expected.getValue())) {
                    headersOk = false;
                }
            }
        }

        String actualRequestBody = new String(Client.unzip(httpExchange.getRequestBody().readAllBytes()));
        if (!expectedRequestBody.equals(actualRequestBody)) {
            bodyOk = false;
        }

        if (pathOk && headersOk && bodyOk) {
            httpExchange.sendResponseHeaders(204, -1);
            OutputStream outputStream = httpExchange.getResponseBody();
            outputStream.write("".getBytes()); // Need to send back empty body to InfluxDbClient on success
            outputStream.flush();
            outputStream.close();
        } else {
            // Print out all details - request path, headers and body
            // to help with debugging/troubleshooting
            System.out.println("***** FAILURE DETAILS ******");
            StringBuilder sb = new StringBuilder();
            sb.append(String.format("\nRequest path ok? %s\n", pathOk));
            sb.append(String.format("\nExpected Request Path:\n"));
            sb.append("/write\n");
            sb.append(String.format("\nActual Request Path:\n"));
            sb.append(String.format("%s\n", httpExchange.getRequestURI().getPath()));
            sb.append(String.format("\nRequest headers ok? %s\n", headersOk));
            sb.append(String.format("\nRequired Request Headers:\n"));
            expectedRequestedHeaders.forEach((key, value) -> sb.append(String.format("%s = %s\n", key, value)));
            sb.append(String.format("\nReceived Request Headers:\n"));
            actualRequestHeaders.forEach((key, value) -> sb.append(String.format("%s = %s\n", key, value)));
            sb.append(String.format("\nRequest body ok? %s\n", bodyOk));
            sb.append(String.format("\nExpected Request Body:\n"));
            sb.append(String.format("%s\n", expectedRequestBody));
            sb.append(String.format("\nActual Request Body:\n"));
            sb.append(String.format("%s\n", actualRequestBody));
            httpExchange.sendResponseHeaders(200, sb.length());
            OutputStream outputStream = httpExchange.getResponseBody();
            outputStream.write(sb.toString().getBytes());
            outputStream.flush();
            outputStream.close();
            System.out.println(sb.toString());
            assertTrue(false);
        }
    }
}
