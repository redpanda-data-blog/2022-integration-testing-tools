import com.github.dockerjava.api.command.InspectContainerResponse;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.rnorth.ducttape.unreliables.Unreliables;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

public class RedpandaTestcontainerTest {

    RedpandaContainer redpanda;

    @Before
    public void init() {
        redpanda = new RedpandaContainer("vectorized/redpanda:v22.1.3");
        redpanda.start();
    }

    @After
    public void tearDown() {
        redpanda.stop();
    }

    @Test
    public void testUsage() throws Exception {
        testRedpandaFunctionality(redpanda.getHost() + ":" + redpanda.getMappedPort(9092), 1, 1);
    }

    protected void testRedpandaFunctionality(String bootstrapServers, int partitions, int replicationFactor) throws Exception {
        try (
                AdminClient adminClient = AdminClient.create(
                        ImmutableMap.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
                );
                KafkaProducer<String, String> producer = new KafkaProducer<>(
                        ImmutableMap.of(
                                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                                bootstrapServers,
                                ProducerConfig.CLIENT_ID_CONFIG,
                                UUID.randomUUID().toString()
                        ),
                        new StringSerializer(),
                        new StringSerializer()
                );
                KafkaConsumer<String, String> consumer = new KafkaConsumer<>(
                        ImmutableMap.of(
                                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                                bootstrapServers,
                                ConsumerConfig.GROUP_ID_CONFIG,
                                "tc-" + UUID.randomUUID(),
                                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                                "earliest"
                        ),
                        new StringDeserializer(),
                        new StringDeserializer()
                );
        ) {
            String topicName = "test-" + UUID.randomUUID();

            Collection<NewTopic> topics = Collections.singletonList(new NewTopic(topicName, partitions, (short) replicationFactor));
            adminClient.createTopics(topics).all().get(50, TimeUnit.SECONDS);

            consumer.subscribe(Collections.singletonList(topicName));

            producer.send(new ProducerRecord<>(topicName, "testcontainers", "redpanda")).get();

            Unreliables.retryUntilTrue(
                    10,
                    TimeUnit.SECONDS,
                    () -> {
                        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                        if (records.isEmpty()) {
                            return false;
                        }
                        assertThat(records)
                                .hasSize(1)
                                .extracting(ConsumerRecord::topic, ConsumerRecord::key, ConsumerRecord::value)
                                .containsExactly(tuple(topicName, "testcontainers", "redpanda"));

                        return true;
                    }
            );
            consumer.unsubscribe();
        }
    }

    static class RedpandaContainer extends GenericContainer<RedpandaContainer> {

        private static final String STARTER_SCRIPT = "/testcontainers_start.sh";

        public RedpandaContainer(String image) {
            super(image);

            withExposedPorts(9092);
            withCreateContainerCmdModifier(cmd -> {
                cmd.withEntrypoint("sh");
            });

            withCommand("-c", "while [ ! -f " + STARTER_SCRIPT + " ]; do sleep 0.1; done; " + STARTER_SCRIPT);
            waitingFor(Wait.forLogMessage(".*Started Kafka API server.*", 1));
        }

        @Override
        protected void containerIsStarting(InspectContainerResponse containerInfo) {
            super.containerIsStarting(containerInfo);

            String command = "#!/bin/bash\n";

            command += "/usr/bin/rpk redpanda start --check=false --node-id 0 ";
            command += "--kafka-addr PLAINTEXT://0.0.0.0:29092,OUTSIDE://0.0.0.0:9092 ";
            command += "--advertise-kafka-addr PLAINTEXT://kafka:29092,OUTSIDE://" + getHost() + ":" + getMappedPort(9092);

            copyFileToContainer(
                    Transferable.of(command.getBytes(StandardCharsets.UTF_8), 0777),
                    STARTER_SCRIPT
            );
        }
    }
}