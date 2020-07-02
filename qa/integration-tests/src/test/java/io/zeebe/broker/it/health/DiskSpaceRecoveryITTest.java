/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.broker.it.health;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import com.github.dockerjava.api.model.Bind;
import com.github.dockerjava.api.model.Volume;
import io.zeebe.client.ZeebeClient;
import io.zeebe.containers.ZeebeBrokerContainer;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.elasticsearch.client.RestClient;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Network;
import org.testcontainers.elasticsearch.ElasticsearchContainer;

public class DiskSpaceRecoveryITTest {
  static ZeebeBrokerContainer zeebeBroker;
  private static final Logger LOG = LoggerFactory.getLogger("TEST");
  private static final String VOLUME_NAME = "data-DiskSpaceRecoveryITTest";
  private static String containerIPAddress;
  private static Integer apiPort;
  private static ElasticsearchContainer elastic;
  private ZeebeClient client;

  @BeforeClass
  public static void setUpClass() {
    final var elasticHostInternal = "http://elastic:9200";
    zeebeBroker = createZeebe(elasticHostInternal);
    final var network = zeebeBroker.getNetwork();
    elastic = createElastic(network);
    zeebeBroker.start();

    apiPort = zeebeBroker.getMappedPort(26500);
    containerIPAddress = zeebeBroker.getContainerIpAddress();
  }

  private static ZeebeBrokerContainer createZeebe(final String elasticHost) {
    zeebeBroker =
        new ZeebeBrokerContainer("current-test")
            .withEnv(
                "ZEEBE_BROKER_EXPORTERS_ELASTICSEARCH_CLASSNAME",
                "io.zeebe.exporter.ElasticsearchExporter")
            .withEnv("ZEEBE_BROKER_EXPORTERS_ELASTICSEARCH_ARGS_URL", elasticHost)
            .withEnv("ZEEBE_BROKER_EXPORTERS_ELASTICSEARCH_ARGS_BULK_DELAY", "1")
            .withEnv("ZEEBE_BROKER_EXPORTERS_ELASTICSEARCH_ARGS_BULK_SIZE", "1")
            .withEnv("ZEEBE_BROKER_DATA_SNAPSHOTPERIOD", "1m")
            .withEnv("ZEEBE_BROKER_DATA_LOGSEGMENTSIZE", "4MB")
            .withEnv("ZEEBE_BROKER_DATA_DISKUSAGECOMMANDWATERMARK", "0.5")
            .withEnv("ZEEBE_BROKER_DATA_LOGINDEXDENSITY", "1")
            .withEnv("ZEEBE_BROKER_EXPORTERS_ELASTICSEARCH_ARGS_INDEX_MESSAGE", "true");

    zeebeBroker
        .getDockerClient()
        .createVolumeCmd()
        .withDriver("local")
        .withDriverOpts(Map.of("type", "tmpfs", "device", "tmpfs", "o", "size=32m"))
        .withName(VOLUME_NAME)
        .exec();
    final Volume newVolume = new Volume("/usr/local/zeebe/data");
    zeebeBroker.withCreateContainerCmdModifier(
        cmd ->
            cmd.withHostConfig(cmd.getHostConfig().withBinds(new Bind(VOLUME_NAME, newVolume)))
                .withName("zeebe-test"));

    return zeebeBroker;
  }

  private static ElasticsearchContainer createElastic(final Network network) {
    final ElasticsearchContainer container =
        new ElasticsearchContainer(
            "docker.elastic.co/elasticsearch/elasticsearch:"
                + RestClient.class.getPackage().getImplementationVersion());

    container.withNetwork(network).withEnv("discovery.type", "single-node");
    container.withCreateContainerCmdModifier(cmd -> cmd.withName("elastic"));
    return container;
  }

  @AfterClass
  public static void tearDownClass() {
    LoggerFactory.getLogger("Test").info(zeebeBroker.getLogs());
    zeebeBroker.stop();
    zeebeBroker.getDockerClient().removeVolumeCmd(VOLUME_NAME).exec();
    elastic.stop();
  }

  @Test
  public void shouldRecoverAfterOutOfDiskSpaceWhenExporterStarts() {
    // given
    client =
        ZeebeClient.newClientBuilder()
            .brokerContactPoint(containerIPAddress + ":" + apiPort)
            .usePlaintext()
            .build();

    // when
    LOG.info("Wait until broker is out of disk space");
    await()
        .timeout(Duration.ofMinutes(3))
        .pollInterval(1, TimeUnit.MICROSECONDS)
        .untilAsserted(
            () ->
                assertThatThrownBy(this::publishMessage)
                    .hasRootCauseMessage(
                        "RESOURCE_EXHAUSTED: Cannot accept requests for partition 1. Broker is out of disk space"));

    elastic.start();

    // then
    await()
        .pollInterval(Duration.ofSeconds(10))
        .timeout(Duration.ofMinutes(2))
        .untilAsserted(() -> assertThatCode(this::publishMessage).doesNotThrowAnyException());
  }

  private void publishMessage() {
    client
        .newPublishMessageCommand()
        .messageName("test")
        .correlationKey(String.valueOf(1))
        .send()
        .join();
  }
}
