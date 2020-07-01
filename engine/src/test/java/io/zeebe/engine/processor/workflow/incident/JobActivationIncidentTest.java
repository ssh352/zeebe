/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.engine.processor.workflow.incident;

import static io.zeebe.protocol.record.Assertions.assertThat;

import io.zeebe.engine.util.EngineRule;
import io.zeebe.model.bpmn.Bpmn;
import io.zeebe.model.bpmn.BpmnModelInstance;
import io.zeebe.protocol.record.intent.IncidentIntent;
import io.zeebe.protocol.record.value.ErrorType;
import io.zeebe.test.util.record.RecordingExporter;
import io.zeebe.test.util.record.RecordingExporterTestWatcher;
import java.util.Map;
import org.assertj.core.api.Assertions;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

public final class JobActivationIncidentTest {
  @ClassRule public static final EngineRule ENGINE = EngineRule.singlePartition();
  private static final int MAX_MESSAGE_SIZE =
      1024 * 1024 * 4; // copy and paste from LogStreamBuilderImpl
  private static final String LARGE_TEXT = "x".repeat((int) (MAX_MESSAGE_SIZE / 4));
  private static final String JOB_TYPE = "test";
  private static final BpmnModelInstance WORKFLOW =
      Bpmn.createExecutableProcess("process")
          .startEvent()
          .serviceTask("task", t -> t.zeebeJobType(JOB_TYPE))
          .endEvent()
          .done();

  private static long workflowKey;

  @Rule
  public RecordingExporterTestWatcher recordingExporterTestWatcher =
      new RecordingExporterTestWatcher();

  @BeforeClass
  public static void init() {
    workflowKey =
        ENGINE
            .deployment()
            .withXmlResource(WORKFLOW)
            .deploy()
            .getValue()
            .getDeployedWorkflows()
            .get(0)
            .getWorkflowKey();
  }

  @Test
  public void shouldRaiseIncidentWhenActivatingJobThatIsTooBigForMessageSize() {
    // given
    final var workflowInstanceKey = ENGINE.workflowInstance().ofBpmnProcessId("process").create();

    for (int i = 0; i < 4; i++) {
      ENGINE
          .variables()
          .ofScope(workflowInstanceKey)
          .withDocument(Map.of(String.valueOf(i), LARGE_TEXT))
          .update();
    }

    // when
    final var activationResult =
        ENGINE.jobs().withMaxJobsToActivate(1).withType(JOB_TYPE).byWorker("dummy").activate();

    // then
    Assertions.assertThat(activationResult.getValue().getJobs()).isEmpty();
    Assertions.assertThat(activationResult.getValue().isTruncated()).isTrue();

    final var incidentCommand =
        RecordingExporter.incidentRecords()
            .withIntent(IncidentIntent.CREATE)
            .withWorkflowInstanceKey(workflowInstanceKey)
            .getFirst();

    assertThat(incidentCommand.getValue())
        .hasErrorType(ErrorType.IO_MAPPING_ERROR)
        .hasBpmnProcessId("process")
        .hasWorkflowKey(workflowKey)
        .hasWorkflowInstanceKey(workflowInstanceKey)
        .hasElementId("task");
  }

  @Test
  public void shouldMakeJobActivatableAfterIncidentIsResolved() {
    // given
    final var workflowInstanceKey = ENGINE.workflowInstance().ofBpmnProcessId("process").create();

    for (int i = 0; i < 4; i++) {
      ENGINE
          .variables()
          .ofScope(workflowInstanceKey)
          .withDocument(Map.of(String.valueOf(i), LARGE_TEXT))
          .update();
    }
    ENGINE.jobs().withMaxJobsToActivate(1).withType(JOB_TYPE).byWorker("dummy").activate();

    // when
    final var incidentCommand =
        RecordingExporter.incidentRecords()
            .withIntent(IncidentIntent.CREATE)
            .withWorkflowInstanceKey(workflowInstanceKey)
            .getFirst();

    ENGINE
        .variables()
        .ofScope(workflowInstanceKey)
        .withDocument(
            Map.of("0", "lorem ipsum", "1", "lorem ipsum", "2", "lorem ipsum", "3", "lorem ipsum"))
        .update();

    ENGINE.incident().ofInstance(workflowInstanceKey).withKey(incidentCommand.getKey()).resolve();

    final var activationResult =
        ENGINE.jobs().withMaxJobsToActivate(1).withType(JOB_TYPE).byWorker("dummy").activate();

    // then
    Assertions.assertThat(activationResult.getValue().getJobs()).hasSize(1);
    Assertions.assertThat(activationResult.getValue().isTruncated()).isFalse();
  }
}
