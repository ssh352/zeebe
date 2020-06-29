/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.broker.it.network;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.zeebe.broker.it.util.BrokerClassRuleHelper;
import io.zeebe.broker.it.util.GrpcClientRule;
import io.zeebe.broker.test.EmbeddedBrokerRule;
import io.zeebe.model.bpmn.Bpmn;
import io.zeebe.model.bpmn.BpmnModelInstance;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.springframework.util.unit.DataSize;

public class SmallMessageSizeTest {
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final DataSize MAX_MESSAGE_SIZE = DataSize.ofKilobytes(4);
  private static final String LARGE_TEXT = "x".repeat((int) (MAX_MESSAGE_SIZE.toBytes() / 4));

  private static final EmbeddedBrokerRule BROKER_RULE =
      new EmbeddedBrokerRule(b -> b.getNetwork().setMaxMessageSize(MAX_MESSAGE_SIZE));
  private static final GrpcClientRule CLIENT_RULE = new GrpcClientRule(BROKER_RULE);

  @ClassRule
  public static RuleChain ruleChain = RuleChain.outerRule(BROKER_RULE).around(CLIENT_RULE);

  @Rule public final BrokerClassRuleHelper helper = new BrokerClassRuleHelper();

  private String jobType;

  private static BpmnModelInstance workflow(final String jobType) {
    return Bpmn.createExecutableProcess("process")
        .startEvent()
        .serviceTask("task", t -> t.zeebeJobType(jobType))
        .endEvent()
        .done();
  }

  @Before
  public void init() {
    jobType = helper.getJobType();
  }

  @Test
  public void
      shouldIgnoreJobsThatAreTooLargeToFitIntoAMessageAndProceedWithJobsThatAreBehindInQueue() {
    // given (two workflows, the first has variables too big to fit into a message, the second fits
    // into a message)
    final var workflowKey = CLIENT_RULE.deployWorkflow(workflow(jobType));

    // workflow with variables that are greater than the message size
    final var workflowInstanceKey1 = CLIENT_RULE.createWorkflowInstance(workflowKey);

    for (int i = 0; i < 4; i++) {
      CLIENT_RULE
          .getClient()
          .newSetVariablesCommand(workflowInstanceKey1)
          .variables(Map.of(String.valueOf(i), LARGE_TEXT))
          .send()
          .join();
    }

    final var workflowInstanceKey2 = CLIENT_RULE.createWorkflowInstance(workflowKey);

    // when (we activate jobs)
    final var response =
        CLIENT_RULE
            .getClient()
            .newActivateJobsCommand()
            .jobType(jobType)
            .maxJobsToActivate(2)
            .send()
            .join(10, TimeUnit.SECONDS);

    // then (the job of the workflow which is too big for the message is ignored, but the other
    // workflow's job is activated)
    assertThat(response.getJobs()).hasSize(1);
    assertThat(response.getJobs().get(0).getWorkflowInstanceKey()).isEqualTo(workflowInstanceKey2);
  }
}
