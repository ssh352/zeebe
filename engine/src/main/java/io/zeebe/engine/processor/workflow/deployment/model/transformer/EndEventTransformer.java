/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.engine.processor.workflow.deployment.model.transformer;

import io.zeebe.engine.processor.workflow.deployment.model.BpmnStep;
import io.zeebe.engine.processor.workflow.deployment.model.element.ExecutableEndEvent;
import io.zeebe.engine.processor.workflow.deployment.model.transformation.ModelElementTransformer;
import io.zeebe.engine.processor.workflow.deployment.model.transformation.TransformContext;
import io.zeebe.model.bpmn.instance.EndEvent;
import io.zeebe.model.bpmn.instance.ErrorEventDefinition;
import io.zeebe.protocol.record.intent.WorkflowInstanceIntent;

public final class EndEventTransformer implements ModelElementTransformer<EndEvent> {

  @Override
  public Class<EndEvent> getType() {
    return EndEvent.class;
  }

  @Override
  public void transform(final EndEvent element, final TransformContext context) {
    final var currentWorkflow = context.getCurrentWorkflow();
    final var endEvent = currentWorkflow.getElementById(element.getId(), ExecutableEndEvent.class);

    if (!element.getEventDefinitions().isEmpty()) {
      transformEventDefinition(element, context, endEvent);
    }

    endEvent.bindLifecycleState(
        WorkflowInstanceIntent.ELEMENT_ACTIVATING, BpmnStep.BPMN_ELEMENT_PROCESSOR);
    endEvent.bindLifecycleState(
        WorkflowInstanceIntent.ELEMENT_ACTIVATED, BpmnStep.BPMN_ELEMENT_PROCESSOR);
    endEvent.bindLifecycleState(
        WorkflowInstanceIntent.ELEMENT_COMPLETING, BpmnStep.BPMN_ELEMENT_PROCESSOR);
    endEvent.bindLifecycleState(
        WorkflowInstanceIntent.ELEMENT_COMPLETED, BpmnStep.BPMN_ELEMENT_PROCESSOR);
    endEvent.bindLifecycleState(
        WorkflowInstanceIntent.ELEMENT_TERMINATING, BpmnStep.BPMN_ELEMENT_PROCESSOR);
    endEvent.bindLifecycleState(
        WorkflowInstanceIntent.ELEMENT_TERMINATED, BpmnStep.BPMN_ELEMENT_PROCESSOR);
    endEvent.bindLifecycleState(
        WorkflowInstanceIntent.EVENT_OCCURRED, BpmnStep.BPMN_ELEMENT_PROCESSOR);
  }

  private void transformEventDefinition(
      final EndEvent element,
      final TransformContext context,
      final ExecutableEndEvent executableElement) {

    final var eventDefinition = element.getEventDefinitions().iterator().next();

    if (eventDefinition instanceof ErrorEventDefinition) {
      transformErrorEventDefinition(
          context, executableElement, (ErrorEventDefinition) eventDefinition);
    }
  }

  private void transformErrorEventDefinition(
      final TransformContext context,
      final ExecutableEndEvent executableElement,
      final ErrorEventDefinition errorEventDefinition) {

    final var error = errorEventDefinition.getError();
    final var executableError = context.getError(error.getId());
    executableElement.setError(executableError);
  }
}
