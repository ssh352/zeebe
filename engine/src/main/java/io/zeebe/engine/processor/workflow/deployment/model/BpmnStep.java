/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.engine.processor.workflow.deployment.model;

public enum BpmnStep {

  // new steps are just lifecycle state
  ELEMENT_ACTIVATING,
  ELEMENT_ACTIVATED,
  EVENT_OCCURRED,
  ELEMENT_COMPLETING,
  ELEMENT_COMPLETED,
  ELEMENT_TERMINATING,
  ELEMENT_TERMINATED,

  ACTIVITY_ELEMENT_ACTIVATING,
  ACTIVITY_ELEMENT_ACTIVATED,
  ACTIVITY_EVENT_OCCURRED,
  ACTIVITY_ELEMENT_COMPLETING,
  ACTIVITY_ELEMENT_TERMINATING,
  ACTIVITY_ELEMENT_TERMINATED,

  CONTAINER_ELEMENT_ACTIVATED,
  CONTAINER_ELEMENT_TERMINATING,

  FLOWOUT_ELEMENT_COMPLETED,

  // ---- delegate to the new BPMN lifecycle processor  ----
  BPMN_ELEMENT_PROCESSOR
}
