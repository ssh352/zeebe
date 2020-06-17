/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.broker.system.monitoring;

import static io.zeebe.broker.Broker.LOG;

import io.zeebe.broker.system.configuration.DataCfg;
import io.zeebe.util.sched.Actor;
import java.io.File;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.LongSupplier;

public class DiskSpaceUsageMonitor extends Actor {

  private final List<DiskSpaceUsageListener> diskSpaceUsageListeners = new ArrayList<>();
  private boolean currentDiskAvailableStatus = true;
  private LongSupplier diskSpaceSupplier;
  private final Duration monitoringDelay;
  private final long minFreeDiskRequired;

  public DiskSpaceUsageMonitor(final DataCfg dataCfg) {
    this.monitoringDelay = dataCfg.getDiskUsageCheckDelay();
    this.minFreeDiskRequired = dataCfg.getHighFreeDiskSpaceWatermarkInBytes();
    final var directory = new File(dataCfg.getDirectories().get(0));
    diskSpaceSupplier = directory::getUsableSpace;
  }

  @Override
  protected void onActorStarted() {
    actor.runAtFixedRate(monitoringDelay, this::checkDiskUsageAndNotifyListeners);
  }

  private void checkDiskUsageAndNotifyListeners() {
    final long diskSpaceUsage = diskSpaceSupplier.getAsLong();
    final boolean previousStatus = currentDiskAvailableStatus;
    currentDiskAvailableStatus = diskSpaceUsage >= minFreeDiskRequired;
    if (currentDiskAvailableStatus != previousStatus) {
      if (!currentDiskAvailableStatus) {
        LOG.debug(
            "Out of disk space. Current available {} bytes. Minimum needed {} bytes.",
            diskSpaceUsage,
            minFreeDiskRequired);
        diskSpaceUsageListeners.forEach(
            DiskSpaceUsageListener::onDiskSpaceUsageIncreasedAboveThreshold);
      } else {
        LOG.debug("Disk space available again. Current available {} bytes", diskSpaceUsage);
        diskSpaceUsageListeners.forEach(
            DiskSpaceUsageListener::onDiskSpaceUsageReducedBelowThreshold);
      }
    }
  }

  public void addDiskUsageListener(final DiskSpaceUsageListener listener) {
    diskSpaceUsageListeners.add(listener);
  }

  public void removeDiskUsageListener(final DiskSpaceUsageListener listener) {
    diskSpaceUsageListeners.remove(listener);
  }

  // Used only for testing
  public void setDiskSpaceSupplier(final LongSupplier diskSpaceSupplier) {
    this.diskSpaceSupplier = diskSpaceSupplier;
  }
}
