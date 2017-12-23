/*
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.zeebe.servicecontainer.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import java.util.concurrent.CompletableFuture;

import io.zeebe.servicecontainer.Service;
import io.zeebe.servicecontainer.ServiceName;
import io.zeebe.servicecontainer.ServiceStartContext;
import io.zeebe.servicecontainer.ServiceStopContext;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;

@SuppressWarnings({"unchecked"})
public class DependenciesTest
{
    ControlledServiceContainer serviceContainer;
    ServiceName<Object> service1;
    ServiceName<Object> service2;

    @Before
    public void setup()
    {
        serviceContainer = new ControlledServiceContainer();
        serviceContainer.start();
        service1 = ServiceName.newServiceName("service1", Object.class);
        service2 = ServiceName.newServiceName("service2", Object.class);
    }

    @Test
    public void shouldInstallServiceWithoutDependencies()
    {
        final Service<Object> mockService = mock(Service.class);

        final CompletableFuture<Void> serviceFuture = serviceContainer.createService(service1, mockService).install();

        serviceContainer.doWorkUntilDone();

        assertCompleted(serviceFuture);
        verify(mockService, times(1)).start(any(ServiceStartContext.class));
    }

    @Test
    public void shouldNotInstallServiceWithMissingDependencies()
    {
        final Service<Object> mockService = mock(Service.class);

        final CompletableFuture<Void> serviceFuture = serviceContainer.createService(service1, mockService)
            .dependency(service2)
            .install();

        serviceContainer.doWorkUntilDone();

        assertNotCompleted(serviceFuture);
        verify(mockService, times(0)).start(any(ServiceStartContext.class));
    }

    @Test
    public void shouldNotInstallServiceWithExistingName()
    {
        // given
        final Service<Object> mockService = mock(Service.class);
        serviceContainer.createService(service1, mockService)
            .dependency(service2)
            .install();
        serviceContainer.doWorkUntilDone();

        // when
        final CompletableFuture<Void> serviceFuture2 = serviceContainer.createService(service1, mockService)
            .dependency(service2)
            .install();
        serviceContainer.doWorkUntilDone();

        // then
        assertFailed(serviceFuture2);
    }

    @Test
    public void shouldInstallServiceWhenDependenciesAreAvailableAtStart()
    {
        final Service<Object> mockService1 = mock(Service.class);
        final Service<Object> mockService2 = mock(Service.class);

        final CompletableFuture<Void> service1Future = serviceContainer.createService(service1, mockService1)
            .install();

        serviceContainer.doWorkUntilDone();

        assertCompleted(service1Future);
        verify(mockService1, times(1)).start(any(ServiceStartContext.class));

        final CompletableFuture<Void> service2Future = serviceContainer.createService(service2, mockService2)
            .dependency(service1)
            .install();

        serviceContainer.doWorkUntilDone();

        assertCompleted(service2Future);
        verify(mockService2, times(1)).start(any(ServiceStartContext.class));
    }

    @Test
    public void shouldInstallServiceWhenDependenciesAreResolved()
    {
        final Service<Object> mockService1 = mock(Service.class);
        final Service<Object> mockService2 = mock(Service.class);

        serviceContainer.createService(service1, mockService1)
            .dependency(service2)
            .install();

        serviceContainer.doWorkUntilDone();

        verify(mockService1, times(0)).start(any(ServiceStartContext.class));

        serviceContainer.createService(service2, mockService2)
            .install();

        serviceContainer.doWorkUntilDone();

        final InOrder inOrder = inOrder(mockService1, mockService2);

        inOrder.verify(mockService2, times(1)).start(any(ServiceStartContext.class));
        inOrder.verify(mockService1, times(1)).start(any(ServiceStartContext.class));
    }

    @Test
    public void shouldRemoveServiceWithoutDependencies()
    {
        final Service<Object> mockService = mock(Service.class);

        // given
        serviceContainer.createService(service1, mockService).install();
        serviceContainer.doWorkUntilDone();

        // when
        final CompletableFuture<Void> removeFuture = serviceContainer.removeService(service1);
        serviceContainer.doWorkUntilDone();

        // then
        assertCompleted(removeFuture);
        verify(mockService, times(1)).stop(any(ServiceStopContext.class));
    }

    @Test
    public void shouldStopDependentServicesFirst()
    {
        final Service<Object> mockService1 = mock(Service.class);
        final Service<Object> mockService2 = mock(Service.class);

        // given
        serviceContainer.createService(service1, mockService1)
            .dependency(service2)
            .install();
        serviceContainer.createService(service2, mockService2)
            .install();
        serviceContainer.doWorkUntilDone();

        // when
        final CompletableFuture<Void> closeFuture = serviceContainer.removeService(service2);
        serviceContainer.doWorkUntilDone();

        // then
        assertCompleted(closeFuture);
        final InOrder inOrder = inOrder(mockService1, mockService2);
        inOrder.verify(mockService1, times(1)).stop(any(ServiceStopContext.class));
        inOrder.verify(mockService2, times(1)).stop(any(ServiceStopContext.class));
    }

    @Test
    public void shouldStopAllServicesWhenStoppingContainer()
    {
        final Service<Object> mockService1 = mock(Service.class);
        final Service<Object> mockService2 = mock(Service.class);

        // given
        serviceContainer.createService(service1, mockService1)
            .install();
        serviceContainer.createService(service2, mockService2)
            .dependency(service1)
            .install();
        serviceContainer.doWorkUntilDone();

        // when
        final CompletableFuture<Void> future = serviceContainer.closeAsync();
        serviceContainer.doWorkUntilDone();

        // then
        assertCompleted(future);
        final InOrder inOrder = inOrder(mockService1, mockService2);
        inOrder.verify(mockService2, times(1)).stop(any(ServiceStopContext.class));
        inOrder.verify(mockService1, times(1)).stop(any(ServiceStopContext.class));
    }

    protected void assertCompleted(CompletableFuture<Void> serviceFuture)
    {
        assertThat(serviceFuture).isCompleted();
    }

    protected void assertNotCompleted(CompletableFuture<Void> serviceFuture)
    {
        assertThat(serviceFuture).isNotCompleted();
    }

    protected void assertFailed(CompletableFuture<Void> serviceFuture)
    {
        assertThat(serviceFuture).hasFailed();
    }
}
