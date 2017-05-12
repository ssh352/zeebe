package org.camunda.tngp.transport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import java.time.Duration;
import java.util.Date;
import java.util.concurrent.CompletionException;

import org.camunda.tngp.test.util.TestUtil;
import org.camunda.tngp.transport.TransportBuilder.ThreadingMode;
import org.camunda.tngp.transport.impl.ClientChannelPoolImpl;
import org.camunda.tngp.util.time.ClockUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class ClientChannelPoolingTest
{

    @Rule
    public ExpectedException exception = ExpectedException.none();

    protected Transport clientTransport;
    protected Transport serverTransport;

    @Before
    public void setUp()
    {
        clientTransport = Transports.createTransport("client")
            .threadingMode(ThreadingMode.SHARED)
            .build();

        serverTransport = Transports.createTransport("server")
                .threadingMode(ThreadingMode.SHARED)
                .build();
    }

    @After
    public void tearDown()
    {
        ClockUtil.reset();
        clientTransport.close();
        serverTransport.close();
    }

    @Test
    public void shouldServeClientChannel()
    {
        // given
        final ClientChannelPool pool = clientTransport.createClientChannelPool().build();

        final SocketAddress addr = new SocketAddress("localhost", 51115);
        serverTransport.createServerSocketBinding(addr).bind();

        // when
        final ClientChannel channel = pool.requestChannel(addr);

        // then
        assertThat(channel.isOpen()).isTrue();
        assertThat(channel.getRemoteAddress()).isEqualTo(addr);
    }

    @Test
    public void shouldServeClientChannelAsync()
    {
        // given
        final ClientChannelPool pool = clientTransport.createClientChannelPool().build();

        final SocketAddress addr = new SocketAddress("localhost", 51115);
        serverTransport.createServerSocketBinding(addr).bind();

        // when
        final PooledFuture<ClientChannel> channelFuture = pool.requestChannelAsync(addr);

        // then
        final ClientChannel channel = TestUtil
                .doRepeatedly(() -> channelFuture.poll())
                .until(c -> c != null);
        assertThat(channelFuture.isFailed()).isFalse();
        assertThat(channel.isOpen()).isTrue();
        assertThat(channel.getRemoteAddress()).isEqualTo(addr);
    }

    @Test
    public void shouldReuseClientChannelsToSameRemoteAddress()
    {
        // given
        final ClientChannelPool pool = clientTransport.createClientChannelPool().build();

        final SocketAddress addr = new SocketAddress("localhost", 51115);
        serverTransport.createServerSocketBinding(addr).bind();

        final ClientChannel channel1 = pool.requestChannel(addr);

        // when
        final ClientChannel channel2 = pool.requestChannel(addr);

        // then
        assertThat(channel2).isSameAs(channel1);
    }

    @Test
    public void shouldNotReuseClientChannelsToDifferentRemoteAddress()
    {
        // given
        final ClientChannelPool pool = clientTransport.createClientChannelPool().build();

        final SocketAddress addr1 = new SocketAddress("localhost", 51115);
        final SocketAddress addr2 = new SocketAddress("localhost", 51116);
        serverTransport.createServerSocketBinding(addr1).bind();
        serverTransport.createServerSocketBinding(addr2).bind();

        final ClientChannel channel1 = pool.requestChannel(addr1);

        // when
        final ClientChannel channel2 = pool.requestChannel(addr2);

        // then
        assertThat(channel2).isNotSameAs(channel1);
        assertThat(channel2.getId()).isNotEqualTo(channel1.getId());
    }

    @Test
    public void shouldOpenNewChannelAfterChannelClose()
    {
        // given
        final ClientChannelPool pool = clientTransport.createClientChannelPool().build();

        final SocketAddress addr = new SocketAddress("localhost", 51115);
        serverTransport.createServerSocketBinding(addr).bind();

        final ClientChannel channel1 = pool.requestChannel(addr);
        channel1.close();

        // when
        final ClientChannel channel2 = pool.requestChannel(addr);

        // then
        assertThat(channel2).isNotSameAs(channel1);
        assertThat(channel2.getId()).isNotEqualTo(channel1.getId());
    }

    @Test
    public void shouldCloseChannelsOnPoolClose()
    {
        // given
        final ClientChannelPoolImpl pool = (ClientChannelPoolImpl) clientTransport.createClientChannelPool().build();

        final SocketAddress addr = new SocketAddress("localhost", 51115);
        serverTransport.createServerSocketBinding(addr).bind();

        final ClientChannel channel = pool.requestChannel(addr);

        // when
        pool.closeAsync().join();

        // then
        assertThat(channel.isClosed()).isTrue();
    }

    @Test
    public void shouldEvictUnusedChannelWhenCapacityIsReached()
    {
        // given
        ClockUtil.setCurrentTime(new Date().getTime());

        final int initialCapacity = 2;

        final ClientChannelPool pool = clientTransport
                .createClientChannelPool()
                .initialCapacity(initialCapacity)
                .build();

        bindServerSocketsInPortRange(51115, initialCapacity + 1);
        final ClientChannel[] channels = openChannelsInPortRange(pool, 51115, initialCapacity);

        pool.returnChannel(channels[1]);
        ClockUtil.addTime(Duration.ofHours(1));
        pool.returnChannel(channels[0]);

        // when
        final ClientChannel newChannel = pool.requestChannel(new SocketAddress("localhost", 51115 + initialCapacity));

        // then
        // there is no object reuse
        assertThat(channels).doesNotContain(newChannel);

        // the least recently returned channel has been closed asynchronously
        TestUtil.waitUntil(() -> channels[1].isClosed());
        assertThat(channels[1].isClosed()).isTrue();
        assertThat(channels[0].isOpen()).isTrue();
    }

    @Test
    public void shouldGrowPoolWhenCapacityIsExceeded()
    {
        final int initialCapacity = 2;

        final ClientChannelPool pool = clientTransport
                .createClientChannelPool()
                .initialCapacity(initialCapacity)
                .build();

        bindServerSocketsInPortRange(51115, initialCapacity + 1);

        // when
        final ClientChannel[] channels = openChannelsInPortRange(pool, 51115, initialCapacity + 1);

        // then all channels are open
        assertThat(channels).hasSize(initialCapacity + 1);
        for (ClientChannel channel : channels)
        {
            assertThat(channel.isOpen()).isTrue();
        }
    }

    @Test
    public void shouldFailToServeAsyncWhenChannelConenctFails()
    {
        // given
        final ClientChannelPool pool = clientTransport.createClientChannelPool().build();
        final SocketAddress addr = new SocketAddress("localhost", 51115);

        // when
        final PooledFuture<ClientChannel> future = pool.requestChannelAsync(addr);

        // then
        TestUtil.waitUntil(() -> future.isFailed());

        assertThat(future.isFailed()).isTrue();
        assertThat(future.poll()).isNull();
    }


    @Test
    public void shouldFailToServeWhenChannelConenctFails()
    {
        // given
        final ClientChannelPool pool = clientTransport.createClientChannelPool().build();
        final SocketAddress addr = new SocketAddress("localhost", 51115);

        // then
        exception.expect(CompletionException.class);

        // when
        pool.requestChannel(addr);
    }

    @Test
    public void shouldAllowNullValuesOnReturnChannel()
    {
        // given
        final ClientChannelPool pool = clientTransport.createClientChannelPool().build();

        // when
        try
        {
            pool.returnChannel(null);
            // then there is no exception
        }
        catch (Exception e)
        {
            fail("should not throw exception");
        }
    }

    @Test
    public void shouldResetFailedConnectFuturesOnRelease()
    {
        // given
        final ClientChannelPool pool = clientTransport
                .createClientChannelPool()
                .build();

        final SocketAddress addr = new SocketAddress("localhost", 51115);
        final PooledFuture<ClientChannel> future = pool.requestChannelAsync(addr);

        TestUtil.waitUntil(() -> future.isFailed());

        // when
        future.release();

        // then
        assertThat(future.poll()).isNull();
        assertThat(future.isFailed()).isFalse();
    }

    @Test
    public void shouldResetSuccessfulConnectFuturesOnRelease()
    {
        // given
        final ClientChannelPool pool = clientTransport
                .createClientChannelPool()
                .build();

        final SocketAddress addr = new SocketAddress("localhost", 51115);
        serverTransport.createServerSocketBinding(addr).bind();

        final PooledFuture<ClientChannel> future = pool.requestChannelAsync(addr);
        TestUtil.waitUntil(() -> future.poll() != null);

        // when
        future.release();

        // then
        assertThat(future.poll()).isNull();
        assertThat(future.isFailed()).isFalse();
    }

    @Test
    public void shouldFailConcurrentRequestsForSameRemoteAddress()
    {
        final ClientChannelPool pool = clientTransport
                .createClientChannelPool()
                .build();

        final SocketAddress addr = new SocketAddress("localhost", 51115);

        // when
        final PooledFuture<ClientChannel> future1 = pool.requestChannelAsync(addr);
        final PooledFuture<ClientChannel> future2 = pool.requestChannelAsync(addr);
        TestUtil.waitUntil(() -> future1.isFailed() && future2.isFailed());

        // then
        assertThat(future1.isFailed()).isTrue();
        assertThat(future2.isFailed()).isTrue();
    }

    protected ClientChannel[] openChannelsInPortRange(ClientChannelPool pool, int firstPort, int range)
    {
        final ClientChannel[] channels = new ClientChannel[range];
        for (int i = 0; i < range; i++)
        {
            channels[i] = pool.requestChannel(new SocketAddress("localhost", firstPort + i));
        }
        return channels;
    }

    protected void bindServerSocketsInPortRange(int firstPort, int range)
    {
        for (int i = 0; i < range + 1; i++)
        {
            serverTransport.createServerSocketBinding(new SocketAddress("localhost", firstPort + i)).bind();
        }
    }
}
