package com.notnoop.apns.internal;

import com.notnoop.apns.ApnsNotification;
import com.notnoop.exceptions.NetworkIOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.concurrent.*;

/**
 * Customized version of apns pooled connection.
 *
 * @author Stanislau Mirzayeu
 */
public class AnyTalkApnsPooledConnection implements ApnsConnection {
    private static final Logger logger = LoggerFactory.getLogger(AnyTalkApnsPooledConnection.class);

    private final ApnsConnection prototype;
    private final int max;
    private final ExecutorService executors;
    private final ApnsConnection[] connections;

    public AnyTalkApnsPooledConnection(ApnsConnection prototype, int max) {
        this(prototype, max, Executors.newFixedThreadPool(max));
    }

    public AnyTalkApnsPooledConnection(ApnsConnection prototype, int max, ExecutorService executors) {
        assert max > 0;

        this.prototype = prototype;
        this.max = max;
        this.executors = executors;

        //create connections
        ApnsConnection[] connections = new ApnsConnection[max];
        for (int i = 0; i < connections.length; i++) {
            connections[i] = prototype.copy();
        }
        this.connections = connections;
    }

    @Override
    public void sendMessage(final ApnsNotification message) throws NetworkIOException {
        final ApnsConnection connection = getConnection(message.getDeviceToken());
        Future future = executors.submit(new Callable<Void>() {
            public Void call() throws Exception {
                connection.sendMessage(message);
                return null;
            }
        });
        try {
            future.get();
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException ee) {
            if (ee.getCause() instanceof NetworkIOException) {
                throw (NetworkIOException) ee.getCause();
            }
        }
    }

    @Override
    public ApnsConnection copy() {
        return new AnyTalkApnsPooledConnection(prototype, max);
    }

    @Override
    public void close() {
        executors.shutdown();
        try {
            executors.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            logger.warn("pool termination interrupted", e);
        }
        for (ApnsConnection connection : connections) {
            Utilities.close(connection);
        }
        Utilities.close(prototype);
    }

    @Override
    public void testConnection() {
        prototype.testConnection();
    }

    @Override
    public synchronized void setCacheLength(int cacheLength) {
        for (ApnsConnection connection : connections) {
            connection.setCacheLength(cacheLength);
        }
    }

    @Override
    public int getCacheLength() {
        return connections[0].getCacheLength();
    }

    private ApnsConnection getConnection(byte[] deviceToken) {
        return connections[(Arrays.hashCode(deviceToken) & 0x7fffffff) % connections.length];
    }
}
