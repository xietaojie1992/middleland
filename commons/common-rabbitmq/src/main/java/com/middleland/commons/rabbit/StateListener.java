package com.middleland.commons.rabbit;

import com.rabbitmq.client.BlockedListener;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Recoverable;
import com.rabbitmq.client.RecoveryListener;
import com.rabbitmq.client.ShutdownListener;
import com.rabbitmq.client.ShutdownSignalException;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;

/**
 * @author xietaojie
 */
@Slf4j
public class StateListener implements ShutdownListener, RecoveryListener, BlockedListener {

    private final String mark;

    public StateListener(String mark) {
        this.mark = mark;
    }

    public String getMark() {
        return mark;
    }

    public void onChannelRecovery() {
    }

    public void onChannelRecoveryStarted() {
    }

    public void onChannelShutdownByBroker() {
    }

    public void onChannelShutdownByApplication() {
    }

    public void onConnectionRecovery() {
    }

    public void onConnectionRecoveryStarted() {
    }

    public void onConnectionShutdownByBroker() {
    }

    public void onConnectionShutdownByApplication() {
    }

    public void onConnectionBlocked() {
    }

    public void onConnectionUnblocked() {
    }

    @Override
    public void shutdownCompleted(ShutdownSignalException cause) {
        String hardError = cause.isHardError() ? "connection" : "channel";
        String criminal = cause.isInitiatedByApplication() ? "application" : "broker";
        log.warn("{} [{}] shutdown completed cause by {}, {}, {}", hardError, mark, criminal, cause.getReason(), cause.getMessage());
        if (cause.isHardError()) {
            if (cause.isInitiatedByApplication()) {
                onConnectionShutdownByApplication();
            } else {
                onConnectionShutdownByBroker();
            }
        } else {
            if (cause.isInitiatedByApplication()) {
                onChannelShutdownByApplication();
            } else {
                onChannelShutdownByBroker();
            }
        }
    }

    @Override
    public void handleRecovery(Recoverable recoverable) {
        if (recoverable instanceof Connection) {
            Connection recoverConnection = (Connection) recoverable;
            log.info("connection recovered. Host={}, ConnectionName={}", recoverConnection.getAddress().getHostAddress(),
                    recoverConnection.getClientProvidedName());
            onConnectionRecovery();
        } else if (recoverable instanceof Channel) {
            Channel recoverChannel = (Channel) recoverable;
            log.info("channel recovered. Host={}, ConnectionName={},  ChannelNo={}",
                    recoverChannel.getConnection().getAddress().getHostAddress(), recoverChannel.getConnection().getClientProvidedName(),
                    recoverChannel.getChannelNumber());
            onChannelRecovery();
        }
    }

    @Override
    public void handleRecoveryStarted(Recoverable recoverable) {
        if (recoverable instanceof Connection) {
            Connection recoverConnection = (Connection) recoverable;
            log.info("connection recovery started. Host={}, ConnectionName={}", recoverConnection.getAddress().getHostAddress(),
                    recoverConnection.getClientProvidedName());
            onConnectionRecoveryStarted();
        } else if (recoverable instanceof Channel) {
            Channel recoverChannel = (Channel) recoverable;
            log.info("channel recovery started. Host={}, ConnectionName={},  ChannelNo={}",
                    recoverChannel.getConnection().getAddress().getHostAddress(), recoverChannel.getConnection().getClientProvidedName(),
                    recoverChannel.getChannelNumber());
            onChannelRecoveryStarted();
        }
    }

    @Override
    public void handleBlocked(String reason) throws IOException {
        log.warn("Connection [name={}] is blocked due to [{}]", mark, reason);
        onConnectionBlocked();
    }

    @Override
    public void handleUnblocked() throws IOException {
        log.info("Connection [name={}] is unblocked.", mark);
        onConnectionUnblocked();
    }
}
