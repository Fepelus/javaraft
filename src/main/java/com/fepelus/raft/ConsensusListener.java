package com.fepelus.raft;

import java.util.EventListener;

interface ConsensusListener extends EventListener {
    void onDelivery(DeliverToApplication command);
    void sendBroadcastRequest(SendBroadcastRequestCommand command);
}
