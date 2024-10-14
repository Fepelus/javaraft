package com.fepelus.raft;

/*
 * Let's not message queue
 * Handle everything in line
 * Move on with our lives
 */
class Handler implements CommandHandler {

    Config config;
    Timer timer;
    OutboundTransport transport;
    ConsensusListener listener;

    Handler(
        Config config,
        Timer timer,
        OutboundTransport transport,
        ConsensusListener listener
    ) {
        this.config = config;
        this.timer = timer;
        this.transport = transport;
        this.listener = listener;
    }

    @Override
    public void handle(Command command) {
        Debug.log(config.thisNode(), String.format("Handling command %s", command.getClass().getSimpleName()));
        switch (command) {
            case SendVoteRequestCommand voteRequest -> this.transport.sendVoteRequest(
                    voteRequest
                );
            case SendVoteResponseCommand voteResponse -> this.transport.sendVoteResponse(
                    voteResponse
                );
            case SendLogRequestCommand logRequest -> this.transport.sendLogRequest(
                    logRequest
                );
            case StartElectionTimerCommand _x -> this.timer.startElectionTimer();
            case CancelElectionTimerCommand _x -> this.timer.cancelElectionTimer();
            case StartPingTimerCommand _x -> this.timer.startPingTimer();
            case CancelPingTimerCommand _x -> this.timer.cancelPingTimer();
            case SendLogResponseCommand logResponse -> this.transport.sendLogResponse(
                    logResponse
                );
            case SendBroadcastRequestCommand broadcastRequest -> this.listener.sendBroadcastRequest(
                    broadcastRequest
                );
            case DeliverToApplication delivery -> this.listener.onDelivery(
                    delivery
                );
        }
    }
}
