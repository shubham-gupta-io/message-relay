package com.relay.mailbox;

public interface DeliveryChannel {
    /**
     * Delivers a message to the active client.
     *
     * @param message the message to deliver.
     * @return true if successfully written to the transport, false on I/O error.
     */
    boolean deliver(MessageRecord message);

    /**
     * Checks if the channel is open.
     */
    boolean isOpen();

    /**
     * Closes the channel quietly without throwing checked exceptions.
     */
    void closeQuietly();
}
