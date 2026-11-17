package com.relay.protocol;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * Protocol frames exchanged over TCP as newline-delimited JSON.
 */
@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = "type",
        visible = true
)
@JsonSubTypes({
        @JsonSubTypes.Type(value = Frame.Register.class, name = "REGISTER"),
        @JsonSubTypes.Type(value = Frame.Registered.class, name = "REGISTERED"),
        @JsonSubTypes.Type(value = Frame.Send.class, name = "SEND"),
        @JsonSubTypes.Type(value = Frame.SendAck.class, name = "SEND_ACK"),
        @JsonSubTypes.Type(value = Frame.Deliver.class, name = "DELIVER"),
        @JsonSubTypes.Type(value = Frame.Ack.class, name = "ACK"),
        @JsonSubTypes.Type(value = Frame.Error.class, name = "ERROR")
})
public sealed interface Frame permits
        Frame.Register,
        Frame.Registered,
        Frame.Send,
        Frame.SendAck,
        Frame.Deliver,
        Frame.Ack,
        Frame.Error {

    String type();

    @JsonIgnoreProperties(ignoreUnknown = true)
    record Register(@JsonProperty("clientId") String clientId) implements Frame {
        @Override public String type() { return "REGISTER"; }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    record Registered(@JsonProperty("clientId") String clientId) implements Frame {
        @Override public String type() { return "REGISTERED"; }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    record Send(
            @JsonProperty("msgId") String msgId,
            @JsonProperty("recipientId") String recipientId,
            @JsonProperty("payload") String payload
    ) implements Frame {
        @Override public String type() { return "SEND"; }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    record SendAck(
            @JsonProperty("msgId") String msgId,
            @JsonProperty("status") String status,
            @JsonProperty("reason") String reason
    ) implements Frame {
        public static final String ACCEPTED = "ACCEPTED";
        public static final String REJECTED = "REJECTED";

        public static SendAck accepted(String msgId) {
            return new SendAck(msgId, ACCEPTED, null);
        }

        public static SendAck rejected(String msgId, String reason) {
            return new SendAck(msgId, REJECTED, reason);
        }

        @Override public String type() { return "SEND_ACK"; }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    record Deliver(
            @JsonProperty("msgId") String msgId,
            @JsonProperty("senderId") String senderId,
            @JsonProperty("payload") String payload
    ) implements Frame {
        @Override public String type() { return "DELIVER"; }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    record Ack(@JsonProperty("msgId") String msgId) implements Frame {
        @Override public String type() { return "ACK"; }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    record Error(
            @JsonProperty("code") String code,
            @JsonProperty("message") String message
    ) implements Frame {
        @Override public String type() { return "ERROR"; }
    }
}
