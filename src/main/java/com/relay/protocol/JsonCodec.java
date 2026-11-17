package com.relay.protocol;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Utility for serializing and deserializing JSON protocol frames.
 */
public final class JsonCodec {

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private JsonCodec() {}

    public static ObjectMapper mapper() {
        return MAPPER;
    }

    public static String serialize(Frame frame) throws JsonProcessingException {
        return MAPPER.writeValueAsString(frame);
    }

    public static Frame deserialize(String json) throws JsonProcessingException {
        return MAPPER.readValue(json, Frame.class);
    }
}
