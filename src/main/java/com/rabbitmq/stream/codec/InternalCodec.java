// Copyright (c) 2020-2026 Broadcom. All Rights Reserved.
// The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.
//
// This software, the RabbitMQ Stream Java client library, is dual-licensed under the
// Mozilla Public License 2.0 ("MPL"), and the Apache License version 2 ("ASL").
// For the MPL, please see LICENSE-MPL-RabbitMQ. For the ASL,
// please see LICENSE-APACHE2.
//
// This software is distributed on an "AS IS" basis, WITHOUT WARRANTY OF ANY KIND,
// either express or implied. See the LICENSE file for specific language governing
// rights and limitations of this software.
//
// If you have any questions regarding licensing, please contact us at
// info@rabbitmq.com.
package com.rabbitmq.stream.codec;

import com.rabbitmq.stream.Codec;
import com.rabbitmq.stream.Message;
import com.rabbitmq.stream.MessageBuilder;
import com.rabbitmq.stream.Properties;
import io.netty.buffer.ByteBuf;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Zero-dependency {@link Codec} implementation that encodes and decodes messages in AMQP 1.0 format
 * using the library's own {@link Amqp10} primitives, with no external AMQP library (e.g. QPid
 * Proton) required.
 *
 * <h3>Encoding</h3>
 *
 * <p>{@link #encode(Message)} returns a {@link StreamingEncodedMessage} that defers AMQP 1.0
 * serialization until write time. The encoded size is pre-computed eagerly so the caller can
 * allocate buffers of the right capacity, but no intermediate byte array is created. Encoding
 * writes the following sections in order when present: message annotations (symbol-keyed map),
 * properties (AMQP 1.0 properties list), application properties (string-keyed map), and body. The
 * body is encoded as an AMQP {@code data} section if {@link Message#getBodyAsBinary()} succeeds, as
 * an {@code amqp-value} section for non-binary bodies (e.g. a String), or as an empty {@code data}
 * section when the body is {@code null}.
 *
 * <h3>Decoding</h3>
 *
 * <p>{@link #decode(ByteBuf, int)} reads AMQP 1.0 described types from the given buffer region and
 * reconstructs an immutable {@code Message}. Decoded properties are backed by a positional {@code
 * List} (not a POJO), avoiding field-by-field allocation. Map sections (message annotations,
 * application properties) preserve insertion order via {@link LinkedHashMap}. AMQP {@code
 * timestamp} values are returned as raw {@code long} milliseconds, not {@link java.util.Date}
 * objects—this matches QPid Proton behavior but means a {@code Date} set on a property will round-
 * trip as a {@code Long}.
 *
 * @see Amqp10
 * @see StreamingEncodedMessage
 */
public class InternalCodec implements Codec {

  @Override
  public EncodedMessage encode(Message message) {
    return new StreamingEncodedMessage(message);
  }

  @Override
  public Message decode(ByteBuf buf, int length) {
    Amqp10.DecodedMessage decoded = Amqp10.decodeMessage(buf, length);
    return new InternalMessage(
        decoded.properties,
        decoded.applicationProperties,
        decoded.messageAnnotations,
        decoded.bodyData,
        decoded.body);
  }

  @Override
  public MessageBuilder messageBuilder() {
    return new WrapperMessageBuilder();
  }

  private static final class InternalMessage implements Message {

    private final Properties properties;
    private final Map<String, Object> applicationProperties;
    private Map<String, Object> messageAnnotations;
    private final byte[] bodyData;
    private final Object body;

    InternalMessage(
        Properties properties,
        Map<String, Object> applicationProperties,
        Map<String, Object> messageAnnotations,
        byte[] bodyData,
        Object body) {
      this.properties = properties;
      this.applicationProperties = applicationProperties;
      this.messageAnnotations = messageAnnotations;
      this.bodyData = bodyData;
      this.body = body;
    }

    @Override
    public boolean hasPublishingId() {
      return false;
    }

    @Override
    public long getPublishingId() {
      return 0;
    }

    @Override
    public byte[] getBodyAsBinary() {
      if (bodyData != null) {
        return bodyData;
      }
      if (body == null) {
        return null;
      }
      if (body instanceof byte[]) {
        return (byte[]) body;
      }
      throw new IllegalStateException(
          "Body cannot be returned as array of bytes. Use #getBody() to get native representation.");
    }

    @Override
    public Object getBody() {
      return body;
    }

    @Override
    public Properties getProperties() {
      return properties;
    }

    @Override
    public Map<String, Object> getApplicationProperties() {
      return applicationProperties;
    }

    @Override
    public Map<String, Object> getMessageAnnotations() {
      if (messageAnnotations == null) {
        messageAnnotations = new LinkedHashMap<>();
      }
      return messageAnnotations;
    }

    @Override
    public Message annotate(String key, Object value) {
      if (messageAnnotations == null) {
        messageAnnotations = new LinkedHashMap<>();
      }
      messageAnnotations.put(key, value);
      return this;
    }

    @Override
    public Message copy() {
      Map<String, Object> annotationsCopy =
          this.messageAnnotations == null ? null : new LinkedHashMap<>(this.messageAnnotations);
      return new InternalMessage(
          this.properties, this.applicationProperties, annotationsCopy, this.bodyData, this.body);
    }
  }
}
