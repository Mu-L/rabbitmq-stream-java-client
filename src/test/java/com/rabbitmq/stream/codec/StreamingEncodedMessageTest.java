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

import static org.assertj.core.api.Assertions.assertThat;

import com.rabbitmq.stream.Codec;
import com.rabbitmq.stream.Message;
import com.rabbitmq.stream.MessageBuilder;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.IntStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class StreamingEncodedMessageTest {

  private static final InternalCodec CODEC = new InternalCodec();

  ByteBuf buf;

  @BeforeEach
  void setUp() {
    buf = Unpooled.buffer();
  }

  @AfterEach
  void tearDown() {
    buf.release();
  }

  @Test
  void sizeCalculationShouldBeExactForSimpleMessage() {
    Message message = CODEC.messageBuilder().addData("Hello World".getBytes()).build();

    Codec.EncodedMessage encoded = CODEC.encode(message);

    // Verify size matches actual encoded data (ByteBuf now includes 4-byte size prefix)
    encoded.writeTo(buf);

    // ByteBuf contains: [4-byte size] + [message content]
    // So total ByteBuf size should be: size + 4
    assertThat(buf.readableBytes()).isEqualTo(encoded.getSize() + 4);

    // Verify the size prefix is correct
    int sizeFromBuf = buf.readInt();
    assertThat(sizeFromBuf).isEqualTo(encoded.getSize());

    // Remaining bytes should match the message size
    assertThat(buf.readableBytes()).isEqualTo(encoded.getSize());
  }

  @Test
  void sizeCalculationShouldBeExactForComplexMessage() {
    UUID messageId = UUID.randomUUID();
    long currentTime = System.currentTimeMillis();

    Message message =
        CODEC
            .messageBuilder()
            .messageAnnotations()
            .entry("x-routing-key", "test.routing.key")
            .entry("x-priority", (byte) 5)
            .entry("x-delay", 1000)
            .messageBuilder()
            .properties()
            .messageId(messageId)
            .userId("test-user".getBytes())
            .to("test-destination")
            .subject("test-subject")
            .replyTo("test-reply")
            .correlationId("correlation-123")
            .contentType("application/json")
            .contentEncoding("utf-8")
            .absoluteExpiryTime(currentTime + 60000)
            .creationTime(currentTime)
            .groupId("test-group")
            .groupSequence(1L)
            .replyToGroupId("reply-group")
            .messageBuilder()
            .applicationProperties()
            .entry("string-prop", "test-value")
            .entry("int-prop", 42)
            .entry("long-prop", 123456789L)
            .entry("boolean-prop", true)
            .entry("uuid-prop", UUID.randomUUID())
            .entry("binary-prop", "binary-data".getBytes())
            .messageBuilder()
            .addData("{\"message\": \"complex test data with unicode: ñáéíóú\"}".getBytes())
            .build();

    Codec.EncodedMessage encoded = CODEC.encode(message);

    // Verify size matches actual encoded data (ByteBuf now includes 4-byte size prefix)
    encoded.writeTo(buf);

    // ByteBuf contains: [4-byte size] + [message content]
    assertThat(buf.readableBytes()).isEqualTo(encoded.getSize() + 4);

    // Verify the size prefix
    int sizeFromBuf = buf.readInt();
    assertThat(sizeFromBuf).isEqualTo(encoded.getSize());
  }

  @Test
  void sizeCalculationShouldBeExactForMessageWithProperties() {
    Message message =
        CODEC
            .messageBuilder()
            .properties()
            .messageId(12345L)
            .contentType("application/json")
            .messageBuilder()
            .addData("Test data with properties".getBytes())
            .build();

    Codec.EncodedMessage encoded = CODEC.encode(message);

    // Verify size matches actual encoded data (ByteBuf now includes 4-byte size prefix)
    encoded.writeTo(buf);

    // ByteBuf contains: [4-byte size] + [message content]
    assertThat(buf.readableBytes()).isEqualTo(encoded.getSize() + 4);

    // Verify the size prefix
    int sizeFromBuf = buf.readInt();
    assertThat(sizeFromBuf).isEqualTo(encoded.getSize());
  }

  @Test
  void sizeCalculationShouldBeExactForEmptyMessage() {
    Message message = CODEC.messageBuilder().build();

    Codec.EncodedMessage encoded = CODEC.encode(message);

    // Verify size matches actual encoded data (ByteBuf now includes 4-byte size prefix)
    encoded.writeTo(buf);

    // ByteBuf contains: [4-byte size] + [message content]
    assertThat(buf.readableBytes()).isEqualTo(encoded.getSize() + 4);

    // Verify the size prefix
    int sizeFromBuf = buf.readInt();
    assertThat(sizeFromBuf).isEqualTo(encoded.getSize());
  }

  @Test
  void sizeCalculationShouldBeExactForBasicTypes() {
    Message message =
        CODEC
            .messageBuilder()
            .applicationProperties()
            .entry("string-prop", "test-string")
            .entry("int-prop", 42)
            .entry("long-prop", 123456789L)
            .entry("boolean-prop", true)
            .entry("double-prop", 3.14159)
            .messageBuilder()
            .addData("Basic types test".getBytes())
            .build();

    Codec.EncodedMessage encoded = CODEC.encode(message);

    // Verify size matches actual encoded data (ByteBuf now includes 4-byte size prefix)
    encoded.writeTo(buf);

    // ByteBuf contains: [4-byte size] + [message content]
    assertThat(buf.readableBytes()).isEqualTo(encoded.getSize() + 4);

    // Verify the size prefix
    int sizeFromBuf = buf.readInt();
    assertThat(sizeFromBuf).isEqualTo(encoded.getSize());
  }

  @Test
  void outputStreamWritingShouldProduceCorrectSize() throws IOException {
    Message message =
        CODEC
            .messageBuilder()
            .properties()
            .messageId("test-message-id")
            .messageBuilder()
            .addData("Test data for OutputStream".getBytes())
            .build();

    Codec.EncodedMessage encoded = CODEC.encode(message);

    // Write to OutputStream
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    encoded.writeTo(outputStream);
    byte[] streamData = outputStream.toByteArray();

    // The first 4 bytes are the size prefix, rest is the message data
    int sizeFromStream =
        ((streamData[0] & 0xFF) << 24)
            | ((streamData[1] & 0xFF) << 16)
            | ((streamData[2] & 0xFF) << 8)
            | (streamData[3] & 0xFF);

    assertThat(sizeFromStream).isEqualTo(encoded.getSize());
    assertThat(streamData.length).isEqualTo(4 + encoded.getSize());

    // Verify ByteBuf output matches OutputStream output (both now include size prefix)
    encoded.writeTo(buf);
    byte[] bufData = new byte[buf.readableBytes()];
    buf.readBytes(bufData);

    // Both OutputStream and ByteBuf should produce identical output now
    assertThat(bufData).isEqualTo(streamData);
  }

  @Test
  void sizeShouldBeAvailableImmediately() {
    Message message =
        CODEC
            .messageBuilder()
            .properties()
            .messageId("immediate-size-test")
            .messageBuilder()
            .addData("Size should be available without encoding".getBytes())
            .build();

    // Size should be available immediately after construction
    Codec.EncodedMessage encoded = new StreamingEncodedMessage(message);
    int size = encoded.getSize();

    assertThat(size).isGreaterThan(0);

    // Verify it matches actual encoding (ByteBuf now includes 4-byte size prefix)
    encoded.writeTo(buf);
    assertThat(buf.readableBytes()).isEqualTo(size + 4);

    // Verify the size prefix matches
    int sizeFromBuf = buf.readInt();
    assertThat(sizeFromBuf).isEqualTo(size);
  }

  @Test
  void sizeCalculationShouldBeExactForSmallApplicationProperties() {
    Message message =
        CODEC
            .messageBuilder()
            .applicationProperties()
            .entry("k", "v")
            .messageBuilder()
            .addData("x".getBytes())
            .build();

    assertSizeMatchesEncoding(message);
  }

  @Test
  void sizeCalculationShouldBeExactForLargeApplicationProperties() {
    MessageBuilder.ApplicationPropertiesBuilder appProps =
        CODEC.messageBuilder().applicationProperties();
    for (int i = 0; i < 50; i++) {
      appProps.entry("key-with-long-name-" + i, "value-with-long-content-" + i);
    }
    Message message = appProps.messageBuilder().addData("payload".getBytes()).build();

    assertSizeMatchesEncoding(message);
  }

  @Test
  void sizeCalculationShouldBeExactForSmallMessageAnnotations() {
    Message message =
        CODEC
            .messageBuilder()
            .messageAnnotations()
            .entry("a", "b")
            .messageBuilder()
            .addData("x".getBytes())
            .build();

    assertSizeMatchesEncoding(message);
  }

  @Test
  void sizeCalculationShouldBeExactForLargeMessageAnnotations() {
    MessageBuilder.MessageAnnotationsBuilder annotations =
        CODEC.messageBuilder().messageAnnotations();
    for (int i = 0; i < 50; i++) {
      annotations.entry("annotation-key-" + i, "annotation-value-" + i);
    }
    Message message = annotations.messageBuilder().addData("payload".getBytes()).build();

    assertSizeMatchesEncoding(message);
  }

  @Test
  void sizeCalculationShouldBeExactForSmallNestedList() {
    Message message =
        CODEC
            .messageBuilder()
            .messageAnnotations()
            .entry("list", List.of("a", "b"))
            .messageBuilder()
            .addData("x".getBytes())
            .build();

    assertSizeMatchesEncoding(message);
  }

  @Test
  void sizeCalculationShouldBeExactForLargeNestedList() {
    List<String> largeList =
        IntStream.range(0, 50)
            .mapToObj(i -> "element-with-some-content-" + i)
            .collect(java.util.stream.Collectors.toList());
    Message message =
        CODEC
            .messageBuilder()
            .messageAnnotations()
            .entry("large-list", largeList)
            .messageBuilder()
            .addData("x".getBytes())
            .build();

    assertSizeMatchesEncoding(message);
  }

  @Test
  void sizeCalculationShouldBeExactForSmallNestedMap() {
    Message message =
        CODEC
            .messageBuilder()
            .messageAnnotations()
            .entry("map", Map.of("k1", "v1"))
            .messageBuilder()
            .addData("x".getBytes())
            .build();

    assertSizeMatchesEncoding(message);
  }

  @Test
  void sizeCalculationShouldBeExactForLargeNestedMap() {
    Map<String, Object> largeMap = new LinkedHashMap<>();
    for (int i = 0; i < 50; i++) {
      largeMap.put("map-key-" + i, "map-value-" + i);
    }
    Message message =
        CODEC
            .messageBuilder()
            .messageAnnotations()
            .entry("large-map", largeMap)
            .messageBuilder()
            .addData("x".getBytes())
            .build();

    assertSizeMatchesEncoding(message);
  }

  @Test
  void sizeCalculationShouldBeExactForSmallArray() {
    Message message =
        CODEC
            .messageBuilder()
            .messageAnnotations()
            .entryArray("arr", new String[] {"a", "b"})
            .messageBuilder()
            .addData("x".getBytes())
            .build();

    assertSizeMatchesEncoding(message);
  }

  @Test
  void sizeCalculationShouldBeExactForLargeArray() {
    String[] largeArray =
        IntStream.range(0, 50).mapToObj(i -> "element-" + i).toArray(String[]::new);
    Message message =
        CODEC
            .messageBuilder()
            .messageAnnotations()
            .entryArray("large-arr", largeArray)
            .messageBuilder()
            .addData("x".getBytes())
            .build();

    assertSizeMatchesEncoding(message);
  }

  @Test
  void sizeCalculationShouldBeExactForSmallProperties() {
    Message message =
        CODEC
            .messageBuilder()
            .properties()
            .messageId(1L)
            .messageBuilder()
            .addData("x".getBytes())
            .build();

    assertSizeMatchesEncoding(message);
  }

  @Test
  void sizeCalculationShouldBeExactForLargeProperties() {
    String longString = "x".repeat(300);
    Message message =
        CODEC
            .messageBuilder()
            .properties()
            .messageId(longString)
            .to(longString)
            .subject(longString)
            .replyTo(longString)
            .correlationId(longString)
            .groupId(longString)
            .replyToGroupId(longString)
            .messageBuilder()
            .addData("x".getBytes())
            .build();

    assertSizeMatchesEncoding(message);
  }

  private void assertSizeMatchesEncoding(Message message) {
    Codec.EncodedMessage encoded = CODEC.encode(message);
    encoded.writeTo(buf);
    assertThat(buf.readableBytes()).isEqualTo(encoded.getSize() + 4);
    int sizeFromBuf = buf.readInt();
    assertThat(sizeFromBuf).isEqualTo(encoded.getSize());
  }
}
