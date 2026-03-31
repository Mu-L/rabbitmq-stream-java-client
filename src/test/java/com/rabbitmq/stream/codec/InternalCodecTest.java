// Copyright (c) 2026 Broadcom. All Rights Reserved.
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
import com.rabbitmq.stream.Properties;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

public class InternalCodecTest {

  private static final Codec CODEC = new InternalCodec();

  @Test
  void testEmptyProperties() {
    MessageBuilder builder = CODEC.messageBuilder();
    builder.properties().groupId(null);
    Message msg = builder.build();

    Codec.EncodedMessage encoded = CODEC.encode(msg);
    ByteBuf buf = Unpooled.buffer(encoded.getSize() + 4);
    encoded.writeTo(buf);
    buf.readInt(); // skip size
    Message decoded = CODEC.decode(buf, encoded.getSize());

    Properties properties = decoded.getProperties();
    assertThat(properties).isNotNull();
    assertThat(properties.getGroupId()).isNull();
  }

  @Test
  void testCharIsReturnedAsInt() {
    // AMQP 1.0 defines char as a UTF-32 code point (4 bytes, full Unicode range).
    // Java's char type is only 16-bit and cannot represent characters above U+FFFF
    // (supplementary plane characters like emoji). The InternalCodec correctly
    // returns int (32-bit) to preserve the full Unicode range, while other codecs
    // truncate to Java's char type and lose data for supplementary characters.
    MessageBuilder builder = CODEC.messageBuilder();
    // Basic ASCII character - works in all codecs
    builder.applicationProperties().entry("ascii", (char) 'A');
    Message msg = builder.build();
    Codec.EncodedMessage encoded = CODEC.encode(msg);
    ByteBuf buf = Unpooled.buffer(encoded.getSize() + 4);
    encoded.writeTo(buf);
    buf.readInt();
    Message decoded = CODEC.decode(buf, encoded.getSize());

    Map<String, Object> props = decoded.getApplicationProperties();
    // ASCII character: InternalCodec returns int, other codecs would return char
    assertThat(props.get("ascii")).isInstanceOf(Integer.class).isEqualTo((int) 'A');
  }

  @Test
  void testCharSupplementaryPlane() {
    // AMQP 1.0 defines char as a UTF-32 code point (4 bytes, full Unicode range).
    // Java's char type is only 16-bit and cannot represent characters above U+FFFF
    // (supplementary plane characters like emoji). The InternalCodec correctly
    // returns int (32-bit) to preserve the full Unicode range, while other codecs
    // truncate to Java's char type and lose data for supplementary characters.

    // Emoji (supplementary plane) - would be truncated by other codecs
    // 0x1F600 = 😀 (grinning face emoji)
    int emojiCodePoint = 0x1F600;
    // We can't use Java's char for this since char can't hold values > 0xFFFF
    // So we manually encode it using the internal AMQP char type
    ByteBuf tempBuf = Unpooled.buffer();
    tempBuf.writeByte(0x73); // AMQP char type code
    tempBuf.writeInt(emojiCodePoint);

    // Test emoji by directly decoding AMQP char bytes
    Object emojiResult = com.rabbitmq.stream.codec.Amqp10.readObject(tempBuf);
    assertThat(emojiResult).isInstanceOf(Integer.class).isEqualTo(emojiCodePoint);

    // If other codecs tried to handle this emoji:
    // - They would cast to (char) and get: (char) 0x1F600 = '\uF600' (wrong!)
    // - The high bits (0x1F000) would be lost, corrupting the character
    char truncated = (char) emojiCodePoint;
    assertThat(truncated).isNotEqualTo(emojiCodePoint); // Demonstrates data loss
    assertThat((int) truncated).isEqualTo(0xF600); // Only lower 16 bits preserved
  }

  @Test
  void smallApplicationPropertiesRoundTrip() {
    Message msg =
        CODEC
            .messageBuilder()
            .applicationProperties()
            .entry("k", "v")
            .messageBuilder()
            .addData("x".getBytes())
            .build();

    Message decoded = encodeDecode(msg);
    assertThat(decoded.getApplicationProperties()).containsEntry("k", "v");
    assertThat(decoded.getBodyAsBinary()).isEqualTo("x".getBytes());
  }

  @Test
  void largeApplicationPropertiesRoundTrip() {
    MessageBuilder.ApplicationPropertiesBuilder appProps =
        CODEC.messageBuilder().applicationProperties();
    for (int i = 0; i < 50; i++) {
      appProps.entry("key-with-long-name-" + i, "value-with-long-content-" + i);
    }
    Message msg = appProps.messageBuilder().addData("payload".getBytes()).build();

    Message decoded = encodeDecode(msg);
    assertThat(decoded.getApplicationProperties()).hasSize(50);
    assertThat(decoded.getApplicationProperties().get("key-with-long-name-0"))
        .isEqualTo("value-with-long-content-0");
    assertThat(decoded.getApplicationProperties().get("key-with-long-name-49"))
        .isEqualTo("value-with-long-content-49");
  }

  @Test
  void smallMessageAnnotationsRoundTrip() {
    Message msg =
        CODEC
            .messageBuilder()
            .messageAnnotations()
            .entry("a", "b")
            .messageBuilder()
            .addData("x".getBytes())
            .build();

    Message decoded = encodeDecode(msg);
    assertThat(decoded.getMessageAnnotations()).containsEntry("a", "b");
  }

  @Test
  void largeMessageAnnotationsRoundTrip() {
    MessageBuilder.MessageAnnotationsBuilder annotations =
        CODEC.messageBuilder().messageAnnotations();
    for (int i = 0; i < 50; i++) {
      annotations.entry("annotation-key-" + i, "annotation-value-" + i);
    }
    Message msg = annotations.messageBuilder().addData("payload".getBytes()).build();

    Message decoded = encodeDecode(msg);
    assertThat(decoded.getMessageAnnotations()).hasSize(50);
    assertThat(decoded.getMessageAnnotations().get("annotation-key-0"))
        .isEqualTo("annotation-value-0");
  }

  @Test
  void smallNestedListRoundTrip() {
    Message msg =
        CODEC
            .messageBuilder()
            .messageAnnotations()
            .entry("list", List.of("a", "b"))
            .messageBuilder()
            .addData("x".getBytes())
            .build();

    Message decoded = encodeDecode(msg);
    @SuppressWarnings("unchecked")
    List<Object> list = (List<Object>) decoded.getMessageAnnotations().get("list");
    assertThat(list).containsExactly("a", "b");
  }

  @Test
  void largeNestedListRoundTrip() {
    List<String> largeList =
        IntStream.range(0, 50)
            .mapToObj(i -> "element-with-some-content-" + i)
            .collect(Collectors.toList());
    Message msg =
        CODEC
            .messageBuilder()
            .messageAnnotations()
            .entry("large-list", largeList)
            .messageBuilder()
            .addData("x".getBytes())
            .build();

    Message decoded = encodeDecode(msg);
    List<?> list = (List<?>) decoded.getMessageAnnotations().get("large-list");
    assertThat(list).hasSize(50);
    assertThat(list.get(0)).isEqualTo("element-with-some-content-0");
    assertThat(list.get(49)).isEqualTo("element-with-some-content-49");
  }

  @Test
  void smallNestedMapRoundTrip() {
    Message msg =
        CODEC
            .messageBuilder()
            .messageAnnotations()
            .entry("map", Map.of("k1", "v1"))
            .messageBuilder()
            .addData("x".getBytes())
            .build();

    Message decoded = encodeDecode(msg);
    @SuppressWarnings("unchecked")
    Map<String, Object> map = (Map<String, Object>) decoded.getMessageAnnotations().get("map");
    assertThat(map).containsEntry("k1", "v1");
  }

  @Test
  void largeNestedMapRoundTrip() {
    Map<String, Object> largeMap = new LinkedHashMap<>();
    for (int i = 0; i < 50; i++) {
      largeMap.put("map-key-" + i, "map-value-" + i);
    }
    Message msg =
        CODEC
            .messageBuilder()
            .messageAnnotations()
            .entry("large-map", largeMap)
            .messageBuilder()
            .addData("x".getBytes())
            .build();

    Message decoded = encodeDecode(msg);
    Map<?, ?> map = (Map<?, ?>) decoded.getMessageAnnotations().get("large-map");
    assertThat(map).hasSize(50);
    assertThat(map.get("map-key-0")).isEqualTo("map-value-0");
    assertThat(map.get("map-key-49")).isEqualTo("map-value-49");
  }

  @Test
  void smallArrayRoundTrip() {
    Message msg =
        CODEC
            .messageBuilder()
            .messageAnnotations()
            .entryArray("arr", new String[] {"a", "b"})
            .messageBuilder()
            .addData("x".getBytes())
            .build();

    Message decoded = encodeDecode(msg);
    Object[] arr = (Object[]) decoded.getMessageAnnotations().get("arr");
    assertThat(arr).containsExactly("a", "b");
  }

  @Test
  void largeArrayRoundTrip() {
    String[] largeArray =
        IntStream.range(0, 50).mapToObj(i -> "element-" + i).toArray(String[]::new);
    Message msg =
        CODEC
            .messageBuilder()
            .messageAnnotations()
            .entryArray("large-arr", largeArray)
            .messageBuilder()
            .addData("x".getBytes())
            .build();

    Message decoded = encodeDecode(msg);
    Object[] arr = (Object[]) decoded.getMessageAnnotations().get("large-arr");
    assertThat(arr).hasSize(50);
    assertThat(arr[0]).isEqualTo("element-0");
    assertThat(arr[49]).isEqualTo("element-49");
  }

  @Test
  void smallPropertiesRoundTrip() {
    Message msg =
        CODEC
            .messageBuilder()
            .properties()
            .messageId(1L)
            .messageBuilder()
            .addData("x".getBytes())
            .build();

    Message decoded = encodeDecode(msg);
    assertThat(decoded.getProperties().getMessageIdAsLong()).isEqualTo(1L);
  }

  @Test
  void multiByteUtf8StringRoundTrip() {
    // 2-byte UTF-8 characters (Latin Extended, U+00E0..U+00FF): each char = 2 bytes
    // 128 such chars = 256 UTF-8 bytes, forcing STR32 despite only 128 characters
    String twoByteChars = "\u00E9".repeat(128);
    assertThat(twoByteChars.length()).isEqualTo(128);
    assertThat(twoByteChars.getBytes(java.nio.charset.StandardCharsets.UTF_8).length)
        .isEqualTo(256);

    // 3-byte UTF-8 characters (CJK, U+4E00): each char = 3 bytes
    String threeByteChars = "\u4E00".repeat(86);
    assertThat(threeByteChars.getBytes(java.nio.charset.StandardCharsets.UTF_8).length)
        .isEqualTo(258);

    // 4-byte UTF-8 characters (emoji, U+1F600): each code point = 4 bytes, 2 Java chars
    String fourByteChars = new String(Character.toChars(0x1F600)).repeat(64);
    assertThat(fourByteChars.getBytes(java.nio.charset.StandardCharsets.UTF_8).length)
        .isEqualTo(256);

    Message msg =
        CODEC
            .messageBuilder()
            .applicationProperties()
            .entry("two-byte", twoByteChars)
            .entry("three-byte", threeByteChars)
            .entry("four-byte", fourByteChars)
            .messageBuilder()
            .addData("x".getBytes())
            .build();

    Message decoded = encodeDecode(msg);
    assertThat(decoded.getApplicationProperties().get("two-byte")).isEqualTo(twoByteChars);
    assertThat(decoded.getApplicationProperties().get("three-byte")).isEqualTo(threeByteChars);
    assertThat(decoded.getApplicationProperties().get("four-byte")).isEqualTo(fourByteChars);
  }

  @Test
  void multiByteUtf8StringAtStr8Str32Boundary() {
    // Exactly 255 UTF-8 bytes: should use STR8
    String justUnder = "\u00E9".repeat(127) + "x";
    assertThat(justUnder.getBytes(java.nio.charset.StandardCharsets.UTF_8).length).isEqualTo(255);

    // Exactly 256 UTF-8 bytes: must use STR32
    String justOver = "\u00E9".repeat(128);
    assertThat(justOver.getBytes(java.nio.charset.StandardCharsets.UTF_8).length).isEqualTo(256);

    Message msg =
        CODEC
            .messageBuilder()
            .applicationProperties()
            .entry("str8", justUnder)
            .entry("str32", justOver)
            .messageBuilder()
            .addData("x".getBytes())
            .build();

    Message decoded = encodeDecode(msg);
    assertThat(decoded.getApplicationProperties().get("str8")).isEqualTo(justUnder);
    assertThat(decoded.getApplicationProperties().get("str32")).isEqualTo(justOver);
  }

  @Test
  void multiByteUtf8InPropertiesRoundTrip() {
    String twoByteStr = "\u00E9".repeat(128);
    String threeByteStr = "\u4E00".repeat(86);

    Message msg =
        CODEC
            .messageBuilder()
            .properties()
            .messageId(twoByteStr)
            .to(threeByteStr)
            .subject(twoByteStr)
            .correlationId(threeByteStr)
            .groupId(twoByteStr)
            .messageBuilder()
            .addData("x".getBytes())
            .build();

    Message decoded = encodeDecode(msg);
    assertThat(decoded.getProperties().getMessageIdAsString()).isEqualTo(twoByteStr);
    assertThat(decoded.getProperties().getTo()).isEqualTo(threeByteStr);
    assertThat(decoded.getProperties().getSubject()).isEqualTo(twoByteStr);
    assertThat(decoded.getProperties().getCorrelationIdAsString()).isEqualTo(threeByteStr);
    assertThat(decoded.getProperties().getGroupId()).isEqualTo(twoByteStr);
  }

  @Test
  void largePropertiesRoundTrip() {
    String longString = "x".repeat(300);
    Message msg =
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

    Message decoded = encodeDecode(msg);
    assertThat(decoded.getProperties().getMessageIdAsString()).isEqualTo(longString);
    assertThat(decoded.getProperties().getTo()).isEqualTo(longString);
    assertThat(decoded.getProperties().getSubject()).isEqualTo(longString);
    assertThat(decoded.getProperties().getReplyTo()).isEqualTo(longString);
    assertThat(decoded.getProperties().getCorrelationIdAsString()).isEqualTo(longString);
    assertThat(decoded.getProperties().getGroupId()).isEqualTo(longString);
    assertThat(decoded.getProperties().getReplyToGroupId()).isEqualTo(longString);
  }

  private Message encodeDecode(Message msg) {
    Codec.EncodedMessage encoded = CODEC.encode(msg);
    ByteBuf buf = Unpooled.buffer(encoded.getSize() + 4);
    encoded.writeTo(buf);
    buf.readInt();
    return CODEC.decode(buf, encoded.getSize());
  }
}
