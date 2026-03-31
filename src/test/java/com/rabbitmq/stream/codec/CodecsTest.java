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

import static com.rabbitmq.stream.impl.TestUtils.encodedMessageByteBuf;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.rabbitmq.stream.Codec;
import com.rabbitmq.stream.Codec.EncodedMessage;
import com.rabbitmq.stream.Message;
import com.rabbitmq.stream.MessageBuilder;
import com.rabbitmq.stream.amqp.UnsignedByte;
import com.rabbitmq.stream.amqp.UnsignedInteger;
import com.rabbitmq.stream.amqp.UnsignedLong;
import com.rabbitmq.stream.amqp.UnsignedShort;
import com.rabbitmq.stream.codec.QpidProtonCodec.QpidProtonAmqpMessageWrapper;
import io.netty.buffer.ByteBuf;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.MessageAnnotations;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.assertj.core.api.ThrowableAssert;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class CodecsTest {

  static Charset CHARSET = StandardCharsets.UTF_8;

  static UUID TEST_UUID = UUID.randomUUID();

  static Iterable<CodecCouple> codecsCouples() {
    List<Codec> codecs = asList(new QpidProtonCodec(), new SwiftMqCodec(), new InternalCodec());
    List<CodecCouple> couples = new ArrayList<>();
    for (Codec serializer : codecs) {
      for (Codec deserializer : codecs) {
        couples.add(new CodecCouple(serializer, deserializer, serializer::messageBuilder));
        couples.add(new CodecCouple(serializer, deserializer, WrapperMessageBuilder::new));
      }
    }
    return couples;
  }

  static Iterable<CodecCouple> codecsCombinations() {
    List<Codec> codecs = asList(new QpidProtonCodec(), new SwiftMqCodec(), new InternalCodec());
    List<CodecCouple> couples = new ArrayList<>();
    for (Codec serializer : codecs) {
      for (Codec deserializer : codecs) {
        couples.add(new CodecCouple(serializer, deserializer, serializer::messageBuilder));
      }
    }
    return couples;
  }

  static Iterable<Supplier<MessageBuilder>> messageBuilderSuppliers() {
    return asList(
        QpidProtonMessageBuilder::new,
        SwiftMqMessageBuilder::new,
        WrapperMessageBuilder::new,
        new InternalCodec()::messageBuilder);
  }

  static Iterable<Codec> readCreatedMessage() {
    return asList(
        when(mock(Codec.class).messageBuilder()).thenReturn(new WrapperMessageBuilder()).getMock(),
        new QpidProtonCodec(),
        new SwiftMqCodec(),
        new InternalCodec());
  }

  static Stream<Codec> allAmqpCodecs() {
    return Stream.of(new QpidProtonCodec(), new SwiftMqCodec(), new InternalCodec());
  }

  static Stream<MessageBuilder> messageBuilders() {
    return Stream.of(
        new QpidProtonMessageBuilder(),
        new SwiftMqMessageBuilder(),
        new WrapperMessageBuilder(),
        new SimpleCodec().messageBuilder(),
        new InternalCodec().messageBuilder());
  }

  @ParameterizedTest
  @MethodSource("codecsCouples")
  void codecs(CodecCouple codecCouple) {
    Codec serializer = codecCouple.serializer;
    Codec deserializer = codecCouple.deserializer;

    Stream<MessageTestConfiguration> messageOperations =
        Stream.of(
            test(
                builder -> builder.properties().messageId(42).messageBuilder(),
                message -> assertThat(message.getProperties().getMessageIdAsLong()).isEqualTo(42)),
            test(
                builder -> builder.properties().messageId("foo").messageBuilder(),
                message ->
                    assertThat(message.getProperties().getMessageIdAsString()).isEqualTo("foo")),
            test(
                builder -> builder.properties().messageId("bar".getBytes(CHARSET)).messageBuilder(),
                message ->
                    assertThat(message.getProperties().getMessageIdAsBinary())
                        .isEqualTo("bar".getBytes(CHARSET))),
            test(
                builder -> builder.properties().messageId(TEST_UUID).messageBuilder(),
                message ->
                    assertThat(message.getProperties().getMessageIdAsUuid()).isEqualTo(TEST_UUID)),
            test(
                builder -> builder.properties().correlationId(42 + 10).messageBuilder(),
                message ->
                    assertThat(message.getProperties().getCorrelationIdAsLong())
                        .isEqualTo(42 + 10)),
            test(
                builder -> builder.properties().correlationId("correlation foo").messageBuilder(),
                message ->
                    assertThat(message.getProperties().getCorrelationIdAsString())
                        .isEqualTo("correlation foo")),
            test(
                builder ->
                    builder
                        .properties()
                        .correlationId("correlation bar".getBytes(CHARSET))
                        .messageBuilder(),
                message ->
                    assertThat(message.getProperties().getCorrelationIdAsBinary())
                        .isEqualTo("correlation bar".getBytes(CHARSET))),
            test(
                builder -> builder.properties().correlationId(TEST_UUID).messageBuilder(),
                message ->
                    assertThat(message.getProperties().getCorrelationIdAsUuid())
                        .isEqualTo(TEST_UUID)),
            test(
                builder -> builder,
                message -> assertThat(message.getProperties().getGroupSequence()).isEqualTo(-1)),
            test(
                builder -> builder.properties().groupSequence(10).messageBuilder(),
                message -> assertThat(message.getProperties().getGroupSequence()).isEqualTo(10)),
            test(
                builder ->
                    builder
                        .properties()
                        .groupSequence((long) Integer.MAX_VALUE + 10)
                        .messageBuilder(),
                message ->
                    assertThat(message.getProperties().getGroupSequence())
                        .isEqualTo((long) Integer.MAX_VALUE + 10)));

    String body = "hello";
    String userId = "yoda";
    String to = "the to address";
    String subject = "the subject";
    String replyTo = "the reply to";
    String contentType = "text/plain";
    String contentEncoding = "gzip";
    String groupId = "the group ID";
    String replyToGroupId = "the reply to group ID";
    long now = new Date().getTime();
    UUID uuid = UUID.randomUUID();
    byte[] binary = "the binary".getBytes(CHARSET);
    String string = "a string";
    String symbol = "a symbol";
    messageOperations.forEach(
        messageTestConfiguration -> {
          Function<MessageBuilder, MessageBuilder> messageOperation =
              messageTestConfiguration.messageOperation;
          Consumer<Message> messageExpectation = messageTestConfiguration.messageExpectation;
          MessageBuilder messageBuilder = codecCouple.messageBuilderSupplier.get();
          Message outboundMessage =
              messageOperation
                  .apply(messageBuilder)
                  .addData(body.getBytes(CHARSET))
                  .properties()
                  .userId(userId.getBytes(CHARSET))
                  .to(to)
                  .subject(subject)
                  .replyTo(replyTo)
                  .contentType(contentType)
                  .contentEncoding(contentEncoding)
                  .absoluteExpiryTime(now + 1000)
                  .creationTime(now)
                  .groupId(groupId)
                  .replyToGroupId(replyToGroupId)
                  .messageBuilder()
                  .applicationProperties()
                  .entry("boolean", Boolean.FALSE)
                  .entry("byte", (byte) 1)
                  .entry("short", (short) 2)
                  .entry("int", 3)
                  .entry("long", 4l)
                  .entryUnsigned("ubyte", (byte) 1)
                  .entryUnsigned("ushort", (short) 2)
                  .entryUnsigned("uint", 3)
                  .entryUnsigned("ulong", 4l)
                  .entryUnsigned("large.ubyte", (byte) (Byte.MAX_VALUE + 10))
                  .entryUnsigned("large.ushort", (short) (Short.MAX_VALUE + 10))
                  .entryUnsigned("large.uint", Integer.MAX_VALUE + 10)
                  .entryUnsigned("large.ulong", Long.MAX_VALUE + 10)
                  .entry("float", 3.14f)
                  .entry("double", 6.28)
                  .entry("char", 'c')
                  .entryTimestamp("timestamp", now)
                  .entry("uuid", uuid)
                  .entry("binary", binary)
                  .entry("string", string)
                  .entrySymbol("symbol", symbol)
                  .entry("null", (String) null)
                  .messageBuilder()
                  .messageAnnotations()
                  .entry("annotations.boolean", Boolean.FALSE)
                  .entry("annotations.byte", (byte) 1)
                  .entry("annotations.short", (short) 2)
                  .entry("annotations.int", 3)
                  .entry("annotations.long", 4l)
                  .entryUnsigned("annotations.ubyte", (byte) 1)
                  .entryUnsigned("annotations.ushort", (short) 2)
                  .entryUnsigned("annotations.uint", 3)
                  .entryUnsigned("annotations.ulong", 4l)
                  .entryUnsigned("annotations.large.ubyte", (byte) (Byte.MAX_VALUE + 10))
                  .entryUnsigned("annotations.large.ushort", (short) (Short.MAX_VALUE + 10))
                  .entryUnsigned("annotations.large.uint", Integer.MAX_VALUE + 10)
                  .entryUnsigned("annotations.large.ulong", Long.MAX_VALUE + 10)
                  .entry("annotations.float", 3.14f)
                  .entry("annotations.double", 6.28)
                  .entry("annotations.char", 'c')
                  .entryTimestamp("annotations.timestamp", now)
                  .entry("annotations.uuid", uuid)
                  .entry("annotations.binary", binary)
                  .entry("annotations.string", string)
                  .entrySymbol("annotations.symbol", symbol)
                  .entry("annotations.null", (String) null)
                  .entry(
                      "list",
                      List.of("1", "2", 3, List.of("1"), Map.of("k1", "v1"), new String[] {"1"}))
                  .entry(
                      "map",
                      Map.of(
                          "k1",
                          "v1",
                          "k2",
                          List.of("v2"),
                          "k3",
                          Map.of("k1", "v1"),
                          "k4",
                          new String[] {"1"}))
                  .entryArray("arrayString", new String[] {"1", "2", "3"})
                  .entryArray("arrayInt", new Integer[] {200, 201, 202})
                  .messageBuilder()
                  .build();
          outboundMessage.annotate("extra.annotation", "extra annotation value");

          Codec.EncodedMessage encoded = serializer.encode(outboundMessage);

          ByteBuf encodedData = encodedMessageByteBuf(encoded);
          Message inboundMessage = deserializer.decode(encodedData, encodedData.readableBytes());

          messageExpectation.accept(inboundMessage);

          assertThat(new String(inboundMessage.getBodyAsBinary())).isEqualTo(body);

          assertThat(inboundMessage.getProperties().getUserId())
              .isEqualTo(userId.getBytes(CHARSET));
          assertThat(inboundMessage.getProperties().getTo()).isEqualTo(to);
          assertThat(inboundMessage.getProperties().getSubject()).isEqualTo(subject);
          assertThat(inboundMessage.getProperties().getReplyTo()).isEqualTo(replyTo);
          assertThat(inboundMessage.getProperties().getContentType()).isEqualTo(contentType);
          assertThat(inboundMessage.getProperties().getContentEncoding())
              .isEqualTo(contentEncoding);
          assertThat(inboundMessage.getProperties().getAbsoluteExpiryTime()).isEqualTo(now + 1000);
          assertThat(inboundMessage.getProperties().getCreationTime()).isEqualTo(now);
          assertThat(inboundMessage.getProperties().getGroupId()).isEqualTo(groupId);
          assertThat(inboundMessage.getProperties().getReplyToGroupId()).isEqualTo(replyToGroupId);

          // application properties
          assertThat(inboundMessage.getApplicationProperties().get("boolean"))
              .isNotNull()
              .isInstanceOf(Boolean.class)
              .isEqualTo(Boolean.FALSE);
          assertThat(inboundMessage.getApplicationProperties().get("byte"))
              .isNotNull()
              .isInstanceOf(Byte.class)
              .isEqualTo(Byte.valueOf((byte) 1));
          assertThat(inboundMessage.getApplicationProperties().get("short"))
              .isNotNull()
              .isInstanceOf(Short.class)
              .isEqualTo(Short.valueOf((short) 2));
          assertThat(inboundMessage.getApplicationProperties().get("int"))
              .isNotNull()
              .isInstanceOf(Integer.class)
              .isEqualTo(Integer.valueOf(3));
          assertThat(inboundMessage.getApplicationProperties().get("long"))
              .isNotNull()
              .isInstanceOf(Long.class)
              .isEqualTo(Long.valueOf(4));

          assertThat(inboundMessage.getApplicationProperties().get("ubyte"))
              .isNotNull()
              .isInstanceOf(UnsignedByte.class)
              .isEqualTo(UnsignedByte.valueOf((byte) 1));
          assertThat(inboundMessage.getApplicationProperties().get("ushort"))
              .isNotNull()
              .isInstanceOf(UnsignedShort.class)
              .isEqualTo(UnsignedShort.valueOf((short) 2));
          assertThat(inboundMessage.getApplicationProperties().get("uint"))
              .isNotNull()
              .isInstanceOf(UnsignedInteger.class)
              .isEqualTo(UnsignedInteger.valueOf(3));
          assertThat(inboundMessage.getApplicationProperties().get("ulong"))
              .isNotNull()
              .isInstanceOf(UnsignedLong.class)
              .isEqualTo(UnsignedLong.valueOf(4));

          assertThat(inboundMessage.getApplicationProperties().get("large.ubyte"))
              .isNotNull()
              .isInstanceOf(UnsignedByte.class)
              .asInstanceOf(InstanceOfAssertFactories.type(UnsignedByte.class))
              .extracting(v -> v.intValue())
              .isEqualTo(Byte.MAX_VALUE + 10);
          assertThat(inboundMessage.getApplicationProperties().get("large.ushort"))
              .isNotNull()
              .isInstanceOf(UnsignedShort.class)
              .asInstanceOf(InstanceOfAssertFactories.type(UnsignedShort.class))
              .extracting(v -> v.intValue())
              .isEqualTo(Short.MAX_VALUE + 10);
          assertThat(inboundMessage.getApplicationProperties().get("large.uint"))
              .isNotNull()
              .isInstanceOf(UnsignedInteger.class)
              .asInstanceOf(InstanceOfAssertFactories.type(UnsignedInteger.class))
              .extracting(v -> v.toString())
              .isEqualTo(BigInteger.valueOf((long) Integer.MAX_VALUE + 10L).toString());
          assertThat(inboundMessage.getApplicationProperties().get("large.ulong"))
              .isNotNull()
              .isInstanceOf(UnsignedLong.class)
              .asInstanceOf(InstanceOfAssertFactories.type(UnsignedLong.class))
              .extracting(v -> v.toString())
              .isEqualTo(BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.TEN).toString());

          assertThat(inboundMessage.getApplicationProperties().get("float"))
              .isNotNull()
              .isInstanceOf(Float.class)
              .isEqualTo(Float.valueOf(3.14f));
          assertThat(inboundMessage.getApplicationProperties().get("double"))
              .isNotNull()
              .isInstanceOf(Double.class)
              .isEqualTo(Double.valueOf(6.28));
          // InternalCodec returns Integer (full Unicode support per AMQP 1.0 spec)
          // QpidProtonCodec and SwiftMqCodec return Character (16-bit, truncated)
          Object charValue = inboundMessage.getApplicationProperties().get("char");
          assertThat(charValue).isNotNull();
          if (codecCouple.deserializer instanceof InternalCodec) {
            assertThat(charValue).isInstanceOf(Integer.class).isEqualTo((int) 'c');
          } else {
            assertThat(charValue).isInstanceOf(Character.class).isEqualTo('c');
          }
          assertThat(inboundMessage.getApplicationProperties().get("timestamp"))
              .isNotNull()
              .isInstanceOf(Long.class)
              .isEqualTo(now);
          assertThat(inboundMessage.getApplicationProperties().get("uuid"))
              .isNotNull()
              .isInstanceOf(UUID.class)
              .isEqualTo(uuid);
          assertThat(inboundMessage.getApplicationProperties().get("binary"))
              .isNotNull()
              .isInstanceOf(byte[].class)
              .isEqualTo(binary);
          assertThat(inboundMessage.getApplicationProperties().get("string"))
              .isNotNull()
              .isInstanceOf(String.class)
              .isEqualTo(string);
          assertThat(inboundMessage.getApplicationProperties().get("symbol"))
              .isNotNull()
              .isInstanceOf(String.class)
              .isEqualTo(symbol);
          assertThat(inboundMessage.getApplicationProperties().get("null")).isNull();

          // message annotations
          assertThat(inboundMessage.getMessageAnnotations().get("annotations.boolean"))
              .isNotNull()
              .isInstanceOf(Boolean.class)
              .isEqualTo(Boolean.FALSE);
          assertThat(inboundMessage.getMessageAnnotations().get("annotations.byte"))
              .isNotNull()
              .isInstanceOf(Byte.class)
              .isEqualTo(Byte.valueOf((byte) 1));
          assertThat(inboundMessage.getMessageAnnotations().get("annotations.short"))
              .isNotNull()
              .isInstanceOf(Short.class)
              .isEqualTo(Short.valueOf((short) 2));
          assertThat(inboundMessage.getMessageAnnotations().get("annotations.int"))
              .isNotNull()
              .isInstanceOf(Integer.class)
              .isEqualTo(Integer.valueOf(3));
          assertThat(inboundMessage.getMessageAnnotations().get("annotations.long"))
              .isNotNull()
              .isInstanceOf(Long.class)
              .isEqualTo(Long.valueOf(4));

          assertThat(inboundMessage.getMessageAnnotations().get("annotations.ubyte"))
              .isNotNull()
              .isInstanceOf(UnsignedByte.class)
              .isEqualTo(UnsignedByte.valueOf((byte) 1));
          assertThat(inboundMessage.getMessageAnnotations().get("annotations.ushort"))
              .isNotNull()
              .isInstanceOf(UnsignedShort.class)
              .isEqualTo(UnsignedShort.valueOf((short) 2));
          assertThat(inboundMessage.getMessageAnnotations().get("annotations.uint"))
              .isNotNull()
              .isInstanceOf(UnsignedInteger.class)
              .isEqualTo(UnsignedInteger.valueOf(3));
          assertThat(inboundMessage.getMessageAnnotations().get("annotations.ulong"))
              .isNotNull()
              .isInstanceOf(UnsignedLong.class)
              .isEqualTo(UnsignedLong.valueOf(4));

          assertThat(inboundMessage.getMessageAnnotations().get("annotations.large.ubyte"))
              .isNotNull()
              .isInstanceOf(UnsignedByte.class)
              .asInstanceOf(InstanceOfAssertFactories.type(UnsignedByte.class))
              .extracting(v -> v.intValue())
              .isEqualTo(Byte.MAX_VALUE + 10);
          assertThat(inboundMessage.getMessageAnnotations().get("annotations.large.ushort"))
              .isNotNull()
              .isInstanceOf(UnsignedShort.class)
              .asInstanceOf(InstanceOfAssertFactories.type(UnsignedShort.class))
              .extracting(v -> v.intValue())
              .isEqualTo(Short.MAX_VALUE + 10);
          assertThat(inboundMessage.getMessageAnnotations().get("annotations.large.uint"))
              .isNotNull()
              .isInstanceOf(UnsignedInteger.class)
              .asInstanceOf(InstanceOfAssertFactories.type(UnsignedInteger.class))
              .extracting(v -> v.toString())
              .isEqualTo(BigInteger.valueOf((long) Integer.MAX_VALUE + 10L).toString());
          assertThat(inboundMessage.getMessageAnnotations().get("annotations.large.ulong"))
              .isNotNull()
              .isInstanceOf(UnsignedLong.class)
              .asInstanceOf(InstanceOfAssertFactories.type(UnsignedLong.class))
              .extracting(v -> v.toString())
              .isEqualTo(BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.TEN).toString());

          assertThat(inboundMessage.getMessageAnnotations().get("annotations.float"))
              .isNotNull()
              .isInstanceOf(Float.class)
              .isEqualTo(Float.valueOf(3.14f));
          assertThat(inboundMessage.getMessageAnnotations().get("annotations.double"))
              .isNotNull()
              .isInstanceOf(Double.class)
              .isEqualTo(Double.valueOf(6.28));
          // InternalCodec returns Integer (full Unicode support per AMQP 1.0 spec)
          // QpidProtonCodec and SwiftMqCodec return Character (16-bit, truncated)
          Object annotationCharValue =
              inboundMessage.getMessageAnnotations().get("annotations.char");
          assertThat(annotationCharValue).isNotNull();
          if (codecCouple.deserializer instanceof InternalCodec) {
            assertThat(annotationCharValue).isInstanceOf(Integer.class).isEqualTo((int) 'c');
          } else {
            assertThat(annotationCharValue).isInstanceOf(Character.class).isEqualTo('c');
          }
          assertThat(inboundMessage.getMessageAnnotations().get("annotations.timestamp"))
              .isNotNull()
              .isInstanceOf(Long.class)
              .isEqualTo(now);
          assertThat(inboundMessage.getMessageAnnotations().get("annotations.uuid"))
              .isNotNull()
              .isInstanceOf(UUID.class)
              .isEqualTo(uuid);
          assertThat(inboundMessage.getMessageAnnotations().get("annotations.binary"))
              .isNotNull()
              .isInstanceOf(byte[].class)
              .isEqualTo(binary);
          assertThat(inboundMessage.getMessageAnnotations().get("annotations.string"))
              .isNotNull()
              .isInstanceOf(String.class)
              .isEqualTo(string);
          assertThat(inboundMessage.getMessageAnnotations().get("annotations.symbol"))
              .isNotNull()
              .isInstanceOf(String.class)
              .isEqualTo(symbol);
          assertThat(inboundMessage.getMessageAnnotations().get("annotations.null")).isNull();
          assertThat(inboundMessage.getMessageAnnotations().get("extra.annotation"))
              .isNotNull()
              .isInstanceOf(String.class)
              .isEqualTo("extra annotation value");

          List<?> list = (List<?>) inboundMessage.getMessageAnnotations().get("list");
          assertThat(list.get(0)).isEqualTo("1");
          assertThat(list.get(1)).isEqualTo("2");
          assertThat(list.get(2)).isEqualTo(3);
          assertThat(list.get(3)).isEqualTo(List.of("1"));
          assertThat(list.get(4)).isEqualTo(Map.of("k1", "v1"));
          assertThat(list.get(5)).isEqualTo(new String[] {"1"});

          Map<?, ?> map = (Map<?, ?>) inboundMessage.getMessageAnnotations().get("map");
          assertThat(map.get("k1")).isEqualTo("v1");
          assertThat(map.get("k2")).isEqualTo(List.of("v2"));
          assertThat(map.get("k3")).isEqualTo(Map.of("k1", "v1"));
          assertThat(map.get("k4")).isEqualTo(new String[] {"1"});

          Object[] arrayString =
              (Object[]) inboundMessage.getMessageAnnotations().get("arrayString");
          assertThat(arrayString).containsExactly("1", "2", "3");
          int[] arrayInt;
          // QPid codec returns int[] and SwiftMQ codec returns Integer[]
          if (inboundMessage.getMessageAnnotations().get("arrayInt") instanceof Integer[]) {
            arrayInt =
                Arrays.stream((Integer[]) inboundMessage.getMessageAnnotations().get("arrayInt"))
                    .mapToInt(Integer::intValue)
                    .toArray();
          } else {
            arrayInt = (int[]) inboundMessage.getMessageAnnotations().get("arrayInt");
          }
          assertThat(arrayInt).containsExactly(200, 201, 202);
        });
  }

  @ParameterizedTest
  @MethodSource("codecsCouples")
  void codecsLargeComposites(CodecCouple codecCouple) {
    Codec serializer = codecCouple.serializer;
    Codec deserializer = codecCouple.deserializer;

    MessageBuilder messageBuilder = codecCouple.messageBuilderSupplier.get();

    List<String> largeList =
        IntStream.range(0, 30).mapToObj(i -> "list-element-" + i).collect(Collectors.toList());
    Map<String, Object> largeMap = new LinkedHashMap<>();
    for (int i = 0; i < 30; i++) {
      largeMap.put("map-key-" + i, "map-value-" + i);
    }
    String[] largeStringArray =
        IntStream.range(0, 30).mapToObj(i -> "arr-element-" + i).toArray(String[]::new);
    Integer[] largeIntArray =
        IntStream.range(0, 30).mapToObj(i -> 1000 + i).toArray(Integer[]::new);

    MessageBuilder.MessageAnnotationsBuilder annotations = messageBuilder.messageAnnotations();
    annotations
        .entry("large-list", largeList)
        .entry("large-map", largeMap)
        .entryArray("large-string-array", largeStringArray)
        .entryArray("large-int-array", largeIntArray);
    for (int i = 0; i < 30; i++) {
      annotations.entry("annotation-key-" + i, "annotation-value-" + i);
    }

    MessageBuilder.ApplicationPropertiesBuilder appProps =
        annotations.messageBuilder().applicationProperties();
    for (int i = 0; i < 30; i++) {
      appProps.entry("app-prop-key-" + i, "app-prop-value-" + i);
    }

    Message outboundMessage =
        appProps
            .messageBuilder()
            .properties()
            .messageId("large-composites-test")
            .to("destination")
            .subject("subject")
            .contentType("application/octet-stream")
            .messageBuilder()
            .addData("large composites body".getBytes(CHARSET))
            .build();

    Codec.EncodedMessage encoded = serializer.encode(outboundMessage);
    ByteBuf encodedData = encodedMessageByteBuf(encoded);
    Message inboundMessage = deserializer.decode(encodedData, encodedData.readableBytes());

    assertThat(new String(inboundMessage.getBodyAsBinary(), CHARSET))
        .isEqualTo("large composites body");
    assertThat(inboundMessage.getProperties().getMessageIdAsString())
        .isEqualTo("large-composites-test");
    assertThat(inboundMessage.getProperties().getTo()).isEqualTo("destination");
    assertThat(inboundMessage.getProperties().getSubject()).isEqualTo("subject");
    assertThat(inboundMessage.getProperties().getContentType())
        .isEqualTo("application/octet-stream");

    assertThat(inboundMessage.getApplicationProperties()).hasSize(30);
    for (int i = 0; i < 30; i++) {
      assertThat(inboundMessage.getApplicationProperties().get("app-prop-key-" + i))
          .isEqualTo("app-prop-value-" + i);
    }

    assertThat(inboundMessage.getMessageAnnotations()).hasSizeGreaterThanOrEqualTo(30 + 4);
    for (int i = 0; i < 30; i++) {
      assertThat(inboundMessage.getMessageAnnotations().get("annotation-key-" + i))
          .isEqualTo("annotation-value-" + i);
    }

    List<?> decodedList = (List<?>) inboundMessage.getMessageAnnotations().get("large-list");
    assertThat(decodedList).hasSize(30);
    assertThat(decodedList.get(0)).isEqualTo("list-element-0");
    assertThat(decodedList.get(29)).isEqualTo("list-element-29");

    Map<?, ?> decodedMap = (Map<?, ?>) inboundMessage.getMessageAnnotations().get("large-map");
    assertThat(decodedMap).hasSize(30);
    assertThat(decodedMap.get("map-key-0")).isEqualTo("map-value-0");
    assertThat(decodedMap.get("map-key-29")).isEqualTo("map-value-29");

    Object[] decodedStringArray =
        (Object[]) inboundMessage.getMessageAnnotations().get("large-string-array");
    assertThat(decodedStringArray).hasSize(30);
    assertThat(decodedStringArray[0]).isEqualTo("arr-element-0");
    assertThat(decodedStringArray[29]).isEqualTo("arr-element-29");

    int[] decodedIntArray;
    Object rawIntArray = inboundMessage.getMessageAnnotations().get("large-int-array");
    if (rawIntArray instanceof Integer[]) {
      decodedIntArray =
          Arrays.stream((Integer[]) rawIntArray).mapToInt(Integer::intValue).toArray();
    } else {
      decodedIntArray = (int[]) rawIntArray;
    }
    assertThat(decodedIntArray).hasSize(30);
    assertThat(decodedIntArray[0]).isEqualTo(1000);
    assertThat(decodedIntArray[29]).isEqualTo(1029);
  }

  @ParameterizedTest
  @MethodSource("codecsCouples")
  void codecsMultiByteUtf8(CodecCouple codecCouple) {
    Codec serializer = codecCouple.serializer;
    Codec deserializer = codecCouple.deserializer;

    // 2-byte UTF-8 chars at the STR8/STR32 boundary (128 * 2 = 256 bytes, forces STR32)
    String twoByteStr = "\u00E9".repeat(128);
    // 3-byte UTF-8 chars (CJK ideographs, 86 * 3 = 258 bytes)
    String threeByteStr = "\u4E00".repeat(86);
    // 4-byte UTF-8 chars (emoji, 64 * 4 = 256 bytes)
    String fourByteStr = new String(Character.toChars(0x1F600)).repeat(64);
    // Exactly 255 UTF-8 bytes: stays in STR8
    String str8Boundary = "\u00E9".repeat(127) + "x";

    MessageBuilder messageBuilder = codecCouple.messageBuilderSupplier.get();
    Message outboundMessage =
        messageBuilder
            .applicationProperties()
            .entry("two-byte", twoByteStr)
            .entry("three-byte", threeByteStr)
            .entry("four-byte", fourByteStr)
            .entry("str8-boundary", str8Boundary)
            .messageBuilder()
            .properties()
            .messageId(twoByteStr)
            .subject(threeByteStr)
            .messageBuilder()
            .addData("body".getBytes(CHARSET))
            .build();

    Codec.EncodedMessage encoded = serializer.encode(outboundMessage);
    ByteBuf encodedData = encodedMessageByteBuf(encoded);
    Message inboundMessage = deserializer.decode(encodedData, encodedData.readableBytes());

    assertThat(inboundMessage.getApplicationProperties().get("two-byte")).isEqualTo(twoByteStr);
    assertThat(inboundMessage.getApplicationProperties().get("three-byte")).isEqualTo(threeByteStr);
    assertThat(inboundMessage.getApplicationProperties().get("four-byte")).isEqualTo(fourByteStr);
    assertThat(inboundMessage.getApplicationProperties().get("str8-boundary"))
        .isEqualTo(str8Boundary);
    assertThat(inboundMessage.getProperties().getMessageIdAsString()).isEqualTo(twoByteStr);
    assertThat(inboundMessage.getProperties().getSubject()).isEqualTo(threeByteStr);
    assertThat(new String(inboundMessage.getBodyAsBinary(), CHARSET)).isEqualTo("body");
  }

  @ParameterizedTest
  @MethodSource
  void readCreatedMessage(Codec codec) {
    // same conversion logic as for encoding/decoding, so not testing all types
    Message message =
        codec
            .messageBuilder()
            .addData("hello".getBytes(CHARSET))
            .properties()
            .messageId(42)
            .messageBuilder()
            .applicationProperties()
            .entry("property1", "value1")
            .messageBuilder()
            .messageAnnotations()
            .entry("annotation1", "value1")
            .messageBuilder()
            .build();

    assertThat(message.getBodyAsBinary()).isEqualTo("hello".getBytes(CHARSET));
    assertThat(message.getBody()).isNotNull();
    assertThat(message.getProperties().getMessageIdAsLong()).isEqualTo(42);
    assertThat(message.getApplicationProperties())
        .hasSize(1)
        .containsKey("property1")
        .containsValue("value1");
    assertThat(message.getMessageAnnotations())
        .hasSize(1)
        .containsKey("annotation1")
        .containsValue("value1");
  }

  @ParameterizedTest
  @MethodSource("messageBuilderSuppliers")
  void notSupportedTypes(Supplier<MessageBuilder> messageBuilderSupplier) {
    Stream.of(
            (ThrowableAssert.ThrowingCallable)
                () ->
                    messageBuilderSupplier
                        .get()
                        .applicationProperties()
                        .entryDecimal32("", BigDecimal.ONE),
            () ->
                messageBuilderSupplier
                    .get()
                    .applicationProperties()
                    .entryDecimal64("", BigDecimal.ONE),
            () ->
                messageBuilderSupplier
                    .get()
                    .applicationProperties()
                    .entryDecimal128("", BigDecimal.ONE))
        .forEach(
            action -> assertThatThrownBy(action).isInstanceOf(UnsupportedOperationException.class));
  }

  @ParameterizedTest
  @MethodSource("allAmqpCodecs")
  void supportAmqpValueBody(Codec codec) {
    Function<Object, Message> encodeDecode =
        content -> {
          org.apache.qpid.proton.message.Message nativeMessage =
              org.apache.qpid.proton.message.Message.Factory.create();
          nativeMessage.setBody(new AmqpValue(content));
          QpidProtonAmqpMessageWrapper wrapper =
              new QpidProtonAmqpMessageWrapper(true, 1L, nativeMessage);
          EncodedMessage encoded = new QpidProtonCodec().encode(wrapper);
          ByteBuf buf = encodedMessageByteBuf(encoded);
          return codec.decode(buf, buf.readableBytes());
        };

    Message m1 = encodeDecode.apply("hello".getBytes(StandardCharsets.UTF_8));
    assertThat(m1.getBodyAsBinary()).asString(StandardCharsets.UTF_8).isEqualTo("hello");

    Message m2 = encodeDecode.apply("a string is not an array of byte");
    assertThatThrownBy(() -> m2.getBodyAsBinary()).isInstanceOf(IllegalStateException.class);
  }

  @ParameterizedTest
  @MethodSource("messageBuilders")
  void publishingIdShouldBeSetOnMessageIfSetOnMessageBuilder(MessageBuilder builder) {
    Message message = builder.publishingId(42).build();
    assertThat(message.hasPublishingId()).isTrue();
    assertThat(message.getPublishingId()).isEqualTo(42);
  }

  @ParameterizedTest
  @MethodSource("messageBuilders")
  void publishingIdShouldNotBeSetOnMessageIfNotSetOnMessageBuilder(MessageBuilder builder) {
    Message message = builder.build();
    assertThat(message.hasPublishingId()).isFalse();
    assertThat(message.getPublishingId()).isEqualTo(0);
  }

  @ParameterizedTest
  @MethodSource("codecsCombinations")
  void copy(CodecCouple codecCouple) {
    Codec serializer = codecCouple.serializer;
    Codec deserializer = codecCouple.deserializer;
    byte[] body = "hello".getBytes(StandardCharsets.UTF_8);

    Message message =
        serializer
            .messageBuilder()
            .addData(body)
            .messageAnnotations()
            .entry("foo", "bar")
            .messageBuilder()
            .build();
    Message copy = message.copy();

    message.annotate("original", "original value");
    copy.annotate("copy", "copy value");

    assertThat(message.getMessageAnnotations())
        .hasSize(2)
        .containsEntry("foo", "bar")
        .containsEntry("original", "original value");

    assertThat(copy.getMessageAnnotations())
        .hasSize(2)
        .containsEntry("foo", "bar")
        .containsEntry("copy", "copy value");

    UnaryOperator<Message> encodeDecode =
        msg -> {
          EncodedMessage encoded = serializer.encode(msg);
          ByteBuf buf = encodedMessageByteBuf(encoded);
          return deserializer.decode(buf, buf.readableBytes());
        };

    message = encodeDecode.apply(message);

    assertThat(message.getMessageAnnotations())
        .hasSize(2)
        .containsEntry("foo", "bar")
        .containsEntry("original", "original value");

    copy = encodeDecode.apply(copy);
    assertThat(copy.getMessageAnnotations())
        .hasSize(2)
        .containsEntry("foo", "bar")
        .containsEntry("copy", "copy value");
  }

  @Test
  void qpidDoesNotSupportPrimitiveArrayEncodingInMap() {
    org.apache.qpid.proton.message.Message message =
        org.apache.qpid.proton.message.Message.Factory.create();
    Map<Symbol, Object> map = new LinkedHashMap<>();
    map.put(Symbol.valueOf("foo"), new int[] {1, 2, 3});
    message.setMessageAnnotations(new MessageAnnotations(map));
    assertThatThrownBy(() -> qpidEncodeDecode(message)).isInstanceOf(ClassCastException.class);
  }

  @Test
  void qpidEncodeIntegerArrayDecodeIntArrayInMap() {
    org.apache.qpid.proton.message.Message message =
        org.apache.qpid.proton.message.Message.Factory.create();
    Map<Symbol, Object> map = new LinkedHashMap<>();
    map.put(Symbol.valueOf("foo"), new Integer[] {1, 2, 3});
    message.setMessageAnnotations(new MessageAnnotations(map));
    message = qpidEncodeDecode(message);
    map = message.getMessageAnnotations().getValue();
    assertThat(map.get(Symbol.valueOf("foo"))).isInstanceOf(int[].class);
  }

  private static org.apache.qpid.proton.message.Message qpidEncodeDecode(
      org.apache.qpid.proton.message.Message in) {
    QpidProtonCodec.ByteArrayWritableBuffer writableBuffer =
        new QpidProtonCodec.ByteArrayWritableBuffer(8192);
    in.encode(writableBuffer);

    org.apache.qpid.proton.message.Message out =
        org.apache.qpid.proton.message.Message.Factory.create();
    out.decode(writableBuffer.getArray(), 0, writableBuffer.getArrayLength());
    return out;
  }

  MessageTestConfiguration test(
      Function<MessageBuilder, MessageBuilder> messageOperation,
      Consumer<Message> messageExpectation) {
    return new MessageTestConfiguration(messageOperation, messageExpectation);
  }

  static class MessageTestConfiguration {

    final Function<MessageBuilder, MessageBuilder> messageOperation;
    final Consumer<Message> messageExpectation;

    MessageTestConfiguration(
        Function<MessageBuilder, MessageBuilder> messageOperation,
        Consumer<Message> messageExpectation) {
      this.messageOperation = messageOperation;
      this.messageExpectation = messageExpectation;
    }
  }

  static class CodecCouple {

    final Codec serializer;
    final Codec deserializer;
    final Supplier<MessageBuilder> messageBuilderSupplier;

    CodecCouple(
        Codec serializer, Codec deserializer, Supplier<MessageBuilder> messageBuilderSupplier) {
      this.serializer = serializer;
      this.deserializer = deserializer;
      this.messageBuilderSupplier = messageBuilderSupplier;
    }

    @Override
    public String toString() {
      return "serializer="
          + serializer.getClass().getSimpleName()
          + ", deserializer="
          + deserializer.getClass().getSimpleName()
          + ", messageBuilder="
          + messageBuilderSupplier.get().getClass().getSimpleName();
    }
  }
}
