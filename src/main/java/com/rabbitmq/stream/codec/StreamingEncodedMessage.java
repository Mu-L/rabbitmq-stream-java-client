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
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import java.io.IOException;
import java.io.OutputStream;

/**
 * A {@link Codec.EncodedMessage} implementation that encodes a {@link Message} directly into AMQP
 * 1.0 format on write, without intermediate byte-array materialization.
 *
 * <p>When a message is wrapped in a {@code StreamingEncodedMessage}, its exact encoded size is
 * computed eagerly at construction time. The actual AMQP 1.0 byte encoding is then deferred until
 * {@link #writeTo(ByteBuf)} or {@link #writeTo(java.io.OutputStream)} is called, so the message is
 * serialized directly to the target buffer or stream. This avoids allocating a temporary byte array
 * to hold the full encoded form.
 *
 * <p>The supported message sections are message annotations, properties, application properties,
 * and body (data, amqp-value, or empty data if no body is set).
 *
 * <p><strong>This class is experimental and subject to change in future releases.</strong>
 *
 * @see Amqp10
 * @see InternalCodec
 */
final class StreamingEncodedMessage implements Codec.EncodedMessage {

  private static final byte[] EMPTY_BODY = new byte[0];

  private final ByteBufAllocator allocator;
  private final Message message;
  private final int size;

  public StreamingEncodedMessage(Message message, ByteBufAllocator allocator) {
    this.message = message;
    this.size = Amqp10.calculateMessageSize(message);
    this.allocator = allocator;
  }

  @Override
  public int getSize() {
    return size;
  }

  @Override
  public void writeTo(ByteBuf buf) {
    buf.writeInt(this.size);
    // Direct encoding to ByteBuf without intermediate storage
    encodeMessageToBuf(buf, message);
  }

  @Override
  public void writeTo(OutputStream outputStream) throws IOException {
    // Write size prefix
    outputStream.write((size >>> 24) & 0xFF);
    outputStream.write((size >>> 16) & 0xFF);
    outputStream.write((size >>> 8) & 0xFF);
    outputStream.write((size >>> 0) & 0xFF);

    // Encode directly to OutputStream via heap ByteBuf with exact size
    ByteBuf tempBuf = this.allocator.heapBuffer(size, size);
    try {
      encodeMessageToBuf(tempBuf, message);
      // Direct access to underlying array for optimal performance
      if (tempBuf.hasArray() && tempBuf.arrayOffset() == 0 && tempBuf.readerIndex() == 0) {
        outputStream.write(tempBuf.array(), 0, tempBuf.readableBytes());
      } else {
        byte[] data = new byte[tempBuf.readableBytes()];
        tempBuf.readBytes(data);
        outputStream.write(data);
      }
    } finally {
      tempBuf.release();
    }
  }

  private static void encodeMessageToBuf(ByteBuf buf, Message message) {
    if (message.getMessageAnnotations() != null && !message.getMessageAnnotations().isEmpty()) {
      Amqp10.writeMessageAnnotations(buf, message.getMessageAnnotations());
    }

    if (message.getProperties() != null) {
      Amqp10.writeProperties(buf, message.getProperties());
    }

    if (message.getApplicationProperties() != null
        && !message.getApplicationProperties().isEmpty()) {
      Amqp10.writeApplicationProperties(buf, message.getApplicationProperties());
    }

    // Body encoding - mirrors InternalCodec.encode logic exactly
    byte[] bodyBinary = null;
    try {
      bodyBinary = message.getBodyAsBinary();
    } catch (IllegalStateException e) {
      // non-binary body (e.g. AmqpValue with a String), handled below
    }

    if (bodyBinary != null) {
      Amqp10.writeData(buf, bodyBinary);
    } else {
      Object body = message.getBody();
      if (body != null) {
        Amqp10.writeDescriptor(buf, Amqp10.AMQP_VALUE_DESCRIPTOR);
        Amqp10.writeObject(buf, body);
      } else {
        Amqp10.writeData(buf, EMPTY_BODY);
      }
    }
  }
}
