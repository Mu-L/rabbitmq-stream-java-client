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
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.netty.buffer.ByteBuf;
import java.io.IOException;
import java.io.OutputStream;

public final class ByteArrayEncodedMessage implements Codec.EncodedMessage {

  private final int size;
  private final byte[] data;

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  public ByteArrayEncodedMessage(int size, byte[] data) {
    this.size = size;
    this.data = data;
  }

  @Override
  public void writeTo(OutputStream outputStream) throws IOException {
    outputStream.write((this.size >>> 24) & 0xFF);
    outputStream.write((this.size >>> 16) & 0xFF);
    outputStream.write((this.size >>> 8) & 0xFF);
    outputStream.write((this.size >>> 0) & 0xFF);
    outputStream.write(this.data, 0, this.size);
  }

  @Override
  public void writeTo(ByteBuf buf) {
    buf.writeInt(this.size).writeBytes(this.data, 0, this.size);
  }

  @Override
  public int getSize() {
    return size;
  }
}
