// Copyright (c) 2020-2025 Broadcom. All Rights Reserved.
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
package com.rabbitmq.stream;

/** Indicate a checksum error for a chunk. */
public class ChunkChecksumValidationException extends StreamException {

  private final long expected;
  private final long computed;

  public ChunkChecksumValidationException(long expected, long computed) {
    super("Expecting " + expected + ", got " + computed);
    this.expected = expected;
    this.computed = computed;
  }

  public long getExpected() {
    return expected;
  }

  public long getComputed() {
    return computed;
  }
}
