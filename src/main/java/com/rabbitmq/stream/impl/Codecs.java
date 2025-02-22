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
package com.rabbitmq.stream.impl;

import com.rabbitmq.stream.Codec;
import com.rabbitmq.stream.StreamException;

final class Codecs {

  private Codecs() {}

  static final Codec DEFAULT;

  static {
    DEFAULT = instanciateDefault();
  }

  private static Codec instanciateDefault() {
    try {
      return (Codec)
          Class.forName("com.rabbitmq.stream.codec.QpidProtonCodec").getConstructor().newInstance();
    } catch (Exception e) {
      throw new StreamException("Error while creating QPid Proton codec", e);
    }
  }
}
