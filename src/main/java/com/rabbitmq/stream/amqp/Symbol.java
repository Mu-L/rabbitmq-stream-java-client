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
package com.rabbitmq.stream.amqp;

import java.util.concurrent.ConcurrentHashMap;

public final class Symbol {

  private static final int CACHE_MAX_SIZE = 2048;
  private static final ConcurrentHashMap<String, Symbol> CACHE = new ConcurrentHashMap<>(64);

  private final String value;

  private Symbol(String value) {
    this.value = value;
  }

  public static Symbol valueOf(String value) {
    if (value == null) {
      return null;
    }
    Symbol symbol = CACHE.get(value);
    if (symbol == null) {
      symbol = new Symbol(value);
      if (CACHE.size() < CACHE_MAX_SIZE) {
        Symbol existing = CACHE.putIfAbsent(value, symbol);
        if (existing != null) {
          symbol = existing;
        }
      }
    }
    return symbol;
  }

  @Override
  public String toString() {
    return value;
  }

  @Override
  public int hashCode() {
    return value.hashCode();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    return value.equals(((Symbol) o).value);
  }
}
