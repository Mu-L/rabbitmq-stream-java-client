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

import java.util.Map;

/**
 * API for outbound and inbound messages.
 *
 * <p>A RabbitMQ stream message must comply to the AMQP 1.0 message format for the best
 * interoperability.
 *
 * <p>Please see section 3.2 "message format" of the AMQP 1.0 specification to find out about the
 * exact meaning of the message sections.
 *
 * <p>Messages instances are usually created with a {@link MessageBuilder}.
 */
public interface Message {

  /**
   * Does this message has a publishing ID?
   *
   * <p>Publishing IDs are used for deduplication of outbound messages. They are not persisted.
   *
   * @return true if the message has a publishing ID, false otherwise
   * @see ProducerBuilder#name(String)
   * @see <a
   *     href="https://rabbitmq.github.io/rabbitmq-stream-java-client/stable/htmlsingle/#outbound-message-deduplication">Deduplication
   *     documentation</a>
   */
  boolean hasPublishingId();

  /**
   * Get the publishing ID for the message.
   *
   * <p>Publishing IDs are used for deduplication of outbound messages. They are not persisted.
   *
   * @return the publishing ID of the message
   * @see ProducerBuilder#name(String)
   * @see <a
   *     href="https://rabbitmq.github.io/rabbitmq-stream-java-client/stable/htmlsingle/#outbound-message-deduplication">Deduplication
   *     documentation</a>
   */
  long getPublishingId();

  /**
   * Get the body of the message as an array of bytes.
   *
   * @return the message body
   */
  byte[] getBodyAsBinary();

  /**
   * Get the message body.
   *
   * <p>The actual type is defined by the underlying AMQP 1.0 codec.
   *
   * @return the message body
   */
  Object getBody();

  /**
   * Get the properties of the message.
   *
   * @return the properties of the message
   */
  Properties getProperties();

  /**
   * Get the application properties of the message.
   *
   * @return the application properties of the message
   */
  Map<String, Object> getApplicationProperties();

  /**
   * Get the message annotations of the message.
   *
   * <p>Message annotations are aimed at the infrastructure, use application properties for
   * application-specific key/value pairs.
   *
   * @return the message annotations
   */
  Map<String, Object> getMessageAnnotations();

  /**
   * Add a message annotation to the message.
   *
   * @param key the message annotation key
   * @param value the message annotation value
   * @return the modified message
   * @since 0.12.0
   */
  default Message annotate(String key, Object value) {
    this.getMessageAnnotations().put(key, value);
    return this;
  }

  /**
   * Create a copy of the message.
   *
   * <p>The message copy contains the exact same instances of the original bare message (body,
   * properties, application properties), only the message annotations are actually copied and can
   * be modified independently.
   *
   * @return the message copy
   * @since 0.12.0
   */
  default Message copy() {
    return this;
  }
}
