// Copyright (c) 2020-2021 VMware, Inc. or its affiliates.  All rights reserved.
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

import java.time.Duration;

/** API to configure and create a {@link Consumer}. */
public interface ConsumerBuilder {

  /**
   * The stream to consume from.
   *
   * @param stream
   * @return this builder instance
   */
  ConsumerBuilder stream(String stream);

  /**
   * Set the consumer to consume from a super stream (partitioned stream).
   *
   * <p>This is an experimental API, subject to change.
   *
   * @param superStream
   * @return this builder instance
   */
  ConsumerBuilder superStream(String superStream);

  /**
   * The offset to start consuming from.
   *
   * <p>The default is {@link OffsetSpecification#next()} (the end of the stream).
   *
   * @param offsetSpecification
   * @return this builder instance
   */
  ConsumerBuilder offset(OffsetSpecification offsetSpecification);

  /**
   * The callback for inbound messages.
   *
   * @param messageHandler
   * @return this builder instance
   */
  ConsumerBuilder messageHandler(MessageHandler messageHandler);

  /**
   * The logical name of the {@link Consumer}.
   *
   * <p>Set a logical name to enable offset tracking.
   *
   * @param name
   * @return this builder instance
   */
  ConsumerBuilder name(String name);

  /**
   * Enable {@link ManualTrackingStrategy}.
   *
   * @return the manual tracking strategy
   */
  ManualTrackingStrategy manualTrackingStrategy();

  /**
   * Enable {@link AutoTrackingStrategy}.
   *
   * <p>This is the default tracking strategy.
   *
   * @return the auto-tracking strategy
   */
  AutoTrackingStrategy autoTrackingStrategy();

  /**
   * Create the configured {@link Consumer}
   *
   * @return the configured consumer
   */
  Consumer build();

  /** Manual tracking strategy. */
  interface ManualTrackingStrategy {

    /**
     * Interval to check if the last requested stored offset has been actually stored.
     *
     * <p>Default is 5 seconds.
     *
     * @param checkInterval
     * @return the manual tracking strategy
     */
    ManualTrackingStrategy checkInterval(Duration checkInterval);

    /**
     * Go back to the builder.
     *
     * @return the consumer builder
     */
    ConsumerBuilder builder();
  }

  /** Auto-tracking strategy. */
  interface AutoTrackingStrategy {

    /**
     * Number of messages before storing.
     *
     * <p>Default is 10,000.
     *
     * @param messageCountBeforeStorage
     * @return the auto-tracking strategy
     */
    AutoTrackingStrategy messageCountBeforeStorage(int messageCountBeforeStorage);

    /**
     * Interval to check and stored the last received offset in case of inactivity.
     *
     * <p>Default is 5 seconds.
     *
     * @param flushInterval
     * @return the auto-tracking strategy
     */
    AutoTrackingStrategy flushInterval(Duration flushInterval);

    /**
     * Go back to the builder.
     *
     * @return the consumer builder
     */
    ConsumerBuilder builder();
  }
}