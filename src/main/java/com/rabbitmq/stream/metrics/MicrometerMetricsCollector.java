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
package com.rabbitmq.stream.metrics;

import io.micrometer.core.instrument.*;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicLong;

public class MicrometerMetricsCollector implements MetricsCollector {

  private final AtomicLong connections;
  private final Counter publish;
  private final Counter publishConfirm;
  private final Counter publishError;
  private final Counter chunk;
  private final Counter consume;
  private final Counter writtenBytes;
  private final Counter readBytes;

  private final AtomicLong outstandingPublishConfirm;
  protected final DistributionSummary chunkSize;

  public MicrometerMetricsCollector(MeterRegistry registry) {
    this(registry, "rabbitmq.stream");
  }

  public MicrometerMetricsCollector(final MeterRegistry registry, final String prefix) {
    this(registry, prefix, Collections.emptyList());
  }

  public MicrometerMetricsCollector(
      final MeterRegistry registry, final String prefix, final String... tags) {
    this(registry, prefix, Tags.of(tags));
  }

  public MicrometerMetricsCollector(
      final MeterRegistry registry, final String prefix, final Iterable<Tag> tags) {
    this.connections = registry.gauge(prefix + ".connections", tags, new AtomicLong(0));
    this.publish = registry.counter(prefix + ".published", tags);
    this.publishConfirm = registry.counter(prefix + ".confirmed", tags);
    this.publishError = registry.counter(prefix + ".errored", tags);
    this.chunk = this.createChunkCounter(registry, prefix, tags);
    this.chunkSize = this.createChunkSizeDistributionSummary(registry, prefix, tags);
    this.consume = registry.counter(prefix + ".consumed", tags);
    this.writtenBytes = registry.counter(prefix + ".written_bytes", tags);
    this.readBytes = registry.counter(prefix + ".read_bytes", tags);
    this.outstandingPublishConfirm =
        registry.gauge(prefix + ".outstanding_publish_confirm", tags, new AtomicLong(0));
  }

  protected Counter createChunkCounter(MeterRegistry registry, String prefix, Iterable<Tag> tags) {
    return Counter.builder(prefix + ".chunk").tags(tags).register(registry);
  }

  protected DistributionSummary createChunkSizeDistributionSummary(
      MeterRegistry registry, String prefix, Iterable<Tag> tags) {
    return DistributionSummary.builder(prefix + ".chunk_size").tags(tags).register(registry);
  }

  @Override
  public void openConnection() {
    this.connections.incrementAndGet();
  }

  @Override
  public void closeConnection() {
    this.connections.decrementAndGet();
  }

  @Override
  public void publish(int count) {
    publish.increment(count);
    outstandingPublishConfirm.addAndGet(count);
  }

  @Override
  public void publishConfirm(int count) {
    publishConfirm.increment(count);
    outstandingPublishConfirm.addAndGet(-count);
  }

  @Override
  public void publishError(int count) {
    publishError.increment(count);
    outstandingPublishConfirm.addAndGet(-count);
  }

  @Override
  public void chunk(int entriesCount) {
    chunk.increment();
    chunkSize.record(entriesCount);
  }

  @Override
  public void consume(long count) {
    consume.increment(count);
  }

  @Override
  public void writtenBytes(int writtenBytes) {
    this.writtenBytes.increment(writtenBytes);
  }

  @Override
  public void readBytes(int readBytes) {
    this.readBytes.increment(readBytes);
  }
}
