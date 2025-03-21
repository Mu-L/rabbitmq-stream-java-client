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

import static com.rabbitmq.stream.impl.Tuples.pair;
import static com.rabbitmq.stream.impl.Utils.*;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

import com.rabbitmq.stream.BackOffDelayPolicy;
import com.rabbitmq.stream.Constants;
import com.rabbitmq.stream.StreamDoesNotExistException;
import com.rabbitmq.stream.StreamException;
import com.rabbitmq.stream.StreamNotAvailableException;
import com.rabbitmq.stream.impl.Client.Broker;
import com.rabbitmq.stream.impl.Client.ClientParameters;
import com.rabbitmq.stream.impl.Client.MetadataListener;
import com.rabbitmq.stream.impl.Client.PublishConfirmListener;
import com.rabbitmq.stream.impl.Client.PublishErrorListener;
import com.rabbitmq.stream.impl.Client.Response;
import com.rabbitmq.stream.impl.Client.ShutdownListener;
import com.rabbitmq.stream.impl.Tuples.Pair;
import com.rabbitmq.stream.impl.Utils.ClientConnectionType;
import com.rabbitmq.stream.impl.Utils.ClientFactory;
import com.rabbitmq.stream.impl.Utils.ClientFactoryContext;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class ProducersCoordinator implements AutoCloseable {

  static final int MAX_PRODUCERS_PER_CLIENT = 256;
  static final int MAX_TRACKING_CONSUMERS_PER_CLIENT = 50;
  private static final boolean DEBUG = false;
  private static final Logger LOGGER = LoggerFactory.getLogger(ProducersCoordinator.class);
  private final StreamEnvironment environment;
  private final ClientFactory clientFactory;
  private final int maxProducersByClient, maxTrackingConsumersByClient;
  private final Function<ClientConnectionType, String> connectionNamingStrategy;
  private final AtomicLong managerIdSequence = new AtomicLong(0);
  private final NavigableSet<ClientProducersManager> managers = new ConcurrentSkipListSet<>();
  private final AtomicLong trackerIdSequence = new AtomicLong(0);
  private final List<ProducerTracker> producerTrackers = new CopyOnWriteArrayList<>();
  private final ExecutorServiceFactory executorServiceFactory =
      new DefaultExecutorServiceFactory(
          AVAILABLE_PROCESSORS, 10, "rabbitmq-stream-producer-connection-");
  private final Lock coordinatorLock = new ReentrantLock();
  private final boolean forceLeader;

  ProducersCoordinator(
      StreamEnvironment environment,
      int maxProducersByClient,
      int maxTrackingConsumersByClient,
      Function<ClientConnectionType, String> connectionNamingStrategy,
      ClientFactory clientFactory,
      boolean forceLeader) {
    this.environment = environment;
    this.clientFactory = clientFactory;
    this.maxProducersByClient = maxProducersByClient;
    this.maxTrackingConsumersByClient = maxTrackingConsumersByClient;
    this.connectionNamingStrategy = connectionNamingStrategy;
    this.forceLeader = forceLeader;
  }

  Runnable registerProducer(StreamProducer producer, String reference, String stream) {
    return lock(
        this.coordinatorLock,
        () -> {
          ProducerTracker tracker =
              new ProducerTracker(trackerIdSequence.getAndIncrement(), reference, stream, producer);
          if (DEBUG) {
            this.producerTrackers.add(tracker);
          }
          return registerAgentTracker(tracker, stream);
        });
  }

  Runnable registerTrackingConsumer(StreamConsumer consumer) {
    return lock(
        this.coordinatorLock,
        () ->
            registerAgentTracker(
                new TrackingConsumerTracker(
                    trackerIdSequence.getAndIncrement(), consumer.stream(), consumer),
                consumer.stream()));
  }

  private Runnable registerAgentTracker(AgentTracker tracker, String stream) {
    List<BrokerWrapper> candidates = findCandidateNodes(stream, this.forceLeader);
    Broker broker = pickBroker(candidates);

    addToManager(broker, candidates, tracker);

    if (DEBUG) {
      return () -> {
        if (tracker instanceof ProducerTracker) {
          try {
            this.producerTrackers.remove(tracker);
          } catch (Exception e) {
            LOGGER.debug("Error while removing producer tracker from list");
          }
        }
        tracker.cancel();
      };
    } else {
      return tracker::cancel;
    }
  }

  private void addToManager(Broker node, List<BrokerWrapper> candidates, AgentTracker tracker) {
    ClientParameters clientParameters =
        environment
            .clientParametersCopy()
            .host(node.getHost())
            .port(node.getPort())
            .executorServiceFactory(this.executorServiceFactory)
            .dispatchingExecutorServiceFactory(Utils.NO_OP_EXECUTOR_SERVICE_FACTORY);
    ClientProducersManager pickedManager = null;
    while (pickedManager == null) {
      Iterator<ClientProducersManager> iterator = this.managers.iterator();
      while (iterator.hasNext()) {
        pickedManager = iterator.next();
        if (pickedManager.isClosed()) {
          iterator.remove();
          pickedManager = null;
        } else {
          if (node.equals(pickedManager.node) && !pickedManager.isFullFor(tracker)) {
            // let's try this one
            break;
          } else {
            pickedManager = null;
          }
        }
      }
      if (pickedManager == null) {
        String name = keyForNode(node);
        LOGGER.debug("Trying to create producer manager on {}", name);
        pickedManager =
            new ClientProducersManager(node, candidates, this.clientFactory, clientParameters);
        LOGGER.debug("Created producer manager on {}, id {}", name, pickedManager.id);
      }
      try {
        pickedManager.register(tracker);
        LOGGER.debug(
            "Assigned {} tracker {} (stream '{}') to manager {} (node {}), publisher ID {}",
            tracker.type(),
            tracker.uniqueId(),
            tracker.stream(),
            pickedManager.id,
            pickedManager.name,
            tracker.identifiable() ? tracker.id() : "N/A");
        this.managers.add(pickedManager);
      } catch (IllegalStateException e) {
        pickedManager = null;
      } catch (ConnectionStreamException | ClientClosedException | StreamNotAvailableException e) {
        // manager connection is dead or stream not available
        // scheduling manager closing if necessary in another thread to avoid blocking this one
        if (pickedManager.isEmpty()) {
          ClientProducersManager manager = pickedManager;
          this.environment.execute(
              () -> {
                manager.closeIfEmpty();
              },
              "Producer manager closing after timeout, producer %d on stream '%s'",
              tracker.uniqueId(),
              tracker.stream());
        }
        throw e;
      } catch (RuntimeException e) {
        if (pickedManager != null) {
          pickedManager.closeIfEmpty();
        }
        throw e;
      }
    }
  }

  // package protected for testing
  List<BrokerWrapper> findCandidateNodes(String stream, boolean forceLeader) {
    Map<String, Client.StreamMetadata> metadata =
        this.environment.locatorOperation(
            namedFunction(c -> c.metadata(stream), "Candidate lookup to publish to '%s'", stream));
    if (metadata.isEmpty() || metadata.get(stream) == null) {
      throw new StreamDoesNotExistException(stream);
    }

    Client.StreamMetadata streamMetadata = metadata.get(stream);
    if (!streamMetadata.isResponseOk()) {
      if (streamMetadata.getResponseCode() == Constants.RESPONSE_CODE_STREAM_DOES_NOT_EXIST) {
        throw new StreamDoesNotExistException(stream);
      } else {
        throw new IllegalStateException(
            "Could not get stream metadata, response code: " + streamMetadata.getResponseCode());
      }
    }

    List<BrokerWrapper> candidates = new ArrayList<>();
    Client.Broker leader = streamMetadata.getLeader();
    if (leader == null) {
      if (forceLeader) {
        throw new IllegalStateException("Not leader available for stream " + stream);
      }
    } else {
      candidates.add(new BrokerWrapper(leader, true));
    }

    if (!forceLeader && streamMetadata.hasReplicas()) {
      candidates.addAll(
          streamMetadata.getReplicas().stream()
              .map(b -> new BrokerWrapper(b, false))
              .collect(toList()));
    }

    if (candidates.isEmpty()) {
      throw new IllegalStateException("No stream member available to publish for stream " + stream);
    } else {
      LOGGER.debug("Candidates to publish to {}: {}", stream, candidates);
    }

    return List.copyOf(candidates);
  }

  static Broker pickBroker(List<BrokerWrapper> candidates) {
    return candidates.stream()
        .filter(BrokerWrapper::isLeader)
        .findFirst()
        .map(BrokerWrapper::broker)
        .orElseThrow(() -> new IllegalStateException("Not leader available"));
  }

  public void close() {
    Iterator<ClientProducersManager> iterator = this.managers.iterator();
    while (iterator.hasNext()) {
      ClientProducersManager manager = iterator.next();
      try {
        iterator.remove();
        manager.close();
      } catch (Exception e) {
        LOGGER.info(
            "Error while closing manager {} connected to node {}: {}",
            manager.id,
            manager.name,
            e.getMessage());
      }
    }
    try {
      this.executorServiceFactory.close();
    } catch (Exception e) {
      LOGGER.info("Error while closing executor service factory: {}", e.getMessage());
    }
  }

  int clientCount() {
    return this.managers.size();
  }

  int nodesConnected() {
    return this.managers.stream().map(m -> m.name).collect(toSet()).size();
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder("{");
    builder.append(jsonField("client_count", this.managers.size())).append(",");
    builder
        .append(
            jsonField(
                "producer_count", this.managers.stream().mapToInt(m -> m.producers.size()).sum()))
        .append(",");
    builder
        .append(
            jsonField(
                "tracking_consumer_count",
                this.managers.stream().mapToInt(m -> m.trackingConsumerTrackers.size()).sum()))
        .append(",");
    if (DEBUG) {
      builder.append(jsonField("producer_tracker_count", this.producerTrackers.size())).append(",");
    }
    builder.append(quote("clients")).append(" : [");
    builder.append(
        this.managers.stream()
            .map(
                m -> {
                  StringBuilder managerBuilder = new StringBuilder("{");
                  managerBuilder
                      .append(jsonField("id", m.id))
                      .append(",")
                      .append(jsonField("node", m.name))
                      .append(",")
                      .append(jsonField("producer_count", m.producers.size()))
                      .append(",")
                      .append(
                          jsonField("tracking_consumer_count", m.trackingConsumerTrackers.size()))
                      .append(",");
                  managerBuilder.append("\"producers\" : [");
                  managerBuilder.append(
                      m.producers.values().stream()
                          .map(
                              p -> {
                                StringBuilder producerBuilder = new StringBuilder("{");
                                producerBuilder.append(jsonField("stream", p.stream())).append(",");
                                producerBuilder.append(jsonField("producer_id", p.publisherId));
                                return producerBuilder.append("}").toString();
                              })
                          .collect(Collectors.joining(",")));
                  managerBuilder.append("],");
                  managerBuilder.append("\"tracking_consumers\" : [");
                  managerBuilder.append(
                      m.trackingConsumerTrackers.stream()
                          .map(
                              t -> {
                                StringBuilder trackerBuilder = new StringBuilder("{");
                                trackerBuilder.append(jsonField("stream", t.stream()));
                                return trackerBuilder.append("}").toString();
                              })
                          .collect(Collectors.joining(",")));
                  managerBuilder.append("]");
                  return managerBuilder.append("}").toString();
                })
            .collect(Collectors.joining(",")));
    builder.append("]");
    if (DEBUG) {
      builder.append(",");
      builder.append("\"producer_trackers\" : [");
      builder.append(
          this.producerTrackers.stream()
              .map(
                  t -> {
                    StringBuilder b = new StringBuilder("{");
                    b.append(quote("stream")).append(":").append(quote(t.stream)).append(",");
                    b.append(quote("node")).append(":");
                    Client client = null;
                    ClientProducersManager manager = t.clientProducersManager;
                    if (manager != null) {
                      client = manager.client;
                    }
                    if (client == null) {
                      b.append("null");
                    } else {
                      b.append(quote(client.getHost() + ":" + client.getPort()));
                    }
                    return b.append("}").toString();
                  })
              .collect(Collectors.joining(",")));
      builder.append("]");
    }
    return builder.append("}").toString();
  }

  private interface AgentTracker {

    void assign(byte producerId, Client client, ClientProducersManager manager);

    boolean identifiable();

    byte id();

    void unavailable();

    void running();

    void cancel();

    void closeAfterStreamDeletion(short code);

    String stream();

    String reference();

    boolean isOpen();

    long uniqueId();

    String type();

    boolean markRecoveryInProgress();
  }

  private static class ProducerTracker implements AgentTracker {

    private final long uniqueId;
    private final String reference;
    private final String stream;
    private final StreamProducer producer;
    private volatile byte publisherId;
    private volatile ClientProducersManager clientProducersManager;
    private final AtomicBoolean recovering = new AtomicBoolean(false);
    private final Lock trackerLock = new ReentrantLock();

    private ProducerTracker(
        long uniqueId, String reference, String stream, StreamProducer producer) {
      this.uniqueId = uniqueId;
      this.reference = reference;
      this.stream = stream;
      this.producer = producer;
    }

    @Override
    public void assign(byte producerId, Client client, ClientProducersManager manager) {
      lock(
          this.trackerLock,
          () -> {
            this.publisherId = producerId;
            this.clientProducersManager = manager;
          });
      this.producer.setPublisherId(producerId);
      this.producer.setClient(client);
    }

    @Override
    public boolean identifiable() {
      return true;
    }

    @Override
    public byte id() {
      return this.publisherId;
    }

    @Override
    public String reference() {
      return this.reference;
    }

    @Override
    public String stream() {
      return this.stream;
    }

    @Override
    public void unavailable() {
      lock(this.trackerLock, () -> this.clientProducersManager = null);
      this.producer.unavailable();
    }

    @Override
    public void running() {
      this.producer.running();
      this.recovering.set(false);
    }

    @Override
    public void cancel() {
      lock(
          this.trackerLock,
          () -> {
            if (this.clientProducersManager != null) {
              this.clientProducersManager.unregister(this);
            }
          });
    }

    @Override
    public void closeAfterStreamDeletion(short code) {
      this.producer.closeAfterStreamDeletion(code);
    }

    @Override
    public boolean isOpen() {
      return producer.isOpen();
    }

    @Override
    public long uniqueId() {
      return this.uniqueId;
    }

    @Override
    public String type() {
      return "producer";
    }

    @Override
    public boolean markRecoveryInProgress() {
      return this.recovering.compareAndSet(false, true);
    }
  }

  private static class TrackingConsumerTracker implements AgentTracker {

    private final long uniqueId;
    private final String stream;
    private final StreamConsumer consumer;
    private volatile ClientProducersManager clientProducersManager;
    private final AtomicBoolean recovering = new AtomicBoolean(false);
    private final Lock trackerLock = new ReentrantLock();

    private TrackingConsumerTracker(long uniqueId, String stream, StreamConsumer consumer) {
      this.uniqueId = uniqueId;
      this.stream = stream;
      this.consumer = consumer;
    }

    @Override
    public void assign(byte producerId, Client client, ClientProducersManager manager) {
      lock(this.trackerLock, () -> this.clientProducersManager = manager);
      this.consumer.setTrackingClient(client);
    }

    @Override
    public boolean identifiable() {
      return false;
    }

    @Override
    public byte id() {
      throw new UnsupportedOperationException();
    }

    @Override
    public String reference() {
      throw new UnsupportedOperationException();
    }

    @Override
    public String stream() {
      return this.stream;
    }

    @Override
    public void unavailable() {
      lock(this.trackerLock, () -> this.clientProducersManager = null);
      this.consumer.unavailable();
    }

    @Override
    public void running() {
      this.consumer.running();
      this.recovering.set(false);
    }

    @Override
    public void cancel() {
      lock(this.trackerLock, () -> this.clientProducersManager.unregister(this));
    }

    @Override
    public void closeAfterStreamDeletion(short code) {
      // nothing to do here, the consumer will be closed by the consumers coordinator if
      // the stream has been deleted
    }

    @Override
    public boolean isOpen() {
      return this.consumer.isOpen();
    }

    @Override
    public long uniqueId() {
      return this.uniqueId;
    }

    @Override
    public String type() {
      return "tracking consumer";
    }

    @Override
    public boolean markRecoveryInProgress() {
      return this.recovering.compareAndSet(false, true);
    }
  }

  private class ClientProducersManager implements Comparable<ClientProducersManager> {

    private final long id;
    private final String name;
    private final Broker node;
    private final ConcurrentMap<Byte, ProducerTracker> producers =
        new ConcurrentHashMap<>(maxProducersByClient);
    private final AtomicInteger producerIndexSequence = new AtomicInteger(0);
    private final Set<AgentTracker> trackingConsumerTrackers =
        ConcurrentHashMap.newKeySet(maxTrackingConsumersByClient);
    private final Map<String, Set<AgentTracker>> streamToTrackers = new ConcurrentHashMap<>();
    private final Client client;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final Lock managerLock = new ReentrantLock();

    private ClientProducersManager(
        Broker targetNode,
        List<BrokerWrapper> candidates,
        ClientFactory cf,
        Client.ClientParameters clientParameters) {
      this.id = managerIdSequence.getAndIncrement();
      AtomicReference<String> nameReference = new AtomicReference<>();
      AtomicReference<Client> ref = new AtomicReference<>();
      AtomicBoolean clientInitializedInManager = new AtomicBoolean(false);
      PublishConfirmListener publishConfirmListener =
          (publisherId, publishingId) -> {
            ProducerTracker producerTracker = producers.get(publisherId);
            if (producerTracker == null) {
              LOGGER.info("Received publish confirm for unknown producer: {}", publisherId);
            } else {
              producerTracker.producer.confirm(publishingId);
            }
          };
      PublishErrorListener publishErrorListener =
          (publisherId, publishingId, errorCode) -> {
            ProducerTracker producerTracker = producers.get(publisherId);
            if (producerTracker == null) {
              LOGGER.info(
                  "Received publish error for unknown producer: {}, error code {}",
                  publisherId,
                  Utils.formatConstant(errorCode));
            } else {
              producerTracker.producer.error(publishingId, errorCode);
            }
          };
      ShutdownListener shutdownListener =
          shutdownContext -> {
            if (clientInitializedInManager.get()) {
              this.closed.set(true);
              managers.remove(this);
            }
            if (shutdownContext.isShutdownUnexpected()) {
              LOGGER.debug(
                  "Recovering {} producer(s) and {} tracking consumer(s) after unexpected connection termination",
                  producers.size(),
                  trackingConsumerTrackers.size());
              producers.forEach((publishingId, tracker) -> tracker.unavailable());
              trackingConsumerTrackers.forEach(AgentTracker::unavailable);
              // execute in thread pool to free the IO thread
              environment
                  .scheduledExecutorService()
                  .execute(
                      namedRunnable(
                          () -> {
                            if (Thread.currentThread().isInterrupted()) {
                              return;
                            }
                            streamToTrackers.forEach(
                                (stream, trackers) -> {
                                  if (!Thread.currentThread().isInterrupted()) {
                                    assignProducersToNewManagers(
                                        trackers, stream, environment.recoveryBackOffDelayPolicy());
                                  }
                                });
                          },
                          "Producer recovery after disconnection from %s",
                          nameReference.get()));
            }
          };
      MetadataListener metadataListener =
          (stream, code) -> {
            LOGGER.debug(
                "Received metadata notification for '{}', stream is likely to have become unavailable",
                stream);
            Set<AgentTracker> affectedTrackers;
            this.managerLock.lock();
            try {
              affectedTrackers = streamToTrackers.remove(stream);
              LOGGER.debug(
                  "Affected publishers and consumer trackers after metadata update: {}",
                  affectedTrackers == null ? 0 : affectedTrackers.size());
              if (affectedTrackers != null && !affectedTrackers.isEmpty()) {
                affectedTrackers.forEach(
                    tracker -> {
                      tracker.unavailable();
                      if (tracker.identifiable()) {
                        producers.remove(tracker.id());
                      } else {
                        trackingConsumerTrackers.remove(tracker);
                      }
                    });
              }
            } finally {
              this.managerLock.unlock();
            }
            if (affectedTrackers != null && !affectedTrackers.isEmpty()) {
              environment
                  .scheduledExecutorService()
                  .execute(
                      namedRunnable(
                          () -> {
                            if (Thread.currentThread().isInterrupted()) {
                              return;
                            }
                            // close manager if no more trackers for it
                            // needs to be done in another thread than the IO thread
                            closeIfEmpty();
                            assignProducersToNewManagers(
                                affectedTrackers,
                                stream,
                                environment.topologyUpdateBackOffDelayPolicy());
                          },
                          "Producer re-assignment after metadata update on stream '%s'",
                          stream));
            }
          };
      String connectionName = connectionNamingStrategy.apply(ClientConnectionType.PRODUCER);
      ClientFactoryContext connectionFactoryContext =
          new ClientFactoryContext(
              clientParameters
                  .publishConfirmListener(publishConfirmListener)
                  .publishErrorListener(publishErrorListener)
                  .shutdownListener(shutdownListener)
                  .metadataListener(metadataListener)
                  .clientProperty("connection_name", connectionName),
              keyForNode(targetNode),
              candidates.stream().map(BrokerWrapper::broker).collect(toList()));
      this.client = cf.client(connectionFactoryContext);
      this.node = Utils.brokerFromClient(this.client);
      this.name = keyForNode(this.node);
      nameReference.set(this.name);
      LOGGER.debug("Created producer connection '{}'", connectionName);
      clientInitializedInManager.set(true);
      ref.set(this.client);
    }

    private void assignProducersToNewManagers(
        Collection<AgentTracker> trackers, String stream, BackOffDelayPolicy delayPolicy) {
      AsyncRetry.asyncRetry(
              () -> {
                List<BrokerWrapper> candidates = findCandidateNodes(stream, forceLeader);
                return pair(pickBroker(candidates), candidates);
              })
          .description("Candidate lookup to publish to " + stream)
          .scheduler(environment.scheduledExecutorService())
          .retry(ex -> !(ex instanceof StreamDoesNotExistException))
          .delayPolicy(delayPolicy)
          .build()
          .thenAccept(
              brokerAndCandidates -> {
                Broker broker = brokerAndCandidates.v1();
                List<BrokerWrapper> candidates = brokerAndCandidates.v2();
                String key = keyForNode(broker);
                LOGGER.debug(
                    "Assigning {} producer(s) and consumer tracker(s) to {} (stream '{}')",
                    trackers.size(),
                    key,
                    stream);
                trackers.forEach(tracker -> maybeRecoverAgent(broker, candidates, tracker));
              })
          .exceptionally(
              ex -> {
                LOGGER.info(
                    "Error while re-assigning producers and consumer trackers, closing them: {}",
                    ex.getMessage());
                for (AgentTracker tracker : trackers) {
                  try {
                    short code;
                    if (ex instanceof StreamDoesNotExistException
                        || ex.getCause() instanceof StreamDoesNotExistException) {
                      code = Constants.RESPONSE_CODE_STREAM_DOES_NOT_EXIST;
                    } else {
                      code = Constants.RESPONSE_CODE_STREAM_NOT_AVAILABLE;
                    }
                    tracker.closeAfterStreamDeletion(code);
                  } catch (Exception e) {
                    LOGGER.debug("Error while closing producer: {}", e.getMessage());
                  }
                }
                return null;
              });
    }

    private void maybeRecoverAgent(
        Broker broker, List<BrokerWrapper> candidates, AgentTracker tracker) {
      if (tracker.markRecoveryInProgress()) {
        try {
          recoverAgent(broker, candidates, tracker);
        } catch (Exception e) {
          LOGGER.warn(
              "Error while recovering {} tracker {} (stream '{}'). Reason: {}",
              tracker.type(),
              tracker.uniqueId(),
              tracker.stream(),
              Utils.exceptionMessage(e));
        }
      } else {
        LOGGER.debug(
            "Not recovering {} (stream '{}'), recovery is already is progress",
            tracker.type(),
            tracker.stream());
      }
    }

    private void recoverAgent(Broker node, List<BrokerWrapper> candidates, AgentTracker tracker) {
      boolean reassignmentCompleted = false;
      while (!reassignmentCompleted) {
        try {
          if (tracker.isOpen()) {
            LOGGER.debug(
                "Using {} to resume {} to {}", node.label(), tracker.type(), tracker.stream());
            addToManager(node, candidates, tracker);
            tracker.running();
          } else {
            LOGGER.debug(
                "Not recovering {} (stream '{}') because it has been closed",
                tracker.type(),
                tracker.stream());
          }
          reassignmentCompleted = true;
        } catch (ConnectionStreamException
            | ClientClosedException
            | StreamNotAvailableException e) {
          LOGGER.debug(
              "{} re-assignment on stream {} (ID {}) timed out or connection closed or stream not available, "
                  + "refreshing candidate leader and retrying",
              tracker.type(),
              tracker.identifiable() ? tracker.id() : "N/A",
              tracker.stream());
          // maybe not a good candidate, let's refresh and retry for this one
          Pair<Broker, List<BrokerWrapper>> brokerAndCandidates =
              callAndMaybeRetry(
                  () -> {
                    List<BrokerWrapper> cs = findCandidateNodes(tracker.stream(), forceLeader);
                    return pair(pickBroker(cs), cs);
                  },
                  ex -> !(ex instanceof StreamDoesNotExistException),
                  environment.recoveryBackOffDelayPolicy(),
                  "Candidate lookup for %s on stream '%s'",
                  tracker.type(),
                  tracker.stream());
          node = brokerAndCandidates.v1();
          candidates = brokerAndCandidates.v2();
        } catch (Exception e) {
          LOGGER.warn(
              "Error while re-assigning {} (stream '{}')", tracker.type(), tracker.stream(), e);
          reassignmentCompleted = true;
        }
      }
    }

    private void register(AgentTracker tracker) {
      lock(
          this.managerLock,
          () -> {
            if (this.isFullFor(tracker)) {
              throw new IllegalStateException(
                  "Cannot add subscription tracker, the manager is full");
            }
            if (this.isClosed()) {
              throw new IllegalStateException(
                  "Cannot add subscription tracker, the manager is closed");
            }
            checkNotClosed();
            if (tracker.identifiable()) {
              ProducerTracker producerTracker = (ProducerTracker) tracker;
              int index = pickSlot(this.producers, producerTracker, this.producerIndexSequence);
              this.checkNotClosed();
              Response response =
                  callAndMaybeRetry(
                      () ->
                          this.client.declarePublisher(
                              (byte) index, tracker.reference(), tracker.stream()),
                      RETRY_ON_TIMEOUT,
                      "Declare publisher request for publisher %d on stream '%s'",
                      producerTracker.uniqueId(),
                      producerTracker.stream());
              if (response.isOk()) {
                tracker.assign((byte) index, this.client, this);
              } else {
                String message =
                    "Error while declaring publisher: "
                        + formatConstant(response.getResponseCode())
                        + ". Could not assign producer to client.";
                LOGGER.info(message);
                throw new StreamException(message, response.getResponseCode());
              }
              producers.put(tracker.id(), producerTracker);
            } else {
              tracker.assign((byte) 0, this.client, this);
              trackingConsumerTrackers.add(tracker);
            }
            streamToTrackers
                .computeIfAbsent(tracker.stream(), s -> ConcurrentHashMap.newKeySet())
                .add(tracker);
          });
    }

    private void unregister(AgentTracker tracker) {
      lock(
          this.managerLock,
          () -> {
            LOGGER.debug(
                "Unregistering {} {} from manager on {}",
                tracker.type(),
                tracker.uniqueId(),
                this.name);
            if (tracker.identifiable()) {
              producers.remove(tracker.id());
            } else {
              trackingConsumerTrackers.remove(tracker);
            }
            streamToTrackers.compute(
                tracker.stream(),
                (s, trackersForThisStream) -> {
                  if (s == null || trackersForThisStream == null) {
                    // should not happen
                    return null;
                  } else {
                    trackersForThisStream.remove(tracker);
                    return trackersForThisStream.isEmpty() ? null : trackersForThisStream;
                  }
                });
            closeIfEmpty();
          });
    }

    boolean isFullFor(AgentTracker tracker) {
      return lock(
          this.managerLock,
          () -> {
            if (tracker.identifiable()) {
              return producers.size() == maxProducersByClient;
            } else {
              return trackingConsumerTrackers.size() == maxTrackingConsumersByClient;
            }
          });
    }

    boolean isEmpty() {
      return lock(
          this.managerLock, () -> producers.isEmpty() && trackingConsumerTrackers.isEmpty());
    }

    private void checkNotClosed() {
      if (!this.client.isOpen()) {
        throw new ClientClosedException();
      }
    }

    boolean isClosed() {
      if (!this.client.isOpen()) {
        this.close();
      }
      return this.closed.get();
    }

    private void closeIfEmpty() {
      if (!closed.get()) {
        lock(
            this.managerLock,
            () -> {
              if (this.isEmpty()) {
                this.close();
              } else {
                LOGGER.debug("Not closing producer manager {} because it is not empty", this.id);
              }
            });
      }
    }

    private void close() {
      if (closed.compareAndSet(false, true)) {
        managers.remove(this);
        try {
          if (this.client.isOpen()) {
            this.client.close();
          }
        } catch (Exception e) {
          LOGGER.debug("Error while closing client producer connection: ", e.getMessage());
        }
      }
    }

    @Override
    public int compareTo(ClientProducersManager o) {
      return Long.compare(this.id, o.id);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      ClientProducersManager that = (ClientProducersManager) o;
      return id == that.id;
    }

    @Override
    public int hashCode() {
      return Objects.hash(id);
    }
  }

  private static final Predicate<Exception> RETRY_ON_TIMEOUT =
      e -> e instanceof TimeoutStreamException;

  private static class ClientClosedException extends StreamException {

    public ClientClosedException() {
      super("Client already closed");
    }
  }

  static <T> int pickSlot(ConcurrentMap<Byte, T> map, T tracker, AtomicInteger sequence) {
    int index = -1;
    T previousValue = tracker;
    while (previousValue != null) {
      index = Integer.remainderUnsigned(sequence.getAndIncrement(), MAX_PRODUCERS_PER_CLIENT);
      previousValue = map.putIfAbsent((byte) index, tracker);
    }
    return index;
  }
}
