// Copyright (c) 2020 VMware, Inc. or its affiliates.  All rights reserved.
//
// This software, the RabbitMQ Java client library, is dual-licensed under the
// Mozilla Public License 1.1 ("MPL"), and the Apache License version 2 ("ASL").
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

import com.rabbitmq.stream.sasl.*;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.flush.FlushConsolidationHandler;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.rabbitmq.stream.Constants.*;
import static java.util.concurrent.TimeUnit.SECONDS;

public class Client implements AutoCloseable {

    public static final int DEFAULT_PORT = 5555;

    private static final Duration RESPONSE_TIMEOUT = Duration.ofSeconds(10);

    private static final Logger LOGGER = LoggerFactory.getLogger(Client.class);

    private final ConfirmListener confirmListener;

    private final PublishErrorListener publishErrorListener;

    private final ChunkListener chunkListener;

    private final MessageListener messageListener;

    private final SubscriptionListener subscriptionListener;

    private final Codec codec;

    private final Channel channel;

    private final AtomicLong publishSequence = new AtomicLong(0);

    private final AtomicInteger correlationSequence = new AtomicInteger(0);

    private final ConcurrentMap<Integer, OutstandingRequest> outstandingRequests = new ConcurrentHashMap<>();

    private final List<SubscriptionOffset> subscriptionOffsets = new CopyOnWriteArrayList<>();

    private final Subscriptions subscriptions = new Subscriptions();

    private final ExecutorService executorService = Executors.newSingleThreadExecutor();

    private final SaslConfiguration saslConfiguration;

    private final CredentialsProvider credentialsProvider;

    private final TuneState tuneState;

    private final AtomicBoolean closing = new AtomicBoolean(false);

    private final AtomicBoolean nettyClosing = new AtomicBoolean(false);

    private final int maxFrameSize;

    private final int heartbeat;

    private final boolean frameSizeCopped;

    private final EventLoopGroup eventLoopGroup;

    private final ChunkChecksum chunkChecksum;

    private final String NETTY_HANDLER_FLUSH_CONSOLIDATION = FlushConsolidationHandler.class.getSimpleName();
    private final String NETTY_HANDLER_FRAME_DECODER = LengthFieldBasedFrameDecoder.class.getSimpleName();
    private final String NETTY_HANDLER_STREAM = StreamHandler.class.getSimpleName();
    private final String NETTY_HANDLER_IDLE_STATE = IdleStateHandler.class.getSimpleName();

    public Client() {
        this(new ClientParameters());
    }

    public Client(ClientParameters parameters) {
        this.confirmListener = parameters.confirmListener;
        this.publishErrorListener = parameters.publishErrorListener;
        this.chunkListener = parameters.chunkListener;
        this.messageListener = parameters.messageListener;
        this.subscriptionListener = parameters.subscriptionListener;
        this.codec = parameters.codec == null ? new QpidProtonCodec() : parameters.codec;
        this.saslConfiguration = parameters.saslConfiguration;
        this.credentialsProvider = parameters.credentialsProvider;
        this.chunkChecksum = parameters.chunkChecksum;

        EventLoopGroup eventLoopGroup;
        if (parameters.eventLoopGroup == null) {
            this.eventLoopGroup = new NioEventLoopGroup();
            eventLoopGroup = this.eventLoopGroup;
        } else {
            this.eventLoopGroup = null;
            eventLoopGroup = parameters.eventLoopGroup;
        }

        Bootstrap b = new Bootstrap();
        b.group(eventLoopGroup);
        b.channel(NioSocketChannel.class);
        b.option(ChannelOption.SO_KEEPALIVE, true);
        // is that the default?
        b.option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
        ChannelCustomizer channelCustomizer = parameters.channelCustomizer == null ? ch -> {
        } : parameters.channelCustomizer;
        b.handler(new ChannelInitializer<SocketChannel>() {
            @Override
            public void initChannel(SocketChannel ch) {
                ch.pipeline().addFirst(NETTY_HANDLER_FLUSH_CONSOLIDATION, new FlushConsolidationHandler(FlushConsolidationHandler.DEFAULT_EXPLICIT_FLUSH_AFTER_FLUSHES, true));
                ch.pipeline().addLast(NETTY_HANDLER_FRAME_DECODER, new LengthFieldBasedFrameDecoder(
                        Integer.MAX_VALUE, 0, 4, 0, 4));
                ch.pipeline().addLast(NETTY_HANDLER_STREAM, new StreamHandler());
                channelCustomizer.customize(ch);
            }
        });

        ChannelFuture f = null;
        try {
            f = b.connect(parameters.host, parameters.port).sync();
        } catch (InterruptedException e) {
            throw new ClientException(e);
        }

        this.channel = f.channel();
        this.tuneState = new TuneState(parameters.requestedMaxFrameSize, (int) parameters.requestedHeartbeat.getSeconds());
        authenticate();
        this.tuneState.await(Duration.ofSeconds(10));
        this.maxFrameSize = this.tuneState.getMaxFrameSize();
        this.frameSizeCopped = this.maxFrameSize > 0;
        this.heartbeat = this.tuneState.getHeartbeat();
        LOGGER.debug("Connection tuned with max frame size {} and heartbeat {}", this.maxFrameSize, this.heartbeat);
        open(parameters.virtualHost);
    }

    static void handleMetadataUpdate(ByteBuf bb, int frameSize, Subscriptions subscriptions, SubscriptionListener subscriptionListener) {
        int read = 2 + 2; // already read the command id and version
        short code = bb.readShort();
        read += 2;
        if (code == RESPONSE_CODE_STREAM_DELETED) {
            String stream = readString(bb);
            read += (2 + stream.length());
            LOGGER.debug("Stream {} has been deleted", stream);
            List<Integer> subscriptionIds = subscriptions.removeSubscriptionsFor(stream);
            for (Integer subscriptionId : subscriptionIds) {
                subscriptionListener.subscriptionCancelled(subscriptionId, stream, RESPONSE_CODE_STREAM_DELETED);
            }

        } else {
            throw new IllegalArgumentException("Unsupported metadata update code " + code);
        }
        if (read != frameSize) {
            throw new IllegalStateException("Read " + read + " bytes in frame, expecting " + frameSize);
        }
    }

    static void handleResponse(ByteBuf bb, int frameSize, ConcurrentMap<Integer, OutstandingRequest> outstandingRequests) {
        int read = 2 + 2; // already read the command id and version
        int correlationId = bb.readInt();
        read += 4;
        short responseCode = bb.readShort();
        read += 2;

        OutstandingRequest<Response> outstandingRequest = remove(outstandingRequests, correlationId, Response.class);
        if (outstandingRequest == null) {
            LOGGER.warn("Could not find outstanding request with correlation ID {}", correlationId);
        } else {
            Response response = new Response(responseCode);
            outstandingRequest.response.set(response);
            outstandingRequest.latch.countDown();
        }

        if (read != frameSize) {
            throw new IllegalStateException("Read " + read + " bytes in frame, expecting " + frameSize);
        }
    }

    static void handleSaslHandshakeResponse(ByteBuf bb, int frameSize, ConcurrentMap<Integer, OutstandingRequest> outstandingRequests) {
        int read = 2 + 2; // already read the command id and version
        int correlationId = bb.readInt();
        read += 4;

        short responseCode = bb.readShort();
        read += 2;
        if (responseCode != RESPONSE_CODE_OK) {
            if (read != frameSize) {
                bb.readBytes(new byte[frameSize - read]);
            }
            // FIXME: should we unlock the request and notify that there's something wrong?
            throw new ClientException("Unexpected response code for SASL handshake response: " + responseCode);
        }


        int mechanismsCount = bb.readInt();

        read += 4;
        List<String> mechanisms = new ArrayList<>(mechanismsCount);
        for (int i = 0; i < mechanismsCount; i++) {
            String mechanism = readString(bb);
            mechanisms.add(mechanism);
            read += 2 + mechanism.length();
        }

        OutstandingRequest<List<String>> outstandingRequest = remove(outstandingRequests, correlationId, new ParameterizedTypeReference<List<String>>() {
        });
        if (outstandingRequest == null) {
            LOGGER.warn("Could not find outstanding request with correlation ID {}", correlationId);
        } else {
            outstandingRequest.response.set(mechanisms);
            outstandingRequest.latch.countDown();
        }

        if (read != frameSize) {
            throw new IllegalStateException("Read " + read + " bytes in frame, expecting " + frameSize);
        }
    }

    static void handleSaslAuthenticateResponse(ByteBuf bb, int frameSize, ConcurrentMap<Integer, OutstandingRequest> outstandingRequests) {
        int read = 2 + 2; // already read the command id and version
        int correlationId = bb.readInt();
        read += 4;

        short responseCode = bb.readShort();
        read += 2;

        byte[] challenge;
        if (responseCode == RESPONSE_CODE_SASL_CHALLENGE) {
            int challengeSize = bb.readInt();
            read += 4;
            challenge = new byte[challengeSize];
            bb.readBytes(challenge);
            read += challenge.length;
        } else {
            challenge = null;
        }

        SaslAuthenticateResponse response = new SaslAuthenticateResponse(responseCode, challenge);

        OutstandingRequest<SaslAuthenticateResponse> outstandingRequest = remove(outstandingRequests, correlationId, SaslAuthenticateResponse.class);
        if (outstandingRequest == null) {
            LOGGER.warn("Could not find outstanding request with correlation ID {}", correlationId);
        } else {
            outstandingRequest.response.set(response);
            outstandingRequest.latch.countDown();
        }

        if (read != frameSize) {
            throw new IllegalStateException("Read " + read + " bytes in frame, expecting " + frameSize);
        }
    }

    private static int negotiatedMaxValue(int clientValue, int serverValue) {
        return (clientValue == 0 || serverValue == 0) ?
                Math.max(clientValue, serverValue) :
                Math.min(clientValue, serverValue);
    }

    @SuppressWarnings("unchecked")
    private static <T> OutstandingRequest<T> remove(ConcurrentMap<Integer, OutstandingRequest> outstandingRequests, int correlationId, ParameterizedTypeReference<T> type) {
        return (OutstandingRequest<T>) outstandingRequests.remove(correlationId);
    }

    @SuppressWarnings("unchecked")
    private static <T> OutstandingRequest<T> remove(ConcurrentMap<Integer, OutstandingRequest> outstandingRequests, int correlationId, Class<T> clazz) {
        return (OutstandingRequest<T>) outstandingRequests.remove(correlationId);
    }

    static void handleMetadata(ByteBuf bb, int frameSize, ConcurrentMap<Integer, OutstandingRequest> outstandingRequests) {
        int read = 2 + 2; // already read the command id and version
        int correlationId = bb.readInt();
        read += 4;

        Map<Short, Broker> brokers = new HashMap<>();
        int brokersCount = bb.readInt();
        read += 4;
        for (int i = 0; i < brokersCount; i++) {
            short brokerReference = bb.readShort();
            read += 2;
            String host = readString(bb);
            read += 2 + host.length();
            int port = bb.readInt();
            read += 4;
            brokers.put(brokerReference, new Broker(host, port));
        }

        int streamsCount = bb.readInt();
        Map<String, StreamMetadata> results = new LinkedHashMap<>(streamsCount);
        read += 4;
        for (int i = 0; i < streamsCount; i++) {
            String stream = readString(bb);
            read += 2 + stream.length();
            short responseCode = bb.readShort();
            read += 2;
            short leaderReference = bb.readShort();
            read += 2;
            int replicasCount = bb.readInt();
            read += 4;
            Collection<Broker> replicas;
            if (replicasCount == 0) {
                replicas = Collections.emptyList();
            } else {
                replicas = new ArrayList<>(replicasCount);
                for (int j = 0; j < replicasCount; j++) {
                    short replicaReference = bb.readShort();
                    read += 2;
                    replicas.add(brokers.get(replicaReference));
                }
            }
            StreamMetadata streamMetadata = new StreamMetadata(stream, responseCode, brokers.get(leaderReference), replicas);
            results.put(stream, streamMetadata);
        }

        OutstandingRequest<Map<String, StreamMetadata>> outstandingRequest = remove(outstandingRequests, correlationId,
                new ParameterizedTypeReference<Map<String, StreamMetadata>>() {
                });
        if (outstandingRequest == null) {
            LOGGER.warn("Could not find outstanding request with correlation ID {}", correlationId);
        } else {
            outstandingRequest.response.set(results);
            outstandingRequest.latch.countDown();
        }

        if (read != frameSize) {
            throw new IllegalStateException("Read " + read + " bytes in frame, expecting " + frameSize);
        }
    }

    private static String readString(ByteBuf bb) {
        short size = bb.readShort();
        byte[] bytes = new byte[size];
        bb.readBytes(bytes);
        String string = new String(bytes, StandardCharsets.UTF_8);
        return string;
    }

    static void handleDeliver(ByteBuf bb, Client client, ChunkListener chunkListener, MessageListener messageListener,
                              int frameSize, Codec codec, List<SubscriptionOffset> subscriptionOffsets, ChunkChecksum chunkChecksum) {
        int read = 2 + 2; // already read the command id and version
        int subscriptionId = bb.readInt();
        read += 4;
/*
%% <<
%%   Magic=5:4/unsigned,
%%   ProtoVersion:4/unsigned,
%%   NumEntries:16/unsigned, %% need some kind of limit on chunk sizes 64k is a good start
%%   NumRecords:32/unsigned, %% total including all sub batch entries
%%   Timestamp:64/signed, %% millisecond posix (ish) timestamp
%%   Epoch:64/unsigned,
%%   ChunkFirstOffset:64/unsigned,
%%   ChunkCrc:32/integer, %% CRC for the records portion of the data
%%   DataLength:32/unsigned, %% length until end of chunk
%%   [Entry]
%%   ...>>
 */
        // FIXME handle magic and version
        byte magicAndVersion = bb.readByte();
        read += 1;

        // FIXME handle unsigned
        int numEntries = bb.readUnsignedShort();
        read += 2;
        long numRecords = bb.readUnsignedInt();
        read += 4;
        long timestamp = bb.readLong();
        read += 8;
        long epoch = bb.readLong(); // unsigned long
        read += 8;
        long offset = bb.readLong(); // unsigned long
        read += 8;
        long crc = bb.readUnsignedInt();
        read += 4;
        long dataLength = bb.readUnsignedInt();
        read += 4;

        chunkListener.handle(client, subscriptionId, offset, numRecords, dataLength);

        long offsetLimit = -1;
        if (!subscriptionOffsets.isEmpty()) {
            Iterator<SubscriptionOffset> iterator = subscriptionOffsets.iterator();
            while (iterator.hasNext()) {
                SubscriptionOffset subscriptionOffset = iterator.next();
                if (subscriptionOffset.subscriptionId == subscriptionId) {
                    subscriptionOffsets.remove(subscriptionOffset);
                    offsetLimit = subscriptionOffset.offset;
                    break;
                }
            }
        }

        final boolean filter = offsetLimit != -1;

        // TODO handle exception in exception handler
        chunkChecksum.checksum(bb, dataLength, crc);

        byte[] data;
        while (numRecords != 0) {
/*
%%   Entry Format
%%   <<0=SimpleEntryType:1,
%%     Size:31/unsigned,
%%     Data:Size/binary>> |
%%
%%   <<1=SubBatchEntryType:1,
%%     CompressionType:3,
%%     Reserved:4,
%%     NumRecords:16/unsigned,
%%     Size:32/unsigned,
%%     Data:Size/binary>>
 */

            // FIXME deal with other type of entry than simple (first bit = 0)
            int typeAndSize = bb.readInt();
            read += 4;

            data = new byte[typeAndSize];
            bb.readBytes(data);
            read += typeAndSize;

            if (filter && Long.compareUnsigned(offset, offsetLimit) < 0) {
                // filter
            } else {
                Message message = codec.decode(data);
                messageListener.handle(subscriptionId, offset, message);
            }
            numRecords--;
            offset++; // works even for unsigned long
        }
        if (read != frameSize) {
            throw new IllegalStateException("Read " + read + " bytes in frame, expecting " + frameSize);
        }
    }

    static void handleConfirm(ByteBuf bb, ConfirmListener confirmListener, int frameSize) {
        int read = 4; // already read the command id and version
        int publishingIdCount = bb.readInt();
        read += 4;
        long publishingId = -1;
        while (publishingIdCount != 0) {
            publishingId = bb.readLong();
            read += 8;
            confirmListener.handle(publishingId);
            publishingIdCount--;
        }
        if (read != frameSize) {
            throw new IllegalStateException("Read " + read + " bytes in frame, expecting " + frameSize);
        }
    }

    static void handlePublishError(ByteBuf bb, PublishErrorListener publishErrorListener, int frameSize) {
        int read = 4; // already read the command id and version
        long publishingErrorCount = bb.readInt();
        read += 4;
        long publishingId = -1;
        short code = -1;
        while (publishingErrorCount != 0) {
            publishingId = bb.readLong();
            read += 8;
            code = bb.readShort();
            read += 2;
            publishErrorListener.handle(publishingId, code);
            publishingErrorCount--;
        }
        if (read != frameSize) {
            throw new IllegalStateException("Read " + read + " bytes in frame, expecting " + frameSize);
        }
    }

    private void handleHeartbeat(int frameSize) {
        // FIXME handle heartbeat
        LOGGER.debug("Received heartbeat frame");
        int read = 2 + 2; // already read the command id and version
        if (read != frameSize) {
            throw new IllegalStateException("Read " + read + " bytes in frame, expecting " + frameSize);
        }
    }

    private void handleTune(ByteBuf bb, int frameSize, ChannelHandlerContext ctx, TuneState tuneState) {
        int read = 2 + 2; // already read the command id and version
        int serverMaxFrameSize = bb.readInt();
        read += 4;
        int serverHeartbeat = bb.readInt();
        read += 4;

        int maxFrameSize = negotiatedMaxValue(tuneState.requestedMaxFrameSize, serverMaxFrameSize);
        int heartbeat = negotiatedMaxValue(tuneState.requestedHeartbeat, serverHeartbeat);

        int length = 2 + 2 + 4 + 4;
        ByteBuf byteBuf = allocateNoCheck(ctx.alloc(), length + 4);
        byteBuf.writeInt(length)
                .writeShort(COMMAND_TUNE).writeShort(VERSION_0)
                .writeInt(maxFrameSize).writeInt(heartbeat);
        ctx.writeAndFlush(byteBuf);

        tuneState.maxFrameSize(maxFrameSize).heartbeat(heartbeat);

        if (heartbeat > 0) {
            this.channel.pipeline().addBefore(
                    NETTY_HANDLER_FRAME_DECODER,
                    NETTY_HANDLER_IDLE_STATE,
                    new IdleStateHandler(heartbeat * 2, heartbeat, 0)
            );
        }

        tuneState.done();

        if (read != frameSize) {
            throw new IllegalStateException("Read " + read + " bytes in frame, expecting " + frameSize);
        }
    }

    private void handleClose(ByteBuf bb, int frameSize, ChannelHandlerContext ctx) {
        int read = 2 + 2; // already read the command id and version
        int correlationId = bb.readInt();
        read += 4;
        short closeCode = bb.readShort();
        read += 2;
        String closeReason = readString(bb);
        read += 2 + closeReason.length();

        LOGGER.info("Received close from server, reason: {} {}", closeCode, closeReason);

        int length = 2 + 2 + 4 + 2;
        ByteBuf byteBuf = allocate(ctx.alloc(), length + 4);
        byteBuf.writeInt(length)
                .writeShort(COMMAND_CLOSE).writeShort(VERSION_0)
                .writeInt(correlationId).writeShort(RESPONSE_CODE_OK);
        ctx.writeAndFlush(byteBuf);

        if (closing.compareAndSet(false, true)) {
            closeNetty();
        }

        if (read != frameSize) {
            throw new IllegalStateException("Read " + read + " bytes in frame, expecting " + frameSize);
        }
    }

    private void authenticate() {
        List<String> saslMechanisms = getSaslMechanisms();
        SaslMechanism saslMechanism = this.saslConfiguration.getSaslMechanism(saslMechanisms);

        byte[] challenge = null;
        boolean authDone = false;
        while (!authDone) {
            byte[] saslResponse = saslMechanism.handleChallenge(challenge, this.credentialsProvider);
            SaslAuthenticateResponse saslAuthenticateResponse = sendSaslAuthenticate(saslMechanism, saslResponse);
            if (saslAuthenticateResponse.isOk()) {
                authDone = true;
            } else if (saslAuthenticateResponse.isChallenge()) {
                challenge = saslAuthenticateResponse.challenge;
            } else if (saslAuthenticateResponse.isAuthenticationFailure()) {
                throw new AuthenticationFailureException("Unexpected response code during authentication: " + saslAuthenticateResponse.getResponseCode());
            } else {
                throw new ClientException("Unexpected response code during authentication: " + saslAuthenticateResponse.getResponseCode());
            }
        }
    }

    private SaslAuthenticateResponse sendSaslAuthenticate(SaslMechanism saslMechanism, byte[] challengeResponse) {
        int length = 2 + 2 + 4 + 2 + saslMechanism.getName().length() +
                4 + (challengeResponse == null ? 0 : challengeResponse.length);
        int correlationId = correlationSequence.incrementAndGet();
        try {
            ByteBuf bb = allocateNoCheck(length + 4);
            bb.writeInt(length);
            bb.writeShort(COMMAND_SASL_AUTHENTICATE);
            bb.writeShort(VERSION_0);
            bb.writeInt(correlationId);
            bb.writeShort(saslMechanism.getName().length());
            bb.writeBytes(saslMechanism.getName().getBytes(StandardCharsets.UTF_8));
            if (challengeResponse == null) {
                bb.writeInt(-1);
            } else {
                bb.writeInt(challengeResponse.length).writeBytes(challengeResponse);
            }
            OutstandingRequest<SaslAuthenticateResponse> request = new OutstandingRequest<>(RESPONSE_TIMEOUT);
            outstandingRequests.put(correlationId, request);
            channel.writeAndFlush(bb);
            request.block();
            return request.response.get();
        } catch (RuntimeException e) {
            outstandingRequests.remove(correlationId);
            throw new ClientException(e);
        }
    }

    private void open(String virtualHost) {
        int length = 2 + 2 + 4 + 2 + virtualHost.length();
        int correlationId = correlationSequence.incrementAndGet();
        try {
            ByteBuf bb = allocate(length + 4);
            bb.writeInt(length);
            bb.writeShort(COMMAND_OPEN);
            bb.writeShort(VERSION_0);
            bb.writeInt(correlationId);
            bb.writeShort(virtualHost.length());
            bb.writeBytes(virtualHost.getBytes(StandardCharsets.UTF_8));
            OutstandingRequest<Response> request = new OutstandingRequest<>(RESPONSE_TIMEOUT);
            outstandingRequests.put(correlationId, request);
            channel.writeAndFlush(bb);
            request.block();
            if (!request.response.get().isOk()) {
                throw new ClientException("Unexpected response code when connecting to virtual host: " + request.response.get().getResponseCode());
            }
        } catch (RuntimeException e) {
            outstandingRequests.remove(correlationId);
            throw new ClientException(e);
        }
    }

    // for testing
    void send(byte[] content) {
        ByteBuf bb = allocateNoCheck(content.length);
        bb.writeBytes(content);
        try {
            channel.writeAndFlush(bb).sync();
        } catch (InterruptedException e) {
            throw new ClientException(e);
        }
    }

    private void sendClose(short code, String reason) {
        int length = 2 + 2 + 4 + 2 + 2 + reason.length();
        int correlationId = correlationSequence.incrementAndGet();
        try {
            ByteBuf bb = allocate(length + 4);
            bb.writeInt(length);
            bb.writeShort(COMMAND_CLOSE);
            bb.writeShort(VERSION_0);
            bb.writeInt(correlationId);
            bb.writeShort(code);
            bb.writeShort(reason.length());
            bb.writeBytes(reason.getBytes(StandardCharsets.UTF_8));
            OutstandingRequest<Response> request = new OutstandingRequest<>(RESPONSE_TIMEOUT);
            outstandingRequests.put(correlationId, request);
            channel.writeAndFlush(bb);
            request.block();
            if (!request.response.get().isOk()) {
                LOGGER.warn("Unexpected response code when closing: {}", request.response.get().getResponseCode());
                throw new ClientException("Unexpected response code when closing: " + request.response.get().getResponseCode());
            }
        } catch (RuntimeException e) {
            outstandingRequests.remove(correlationId);
            throw new ClientException(e);
        }
    }

    private List<String> getSaslMechanisms() {
        int length = 2 + 2 + 4;
        int correlationId = correlationSequence.incrementAndGet();
        try {
            ByteBuf bb = allocateNoCheck(length + 4);
            bb.writeInt(length);
            bb.writeShort(COMMAND_SASL_HANDSHAKE);
            bb.writeShort(VERSION_0);
            bb.writeInt(correlationId);
            OutstandingRequest<List<String>> request = new OutstandingRequest<>(RESPONSE_TIMEOUT);
            outstandingRequests.put(correlationId, request);
            channel.writeAndFlush(bb);
            request.block();
            return request.response.get();
        } catch (RuntimeException e) {
            outstandingRequests.remove(correlationId);
            throw new ClientException(e);
        }
    }

    public Response create(String stream) {
        return create(stream, Collections.emptyMap());
    }

    public Response create(String stream, Map<String, String> arguments) {
        int length = 2 + 2 + 4 + 2 + stream.length() + 4;
        for (Map.Entry<String, String> argument : arguments.entrySet()) {
            length = length + 2 + argument.getKey().length() + 2 + argument.getValue().length();
        }
        int correlationId = correlationSequence.incrementAndGet();
        try {
            ByteBuf bb = allocate(length + 4);
            bb.writeInt(length);
            bb.writeShort(COMMAND_CREATE_STREAM);
            bb.writeShort(VERSION_0);
            bb.writeInt(correlationId);
            bb.writeShort(stream.length());
            bb.writeBytes(stream.getBytes(StandardCharsets.UTF_8));
            bb.writeInt(arguments.size());
            for (Map.Entry<String, String> argument : arguments.entrySet()) {
                bb.writeShort(argument.getKey().length());
                bb.writeBytes(argument.getKey().getBytes(StandardCharsets.UTF_8));
                bb.writeShort(argument.getValue().length());
                bb.writeBytes(argument.getValue().getBytes(StandardCharsets.UTF_8));
            }
            OutstandingRequest<Response> request = new OutstandingRequest<>(RESPONSE_TIMEOUT);
            outstandingRequests.put(correlationId, request);
            channel.writeAndFlush(bb);
            request.block();
            return request.response.get();
        } catch (RuntimeException e) {
            outstandingRequests.remove(correlationId);
            throw new ClientException(e);
        }
    }

    private ByteBuf allocate(ByteBufAllocator allocator, int capacity) {
        if (frameSizeCopped && capacity > this.maxFrameSize) {
            throw new IllegalArgumentException("Cannot allocate " + capacity + " bytes for outbound frame, limit is " + this.maxFrameSize);
        }
        return allocator.buffer(capacity);
    }

    private ByteBuf allocate(int capacity) {
        return allocate(channel.alloc(), capacity);
    }

    private ByteBuf allocateNoCheck(ByteBufAllocator allocator, int capacity) {
        return allocator.buffer(capacity);
    }

    private ByteBuf allocateNoCheck(int capacity) {
        return allocateNoCheck(channel.alloc(), capacity);
    }

    public Response delete(String stream) {
        int length = 2 + 2 + 4 + 2 + stream.length();
        int correlationId = correlationSequence.incrementAndGet();
        try {
            ByteBuf bb = allocate(length + 4);
            bb.writeInt(length);
            bb.writeShort(COMMAND_DELETE_STREAM);
            bb.writeShort(VERSION_0);
            bb.writeInt(correlationId);
            bb.writeShort(stream.length());
            bb.writeBytes(stream.getBytes(StandardCharsets.UTF_8));
            OutstandingRequest<Response> request = new OutstandingRequest<>(RESPONSE_TIMEOUT);
            outstandingRequests.put(correlationId, request);
            channel.writeAndFlush(bb);
            request.block();
            return request.response.get();
        } catch (RuntimeException e) {
            outstandingRequests.remove(correlationId);
            throw new ClientException(e);
        }
    }

    public Map<String, StreamMetadata> metadata(String... streams) {
        if (streams == null || streams.length == 0) {
            throw new IllegalArgumentException("At least one stream must be specified");
        }
        int length = 2 + 2 + 4 + 4; // API code, version, correlation ID, size of array
        for (String stream : streams) {
            length += 2;
            length += stream.length();
        }
        int correlationId = correlationSequence.incrementAndGet();
        try {
            ByteBuf bb = allocate(length + 4);
            bb.writeInt(length);
            bb.writeShort(COMMAND_METADATA);
            bb.writeShort(VERSION_0);
            bb.writeInt(correlationId);
            bb.writeInt(streams.length);
            for (String stream : streams) {
                bb.writeShort(stream.length());
                bb.writeBytes(stream.getBytes(StandardCharsets.UTF_8));
            }
            OutstandingRequest<Map<String, StreamMetadata>> request = new OutstandingRequest<>(RESPONSE_TIMEOUT);
            outstandingRequests.put(correlationId, request);
            channel.writeAndFlush(bb);
            request.block();
            return request.response.get();
        } catch (RuntimeException e) {
            outstandingRequests.remove(correlationId);
            throw new ClientException(e);
        }
    }

    public List<Long> publish(String stream, List<Message> messages) {
        List<Codec.EncodedMessage> encodedMessages = new ArrayList<>(messages.size());
        for (Message message : messages) {
            Codec.EncodedMessage encodedMessage = codec.encode(message);
            checkMessageFitsInFrame(stream, encodedMessage);
            encodedMessages.add(encodedMessage);
        }
        return publishInternal(this.channel, stream, encodedMessages);
    }

    public long publish(String stream, Message message) {
        Codec.EncodedMessage encodedMessage = codec.encode(message);
        checkMessageFitsInFrame(stream, encodedMessage);
        return publishInternal(this.channel, stream, Collections.singletonList(encodedMessage)).get(0);
    }

    public List<Long> publishBinary(String stream, List<byte[]> messages) {
        List<Codec.EncodedMessage> encodedMessages = new ArrayList<>(messages.size());
        for (byte[] message : messages) {
            Codec.EncodedMessage encodedMessage = codec.encode(new BinaryOnlyMessage(message));
            checkMessageFitsInFrame(stream, encodedMessage);
            encodedMessages.add(encodedMessage);
        }
        return publishInternal(this.channel, stream, encodedMessages);
    }

    private void checkMessageFitsInFrame(String stream, Codec.EncodedMessage encodedMessage) {
        int frameBeginning = 4 + 2 + 2 + 2 + stream.length() + 4 + 8 + 4 + encodedMessage.getSize();
        if (frameBeginning > this.maxFrameSize) {
            throw new IllegalArgumentException("Message too big to fit in one frame: " + encodedMessage.getSize());
        }
    }

    public long publish(String stream, byte[] data) {
        Codec.EncodedMessage encodedMessage = codec.encode(new BinaryOnlyMessage(data));
        checkMessageFitsInFrame(stream, encodedMessage);
        return publishInternal(this.channel, stream, Collections.singletonList(encodedMessage)).get(0);
    }

    List<Long> publishInternal(Channel ch, String stream, List<Codec.EncodedMessage> encodedMessages) {
        int frameHeaderLength = 2 + 2 + 2 + stream.length() + 4;

        List<Long> sequences = new ArrayList<>(encodedMessages.size());
        int length = frameHeaderLength;
        int currentIndex = 0;
        int startIndex = 0;
        for (Codec.EncodedMessage encodedMessage : encodedMessages) {
            length += (8 + 4 + encodedMessage.getSize()); // publish ID + message size
            if (length > this.maxFrameSize) {
                // the current message does not fit, we're sending the batch
                int frameLength = length - (12 + encodedMessage.getSize());
                sendMessageBatch(ch, frameLength, stream, startIndex, currentIndex, encodedMessages, sequences);
                length = frameHeaderLength + (8 + 4 + encodedMessage.getSize()); // publish ID + message size
                startIndex = currentIndex;
            }
            currentIndex++;
        }
        sendMessageBatch(ch, length, stream, startIndex, currentIndex, encodedMessages, sequences);

        return sequences;
    }

    private void sendMessageBatch(Channel ch, int frameLength, String stream, int fromIncluded, int toExcluded, List<Codec.EncodedMessage> messages, List<Long> sequences) {
        // no check because it's been done already
        ByteBuf out = allocateNoCheck(ch.alloc(), frameLength + 4);
        out.writeInt(frameLength);
        out.writeShort(COMMAND_PUBLISH);
        out.writeShort(VERSION_0);
        out.writeShort(stream.length());
        out.writeBytes(stream.getBytes(StandardCharsets.UTF_8));
        out.writeInt(toExcluded - fromIncluded);
        for (int i = fromIncluded; i < toExcluded; i++) {
            Codec.EncodedMessage messageToPublish = messages.get(i);
            long sequence = publishSequence.getAndIncrement();
            out.writeLong(sequence);
            out.writeInt(messageToPublish.getSize());
            out.writeBytes(messageToPublish.getData(), 0, messageToPublish.getSize());
            sequences.add(sequence);
        }
        ch.writeAndFlush(out);
    }

    public MessageBuilder messageBuilder() {
        return this.codec.messageBuilder();
    }

    public void credit(int subscriptionId, int credit) {
        if (credit < 0 || credit > Short.MAX_VALUE) {
            throw new IllegalArgumentException("Credit value must be between 0 and " + Short.MAX_VALUE);
        }
        int length = 2 + 2 + 4 + 2;

        ByteBuf bb = allocate(length + 4);
        bb.writeInt(length);
        bb.writeShort(COMMAND_CREDIT);
        bb.writeShort(VERSION_0);
        bb.writeInt(subscriptionId);
        bb.writeShort((short) credit);
        channel.writeAndFlush(bb);
    }

    /**
     * Subscribe to receive messages from a stream.
     * <p>
     * Note the offset is an unsigned long. Longs are signed in Java, but unsigned longs
     * can be used as long as some care is taken for some operations. See
     * the <code>unsigned*</code> static methods in {@link Long}.
     *
     * @param subscriptionId      identifier to correlate inbound messages to this subscription
     * @param stream              the stream to consume from
     * @param offsetSpecification the specification of the offset to consume from
     * @param credit              the initial number of credits
     * @return the subscription confirmation
     */
    public Response subscribe(int subscriptionId, String stream, OffsetSpecification offsetSpecification, int credit) {
        if (credit < 0 || credit > Short.MAX_VALUE) {
            throw new IllegalArgumentException("Credit value must be between 0 and " + Short.MAX_VALUE);
        }
        int length = 2 + 2 + 4 + 4 + 2 + stream.length() + 2 + 2; // misses the offset
        if (offsetSpecification.isOffset() || offsetSpecification.isTimestamp()) {
            length += 8;
        }
        int correlationId = correlationSequence.getAndIncrement();
        try {
            ByteBuf bb = allocate(length + 4);
            bb.writeInt(length);
            bb.writeShort(COMMAND_SUBSCRIBE);
            bb.writeShort(VERSION_0);
            bb.writeInt(correlationId);
            bb.writeInt(subscriptionId);
            bb.writeShort(stream.length());
            bb.writeBytes(stream.getBytes(StandardCharsets.UTF_8));
            bb.writeShort(offsetSpecification.getType());
            if (offsetSpecification.isOffset() || offsetSpecification.isTimestamp()) {
                bb.writeLong(offsetSpecification.getOffset());
            }
            bb.writeShort(credit);
            OutstandingRequest<Response> request = new OutstandingRequest<>(RESPONSE_TIMEOUT);
            outstandingRequests.put(correlationId, request);
            if (offsetSpecification.isOffset()) {
                subscriptionOffsets.add(new SubscriptionOffset(subscriptionId, offsetSpecification.getOffset()));
            }
            channel.writeAndFlush(bb);
            request.block();
            subscriptions.add(stream, subscriptionId);
            return request.response.get();
        } catch (RuntimeException e) {
            outstandingRequests.remove(correlationId);
            throw new ClientException(e);
        }
    }

    public Response unsubscribe(int subscriptionId) {
        int length = 2 + 2 + 4 + 4;
        int correlationId = correlationSequence.getAndIncrement();
        try {
            ByteBuf bb = allocate(length + 4);
            bb.writeInt(length);
            bb.writeShort(COMMAND_UNSUBSCRIBE);
            bb.writeShort(VERSION_0);
            bb.writeInt(correlationId);
            bb.writeInt(subscriptionId);
            OutstandingRequest<Response> request = new OutstandingRequest<>(RESPONSE_TIMEOUT);
            outstandingRequests.put(correlationId, request);
            channel.writeAndFlush(bb);
            request.block();
            subscriptions.remove(subscriptionId);
            return request.response.get();
        } catch (RuntimeException e) {
            outstandingRequests.remove(correlationId);
            throw new ClientException(e);
        }
    }

    public void close() {
        if (closing.compareAndSet(false, true)) {
            LOGGER.debug("Closing client");

            // FIXME unsubscribe current subscriptions?

            sendClose(RESPONSE_CODE_OK, "OK");

            closeNetty();

            if (this.executorService != null) {
                this.executorService.shutdownNow();
            }

            LOGGER.debug("Client closed");
        }
    }

    private void closeNetty() {
        if (this.nettyClosing.compareAndSet(false, true)) {
            try {
                if (this.channel.isOpen()) {
                    LOGGER.debug("Closing Netty channel");
                    this.channel.close().get(10, TimeUnit.SECONDS);
                }
                if (this.eventLoopGroup != null && (!this.eventLoopGroup.isShuttingDown() || !this.eventLoopGroup.isShutdown())) {
                    LOGGER.debug("Closing Netty event loop group");
                    this.eventLoopGroup.shutdownGracefully(1, 10, SECONDS).get(10, SECONDS);
                }
            } catch (InterruptedException e) {
                LOGGER.info("Channel closing has been interrupted");
                Thread.currentThread().interrupt();
            } catch (ExecutionException e) {
                LOGGER.info("Channel closing failed", e);
            } catch (TimeoutException e) {
                LOGGER.info("Could not close channel in 10 seconds");
            }
        }
    }

    public boolean isOpen() {
        return !closing.get();
    }

    public interface ConfirmListener {

        void handle(long publishingId);

    }

    public interface PublishErrorListener {

        void handle(long publishingId, short errorCode);

    }


    public interface SubscriptionListener {

        void subscriptionCancelled(int subscriptionId, String stream, short reason);

    }

    public interface ChunkListener {

        /**
         * Callback when a chunk is received as part of a deliver operation.
         * <p>
         * Note the offset is an unsigned long. Longs are signed in Java, but unsigned longs
         * can be used as long as some care is taken for some operations. See
         * the <code>unsigned*</code> static methods in {@link Long}.
         *
         * @param client         the client instance (e.g. to ask for more credit)
         * @param subscriptionId the subscription ID to correlate with a callback
         * @param offset         the first offset in the chunk
         * @param messageCount   the total number of messages in the chunk
         * @param dataSize       the size in bytes of the data in the chunk
         */
        void handle(Client client, int subscriptionId, long offset, long messageCount, long dataSize);

    }

    public interface MessageListener {

        void handle(int subscriptionId, long offset, Message message);

    }

    private static class TuneState {

        private final CountDownLatch latch = new CountDownLatch(1);

        private final AtomicInteger maxFrameSize = new AtomicInteger();

        private final AtomicInteger heartbeat = new AtomicInteger();

        private final int requestedMaxFrameSize;

        private final int requestedHeartbeat;

        public TuneState(int requestedMaxFrameSize, int requestedHeartbeat) {
            this.requestedMaxFrameSize = requestedMaxFrameSize;
            this.requestedHeartbeat = requestedHeartbeat;
        }

        void await(Duration duration) {
            try {
                boolean completed = latch.await(duration.toMillis(), TimeUnit.MILLISECONDS);
                if (!completed) {
                    throw new ClientException("Waited for tune frame for " + duration.getSeconds() + " second(s)");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new ClientException("Interrupted while waiting for tune frame");
            }
        }

        int getMaxFrameSize() {
            return maxFrameSize.get();
        }

        int getHeartbeat() {
            return heartbeat.get();
        }

        TuneState maxFrameSize(int maxFrameSize) {
            this.maxFrameSize.set(maxFrameSize);
            return this;
        }

        TuneState heartbeat(int heartbeat) {
            this.heartbeat.set(heartbeat);
            return this;
        }

        public void done() {
            latch.countDown();
        }
    }

    public static class Response {

        private final short responseCode;

        public Response(short responseCode) {
            this.responseCode = responseCode;
        }

        public boolean isOk() {
            return responseCode == RESPONSE_CODE_OK;
        }

        public short getResponseCode() {
            return responseCode;
        }
    }

    private static class SaslAuthenticateResponse extends Response {

        private final byte[] challenge;

        public SaslAuthenticateResponse(short responseCode, byte[] challenge) {
            super(responseCode);
            this.challenge = challenge;
        }

        public boolean isChallenge() {
            return this.getResponseCode() == RESPONSE_CODE_SASL_CHALLENGE;
        }

        public boolean isAuthenticationFailure() {
            return this.getResponseCode() == RESPONSE_CODE_AUTHENTICATION_FAILURE ||
                    this.getResponseCode() == RESPONSE_CODE_AUTHENTICATION_FAILURE_LOOPBACK;
        }
    }

    public static class StreamMetadata {

        private final String stream;

        private final short responseCode;

        private final Broker leader;

        private final Collection<Broker> replicas;

        public StreamMetadata(String stream, short responseCode, Broker leader, Collection<Broker> replicas) {
            this.stream = stream;
            this.responseCode = responseCode;
            this.leader = leader;
            this.replicas = replicas;
        }

        public short getResponseCode() {
            return responseCode;
        }

        public Broker getLeader() {
            return leader;
        }

        public Collection<Broker> getReplicas() {
            return replicas;
        }

        public String getStream() {
            return stream;
        }

        @Override
        public String toString() {
            return "StreamMetadata{" +
                    "stream='" + stream + '\'' +
                    ", responseCode=" + responseCode +
                    ", leader=" + leader +
                    ", replicas=" + replicas +
                    '}';
        }
    }

    public static class Broker {

        private final String host;

        private final int port;

        public Broker(String host, int port) {
            this.host = host;
            this.port = port;
        }

        public String getHost() {
            return host;
        }

        public int getPort() {
            return port;
        }

        @Override
        public String toString() {
            return "Broker{" +
                    "host='" + host + '\'' +
                    ", port=" + port +
                    '}';
        }
    }

    public static class ClientParameters {

        private String host = "localhost";
        private int port = DEFAULT_PORT;
        private String virtualHost = "/";
        private Duration requestedHeartbeat = Duration.ofSeconds(60);
        private int requestedMaxFrameSize = 131072;

        private ConfirmListener confirmListener = (publishingId) -> {

        };

        private PublishErrorListener publishErrorListener = (publishingId, responseCode) -> {

        };

        private ChunkListener chunkListener = (client, correlationId, offset, messageCount, dataSize) -> {

        };

        private MessageListener messageListener = (correlationId, offset, message) -> {

        };

        private SubscriptionListener subscriptionListener = (subscriptionId, stream, reason) -> {

        };

        private Codec codec;

        // TODO eventloopgroup should be shared between clients, this could justify the introduction of client factory
        private EventLoopGroup eventLoopGroup;

        private SaslConfiguration saslConfiguration = DefaultSaslConfiguration.PLAIN;

        private CredentialsProvider credentialsProvider = new DefaultUsernamePasswordCredentialsProvider("guest", "guest");

        private ChannelCustomizer channelCustomizer = ch -> {
        };

        private ChunkChecksum chunkChecksum = JdkChunkChecksum.CRC32_SINGLETON;

        public ClientParameters host(String host) {
            this.host = host;
            return this;
        }

        public ClientParameters port(int port) {
            this.port = port;
            return this;
        }

        public ClientParameters confirmListener(ConfirmListener confirmListener) {
            this.confirmListener = confirmListener;
            return this;
        }

        public ClientParameters publishErrorListener(PublishErrorListener publishErrorListener) {
            this.publishErrorListener = publishErrorListener;
            return this;
        }

        public ClientParameters chunkListener(ChunkListener chunkListener) {
            this.chunkListener = chunkListener;
            return this;
        }

        public ClientParameters messageListener(MessageListener messageListener) {
            this.messageListener = messageListener;
            return this;
        }

        public ClientParameters subscriptionListener(SubscriptionListener subscriptionListener) {
            this.subscriptionListener = subscriptionListener;
            return this;
        }

        public ClientParameters codec(Codec codec) {
            this.codec = codec;
            return this;
        }

        public ClientParameters eventLoopGroup(EventLoopGroup eventLoopGroup) {
            this.eventLoopGroup = eventLoopGroup;
            return this;
        }

        public ClientParameters saslConfiguration(SaslConfiguration saslConfiguration) {
            this.saslConfiguration = saslConfiguration;
            return this;
        }

        public ClientParameters credentialsProvider(CredentialsProvider credentialsProvider) {
            this.credentialsProvider = credentialsProvider;
            return this;
        }

        public ClientParameters username(String username) {
            if (this.credentialsProvider instanceof UsernamePasswordCredentialsProvider) {
                this.credentialsProvider = new DefaultUsernamePasswordCredentialsProvider(
                        username,
                        ((UsernamePasswordCredentialsProvider) this.credentialsProvider).getPassword()
                );
            } else {
                this.credentialsProvider = new DefaultUsernamePasswordCredentialsProvider(username, null);
            }
            return this;
        }

        public ClientParameters password(String password) {
            if (this.credentialsProvider instanceof UsernamePasswordCredentialsProvider) {
                this.credentialsProvider = new DefaultUsernamePasswordCredentialsProvider(
                        ((UsernamePasswordCredentialsProvider) this.credentialsProvider).getUsername(),
                        password
                );
            } else {
                this.credentialsProvider = new DefaultUsernamePasswordCredentialsProvider(null, password);
            }
            return this;
        }

        public ClientParameters virtualHost(String virtualHost) {
            this.virtualHost = virtualHost;
            return this;
        }

        public ClientParameters requestedHeartbeat(Duration requestedHeartbeat) {
            this.requestedHeartbeat = requestedHeartbeat;
            return this;
        }

        public ClientParameters requestedMaxFrameSize(int requestedMaxFrameSize) {
            this.requestedMaxFrameSize = requestedMaxFrameSize;
            return this;
        }

        public ClientParameters channelCustomizer(ChannelCustomizer channelCustomizer) {
            this.channelCustomizer = channelCustomizer;
            return this;
        }

        public ClientParameters chunkChecksum(ChunkChecksum chunkChecksum) {
            this.chunkChecksum = chunkChecksum;
            return this;
        }
    }

    private static final class BinaryOnlyMessage implements Message {

        private final byte[] body;

        private BinaryOnlyMessage(byte[] body) {
            this.body = body;
        }

        @Override
        public byte[] getBodyAsBinary() {
            return body;
        }

        @Override
        public Object getBody() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Properties getProperties() {
            return null;
        }

        @Override
        public Map<String, Object> getApplicationProperties() {
            return null;
        }

        @Override
        public Map<String, Object> getMessageAnnotations() {
            return null;
        }
    }

    private static class OutstandingRequest<T> {

        private final CountDownLatch latch = new CountDownLatch(1);

        private final Duration timeout;

        private final AtomicReference<T> response = new AtomicReference<>();

        private OutstandingRequest(Duration timeout) {
            this.timeout = timeout;
        }

        void block() {
            boolean completed;
            try {
                completed = latch.await(timeout.toMillis(), TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new ClientException("Interrupted while waiting for response");
            }
            if (!completed) {
                throw new ClientException("Could not get response in " + timeout.toMillis() + " ms");
            }
        }

    }

    static final class SubscriptionOffset {

        private final int subscriptionId;
        private final long offset;

        SubscriptionOffset(int subscriptionId, long offset) {
            this.subscriptionId = subscriptionId;
            this.offset = offset;
        }
    }

    public static class StreamParametersBuilder {

        private final Map<String, String> parameters = new HashMap<>();

        public StreamParametersBuilder maxLengthBytes(long bytes) {
            this.parameters.put("max-length-bytes", String.valueOf(bytes));
            return this;
        }

        public StreamParametersBuilder maxLengthBytes(ByteCapacity bytes) {
            return maxLengthBytes(bytes.toBytes());
        }

        public StreamParametersBuilder maxLengthKb(long kiloBytes) {
            return maxLengthBytes(kiloBytes * 1000);
        }

        public StreamParametersBuilder maxLengthMb(long megaBytes) {
            return maxLengthBytes(megaBytes * 1000 * 1000);
        }

        public StreamParametersBuilder maxLengthGb(long gigaBytes) {
            return maxLengthBytes(gigaBytes * 1000 * 1000 * 1000);
        }

        public StreamParametersBuilder maxLengthTb(long teraBytes) {
            return maxLengthBytes(teraBytes * 1000 * 1000 * 1000 * 1000);
        }

        public StreamParametersBuilder maxSegmentSizeBytes(long bytes) {
            this.parameters.put("max-segment-size", String.valueOf(bytes));
            return this;
        }

        public StreamParametersBuilder maxSegmentSizeBytes(ByteCapacity bytes) {
            return maxSegmentSizeBytes(bytes.toBytes());
        }

        public StreamParametersBuilder maxSegmentSizeKb(long kiloBytes) {
            return maxSegmentSizeBytes(kiloBytes * 1000);
        }

        public StreamParametersBuilder maxSegmentSizeMb(long megaBytes) {
            return maxSegmentSizeBytes(megaBytes * 1000 * 1000);
        }

        public StreamParametersBuilder maxSegmentSizeGb(long gigaBytes) {
            return maxSegmentSizeBytes(gigaBytes * 1000 * 1000 * 1000);
        }

        public StreamParametersBuilder maxSegmentSizeTb(long teraBytes) {
            return maxSegmentSizeBytes(teraBytes * 1000 * 1000 * 1000 * 1000);
        }

        public StreamParametersBuilder put(String key, String value) {
            parameters.put(key, value);
            return this;
        }

        public Map<String, String> build() {
            return parameters;
        }

    }

    private static class Subscriptions {

        private final Map<String, List<Integer>> streamsToSubscriptions = new ConcurrentHashMap<>();

        void add(String stream, int subscriptionId) {
            List<Integer> subscriptionIds = streamsToSubscriptions.compute(stream, (k, v) -> v == null ? new CopyOnWriteArrayList<>() : v);
            subscriptionIds.add(subscriptionId);
        }

        void remove(int subscriptionId) {
            Iterator<Map.Entry<String, List<Integer>>> iterator = streamsToSubscriptions.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<String, List<Integer>> entry = iterator.next();
                boolean removed = entry.getValue().remove((Object) subscriptionId);
                if (removed) {
                    if (entry.getValue().isEmpty()) {
                        iterator.remove();
                        break;
                    }
                }
            }
        }

        List<Integer> removeSubscriptionsFor(String stream) {
            List<Integer> subscriptionIds = streamsToSubscriptions.remove(stream);
            return subscriptionIds == null ? Collections.emptyList() : subscriptionIds;
        }

    }

    private class StreamHandler extends ChannelInboundHandlerAdapter {

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            ByteBuf m = (ByteBuf) msg;

            int frameSize = m.readableBytes();
            short commandId = m.readShort();
            short version = m.readShort();
            if (version != VERSION_0) {
                throw new ClientException("Unsupported version " + version + " for command " + commandId);
            }
            Runnable task;
            if (closing.get()) {
                if (commandId == COMMAND_CLOSE) {
                    task = () -> handleResponse(m, frameSize, outstandingRequests);
                } else {
                    LOGGER.debug("Ignoring command {} from server while closing", commandId);
                    try {
                        while (m.isReadable()) {
                            m.readByte();
                        }
                    } finally {
                        m.release();
                    }
                    task = null;
                }
            } else {
                if (commandId == COMMAND_PUBLISH_CONFIRM) {
                    task = () -> handleConfirm(m, confirmListener, frameSize);
                } else if (commandId == COMMAND_DELIVER) {
                    task = () -> handleDeliver(m, Client.this, chunkListener, messageListener, frameSize, codec, subscriptionOffsets, chunkChecksum);
                } else if (commandId == COMMAND_PUBLISH_ERROR) {
                    task = () -> handlePublishError(m, publishErrorListener, frameSize);
                } else if (commandId == COMMAND_METADATA_UPDATE) {
                    task = () -> handleMetadataUpdate(m, frameSize, subscriptions, subscriptionListener);
                } else if (commandId == COMMAND_METADATA) {
                    task = () -> handleMetadata(m, frameSize, outstandingRequests);
                } else if (commandId == COMMAND_SASL_HANDSHAKE) {
                    task = () -> handleSaslHandshakeResponse(m, frameSize, outstandingRequests);
                } else if (commandId == COMMAND_SASL_AUTHENTICATE) {
                    task = () -> handleSaslAuthenticateResponse(m, frameSize, outstandingRequests);
                } else if (commandId == COMMAND_TUNE) {
                    task = () -> handleTune(m, frameSize, ctx, tuneState);
                } else if (commandId == COMMAND_CLOSE) {
                    task = () -> handleClose(m, frameSize, ctx);
                } else if (commandId == COMMAND_HEARTBEAT) {
                    task = () -> handleHeartbeat(frameSize);
                } else if (commandId == COMMAND_SUBSCRIBE || commandId == COMMAND_UNSUBSCRIBE
                        || commandId == COMMAND_CREATE_STREAM || commandId == COMMAND_DELETE_STREAM
                        || commandId == COMMAND_OPEN) {
                    task = () -> handleResponse(m, frameSize, outstandingRequests);
                } else {
                    throw new ClientException("Unsupported command " + commandId);
                }
            }

            if (task != null) {
                executorService.submit(() -> {
                    try {
                        task.run();
                    } catch (Exception e) {
                        LOGGER.warn("Error while handling response from server", e);
                    } finally {
                        m.release();
                    }
                });
            }
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            LOGGER.debug("Netty channel became inactive", closing.get());
            closing.set(true);
            closeNetty();
        }

        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
            if (evt instanceof IdleStateEvent) {
                IdleStateEvent e = (IdleStateEvent) evt;
                if (e.state() == IdleState.READER_IDLE) {
                    LOGGER.info("Closing connection because it's been idle for too long");
                    closing.set(true);
                    closeNetty();
                } else if (e.state() == IdleState.WRITER_IDLE) {
                    LOGGER.debug("Sending heartbeat frame");
                    ByteBuf bb = allocate(ctx.alloc(), 4 + 2 + 2);
                    bb.writeInt(4).writeShort(COMMAND_HEARTBEAT).writeShort(VERSION_0);
                    ctx.writeAndFlush(bb);
                }
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            LOGGER.warn("Error in stream handler", cause);
            ctx.close();
        }
    }


}
