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

public final class Constants {

  public static final short RESPONSE_CODE_OK = 0;
  public static final short RESPONSE_CODE_STREAM_DOES_NOT_EXIST = 1;
  public static final short RESPONSE_CODE_SUBSCRIPTION_ID_ALREADY_EXISTS = 2;
  public static final short RESPONSE_CODE_SUBSCRIPTION_ID_DOES_NOT_EXIST = 3;
  public static final short RESPONSE_CODE_STREAM_ALREADY_EXISTS = 4;
  public static final short RESPONSE_CODE_STREAM_NOT_AVAILABLE = 5;
  public static final short RESPONSE_CODE_SASL_MECHANISM_NOT_SUPPORTED = 6;
  public static final short RESPONSE_CODE_AUTHENTICATION_FAILURE = 7;
  public static final short RESPONSE_CODE_SASL_ERROR = 8;
  public static final short RESPONSE_CODE_SASL_CHALLENGE = 9;
  public static final short RESPONSE_CODE_AUTHENTICATION_FAILURE_LOOPBACK = 10;
  public static final short RESPONSE_CODE_VIRTUAL_HOST_ACCESS_FAILURE = 11;
  public static final short RESPONSE_CODE_UNKNOWN_FRAME = 12;
  public static final short RESPONSE_CODE_FRAME_TOO_LARGE = 13;
  public static final short RESPONSE_CODE_INTERNAL_ERROR = 14;
  public static final short RESPONSE_CODE_ACCESS_REFUSED = 15;
  public static final short RESPONSE_CODE_PRECONDITION_FAILED = 16;
  public static final short RESPONSE_CODE_PUBLISHER_DOES_NOT_EXIST = 17;

  public static final short CODE_MESSAGE_ENQUEUEING_FAILED = 10_000;
  public static final short CODE_PRODUCER_NOT_AVAILABLE = 10_001;
  public static final short CODE_PRODUCER_CLOSED = 10_002;
  public static final short CODE_PUBLISH_CONFIRM_TIMEOUT = 10_003;

  public static final short COMMAND_DECLARE_PUBLISHER = 0;
  public static final short COMMAND_PUBLISH = 1;
  public static final short COMMAND_PUBLISH_CONFIRM = 2;
  public static final short COMMAND_PUBLISH_ERROR = 3;
  public static final short COMMAND_QUERY_PUBLISHER_SEQUENCE = 4;
  public static final short COMMAND_DELETE_PUBLISHER = 5;
  public static final short COMMAND_SUBSCRIBE = 6;
  public static final short COMMAND_DELIVER = 7;
  public static final short COMMAND_CREDIT = 8;
  public static final short COMMAND_COMMIT_OFFSET = 9;
  public static final short COMMAND_QUERY_OFFSET = 10;
  public static final short COMMAND_UNSUBSCRIBE = 11;
  public static final short COMMAND_CREATE_STREAM = 12;
  public static final short COMMAND_DELETE_STREAM = 13;
  public static final short COMMAND_METADATA = 14;
  public static final short COMMAND_METADATA_UPDATE = 15;
  public static final short COMMAND_PEER_PROPERTIES = 16;
  public static final short COMMAND_SASL_HANDSHAKE = 17;
  public static final short COMMAND_SASL_AUTHENTICATE = 18;
  public static final short COMMAND_TUNE = 19;
  public static final short COMMAND_OPEN = 20;
  public static final short COMMAND_CLOSE = 21;
  public static final short COMMAND_HEARTBEAT = 22;
  public static final short COMMAND_ROUTE = 23;
  public static final short COMMAND_PARTITIONS = 24;

  public static final short VERSION_0 = 0;

  private Constants() {}
}
