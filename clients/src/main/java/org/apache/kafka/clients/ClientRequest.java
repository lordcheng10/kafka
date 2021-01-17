/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.clients;

import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.RequestHeader;

/**
 * A request being sent to the server. This holds both the network send as well as the client-level metadata.
 */
public final class ClientRequest {
    /**
     * destination：发送请求的目的broker
     * requestBuilder：请求构建器
     * correlationId: 关联request和response的id号，correlation是相关性的意思
     * clientId: 放到header中
     * createdTimeMs: 请求被创建时的系统时间戳
     * expectResponse：是否等待response
     * requestTimeoutMs：请求超时时间
     * callback: 当收到response后，触发的回调，如果为null，那么就没有回调处理
     * */
    private final String destination;
    private final AbstractRequest.Builder<?> requestBuilder;
    private final int correlationId;
    private final String clientId;
    private final long createdTimeMs;
    private final boolean expectResponse;
    /**
     * 果然requestTimeoutMs是后面加的，所以下面的参数注释没有他，toString也没有他，应该是修改的人忘记加了
     * */
    private final int requestTimeoutMs;
    private final RequestCompletionHandler callback;

    /**
     * 这里似乎并不是需要把所有的参数都注释一遍，对于很显而易见的，不需要注释.
     * @param destination The brokerId to send the request to
     * @param requestBuilder The builder for the request to make
     * @param correlationId The correlation id for this client request
     * @param clientId The client ID to use for the header
     * @param createdTimeMs The unix timestamp in milliseconds for the time at which this request was created.
     * @param expectResponse Should we expect a response message or is this request complete once it is sent?
     * @param callback A callback to execute when the response has been received (or null if no callback is necessary)
     */
    public ClientRequest(String destination,
                         AbstractRequest.Builder<?> requestBuilder,
                         int correlationId,
                         String clientId,
                         long createdTimeMs,
                         boolean expectResponse,
                         int requestTimeoutMs,
                         RequestCompletionHandler callback) {
        this.destination = destination;
        this.requestBuilder = requestBuilder;
        this.correlationId = correlationId;
        this.clientId = clientId;
        this.createdTimeMs = createdTimeMs;
        this.expectResponse = expectResponse;
        this.requestTimeoutMs = requestTimeoutMs;
        this.callback = callback;
    }


    /**
     * TODO-patch chenlin 添加requestTimeoutMs，应该是后面加的，忘记加了 遗漏了
     * */
    @Override
    public String toString() {
        return "ClientRequest(expectResponse=" + expectResponse +
            ", callback=" + callback +
            ", destination=" + destination +
            ", correlationId=" + correlationId +
            ", clientId=" + clientId +
            ", createdTimeMs=" + createdTimeMs +
            ", requestBuilder=" + requestBuilder +
            ")";
    }

    public boolean expectResponse() {
        return expectResponse;
    }

    public ApiKeys apiKey() {
        return requestBuilder.apiKey();
    }

    /**
     * 构建header.
     *
     * 这里的new RequestHeaderData()
     *                 .setRequestApiKey(requestApiKey.id)
     *                 .setRequestApiVersion(version)
     *                 .setClientId(clientId)
     *                 .setCorrelationId(correlationId)
     *                 很有意思，全是链式编程，每个方法都return this了,而且这还是自动生成的代码.
     *
     * RequestHeader主要包含两个信息：RequestHeaderData和headerVersion
     *
     * 根据version来获取header的version号
     * */
    public RequestHeader makeHeader(short version) {
        ApiKeys requestApiKey = apiKey();
        return new RequestHeader(
            new RequestHeaderData()
                .setRequestApiKey(requestApiKey.id)
                .setRequestApiVersion(version)
                .setClientId(clientId)
                .setCorrelationId(correlationId),
            requestApiKey.requestHeaderVersion(version));
    }

    public AbstractRequest.Builder<?> requestBuilder() {
        return requestBuilder;
    }

    public String destination() {
        return destination;
    }

    public RequestCompletionHandler callback() {
        return callback;
    }

    public long createdTimeMs() {
        return createdTimeMs;
    }

    public int correlationId() {
        return correlationId;
    }

    public int requestTimeoutMs() {
        return requestTimeoutMs;
    }
}
