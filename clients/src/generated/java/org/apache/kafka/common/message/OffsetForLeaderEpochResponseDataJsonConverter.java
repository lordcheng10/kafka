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

// THIS CODE IS AUTOMATICALLY GENERATED.  DO NOT EDIT.

package org.apache.kafka.common.message;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.LongNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ShortNode;
import com.fasterxml.jackson.databind.node.TextNode;
import java.util.ArrayList;
import org.apache.kafka.common.protocol.MessageUtil;

import static org.apache.kafka.common.message.OffsetForLeaderEpochResponseData.*;

public class OffsetForLeaderEpochResponseDataJsonConverter {
    public static OffsetForLeaderEpochResponseData read(JsonNode _node, short _version) {
        OffsetForLeaderEpochResponseData _object = new OffsetForLeaderEpochResponseData();
        JsonNode _throttleTimeMsNode = _node.get("throttleTimeMs");
        if (_throttleTimeMsNode == null) {
            if (_version >= 2) {
                throw new RuntimeException("OffsetForLeaderEpochResponseData: unable to locate field 'throttleTimeMs', which is mandatory in version " + _version);
            } else {
                _object.throttleTimeMs = 0;
            }
        } else {
            _object.throttleTimeMs = MessageUtil.jsonNodeToInt(_throttleTimeMsNode, "OffsetForLeaderEpochResponseData");
        }
        JsonNode _topicsNode = _node.get("topics");
        if (_topicsNode == null) {
            throw new RuntimeException("OffsetForLeaderEpochResponseData: unable to locate field 'topics', which is mandatory in version " + _version);
        } else {
            if (!_topicsNode.isArray()) {
                throw new RuntimeException("OffsetForLeaderEpochResponseData expected a JSON array, but got " + _node.getNodeType());
            }
            ArrayList<OffsetForLeaderTopicResult> _collection = new ArrayList<OffsetForLeaderTopicResult>();
            _object.topics = _collection;
            for (JsonNode _element : _topicsNode) {
                _collection.add(OffsetForLeaderTopicResultJsonConverter.read(_element, _version));
            }
        }
        return _object;
    }
    public static JsonNode write(OffsetForLeaderEpochResponseData _object, short _version) {
        ObjectNode _node = new ObjectNode(JsonNodeFactory.instance);
        if (_version >= 2) {
            _node.set("throttleTimeMs", new IntNode(_object.throttleTimeMs));
        }
        ArrayNode _topicsArray = new ArrayNode(JsonNodeFactory.instance);
        for (OffsetForLeaderTopicResult _element : _object.topics) {
            _topicsArray.add(OffsetForLeaderTopicResultJsonConverter.write(_element, _version));
        }
        _node.set("topics", _topicsArray);
        return _node;
    }
    
    public static class OffsetForLeaderPartitionResultJsonConverter {
        public static OffsetForLeaderPartitionResult read(JsonNode _node, short _version) {
            OffsetForLeaderPartitionResult _object = new OffsetForLeaderPartitionResult();
            JsonNode _errorCodeNode = _node.get("errorCode");
            if (_errorCodeNode == null) {
                throw new RuntimeException("OffsetForLeaderPartitionResult: unable to locate field 'errorCode', which is mandatory in version " + _version);
            } else {
                _object.errorCode = MessageUtil.jsonNodeToShort(_errorCodeNode, "OffsetForLeaderPartitionResult");
            }
            JsonNode _partitionIndexNode = _node.get("partitionIndex");
            if (_partitionIndexNode == null) {
                throw new RuntimeException("OffsetForLeaderPartitionResult: unable to locate field 'partitionIndex', which is mandatory in version " + _version);
            } else {
                _object.partitionIndex = MessageUtil.jsonNodeToInt(_partitionIndexNode, "OffsetForLeaderPartitionResult");
            }
            JsonNode _leaderEpochNode = _node.get("leaderEpoch");
            if (_leaderEpochNode == null) {
                if (_version >= 1) {
                    throw new RuntimeException("OffsetForLeaderPartitionResult: unable to locate field 'leaderEpoch', which is mandatory in version " + _version);
                } else {
                    _object.leaderEpoch = -1;
                }
            } else {
                _object.leaderEpoch = MessageUtil.jsonNodeToInt(_leaderEpochNode, "OffsetForLeaderPartitionResult");
            }
            JsonNode _endOffsetNode = _node.get("endOffset");
            if (_endOffsetNode == null) {
                throw new RuntimeException("OffsetForLeaderPartitionResult: unable to locate field 'endOffset', which is mandatory in version " + _version);
            } else {
                _object.endOffset = MessageUtil.jsonNodeToLong(_endOffsetNode, "OffsetForLeaderPartitionResult");
            }
            return _object;
        }
        public static JsonNode write(OffsetForLeaderPartitionResult _object, short _version) {
            ObjectNode _node = new ObjectNode(JsonNodeFactory.instance);
            _node.set("errorCode", new ShortNode(_object.errorCode));
            _node.set("partitionIndex", new IntNode(_object.partitionIndex));
            if (_version >= 1) {
                _node.set("leaderEpoch", new IntNode(_object.leaderEpoch));
            }
            _node.set("endOffset", new LongNode(_object.endOffset));
            return _node;
        }
    }
    
    public static class OffsetForLeaderTopicResultJsonConverter {
        public static OffsetForLeaderTopicResult read(JsonNode _node, short _version) {
            OffsetForLeaderTopicResult _object = new OffsetForLeaderTopicResult();
            JsonNode _nameNode = _node.get("name");
            if (_nameNode == null) {
                throw new RuntimeException("OffsetForLeaderTopicResult: unable to locate field 'name', which is mandatory in version " + _version);
            } else {
                if (!_nameNode.isTextual()) {
                    throw new RuntimeException("OffsetForLeaderTopicResult expected a string type, but got " + _node.getNodeType());
                }
                _object.name = _nameNode.asText();
            }
            JsonNode _partitionsNode = _node.get("partitions");
            if (_partitionsNode == null) {
                throw new RuntimeException("OffsetForLeaderTopicResult: unable to locate field 'partitions', which is mandatory in version " + _version);
            } else {
                if (!_partitionsNode.isArray()) {
                    throw new RuntimeException("OffsetForLeaderTopicResult expected a JSON array, but got " + _node.getNodeType());
                }
                ArrayList<OffsetForLeaderPartitionResult> _collection = new ArrayList<OffsetForLeaderPartitionResult>();
                _object.partitions = _collection;
                for (JsonNode _element : _partitionsNode) {
                    _collection.add(OffsetForLeaderPartitionResultJsonConverter.read(_element, _version));
                }
            }
            return _object;
        }
        public static JsonNode write(OffsetForLeaderTopicResult _object, short _version) {
            ObjectNode _node = new ObjectNode(JsonNodeFactory.instance);
            _node.set("name", new TextNode(_object.name));
            ArrayNode _partitionsArray = new ArrayNode(JsonNodeFactory.instance);
            for (OffsetForLeaderPartitionResult _element : _object.partitions) {
                _partitionsArray.add(OffsetForLeaderPartitionResultJsonConverter.write(_element, _version));
            }
            _node.set("partitions", _partitionsArray);
            return _node;
        }
    }
}
