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
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.protocol.MessageUtil;

import static org.apache.kafka.common.message.ListOffsetResponseData.*;

public class ListOffsetResponseDataJsonConverter {
    public static ListOffsetResponseData read(JsonNode _node, short _version) {
        ListOffsetResponseData _object = new ListOffsetResponseData();
        JsonNode _throttleTimeMsNode = _node.get("throttleTimeMs");
        if (_throttleTimeMsNode == null) {
            if (_version >= 2) {
                throw new RuntimeException("ListOffsetResponseData: unable to locate field 'throttleTimeMs', which is mandatory in version " + _version);
            } else {
                _object.throttleTimeMs = 0;
            }
        } else {
            _object.throttleTimeMs = MessageUtil.jsonNodeToInt(_throttleTimeMsNode, "ListOffsetResponseData");
        }
        JsonNode _topicsNode = _node.get("topics");
        if (_topicsNode == null) {
            throw new RuntimeException("ListOffsetResponseData: unable to locate field 'topics', which is mandatory in version " + _version);
        } else {
            if (!_topicsNode.isArray()) {
                throw new RuntimeException("ListOffsetResponseData expected a JSON array, but got " + _node.getNodeType());
            }
            ArrayList<ListOffsetTopicResponse> _collection = new ArrayList<ListOffsetTopicResponse>();
            _object.topics = _collection;
            for (JsonNode _element : _topicsNode) {
                _collection.add(ListOffsetTopicResponseJsonConverter.read(_element, _version));
            }
        }
        return _object;
    }
    public static JsonNode write(ListOffsetResponseData _object, short _version) {
        ObjectNode _node = new ObjectNode(JsonNodeFactory.instance);
        if (_version >= 2) {
            _node.set("throttleTimeMs", new IntNode(_object.throttleTimeMs));
        }
        ArrayNode _topicsArray = new ArrayNode(JsonNodeFactory.instance);
        for (ListOffsetTopicResponse _element : _object.topics) {
            _topicsArray.add(ListOffsetTopicResponseJsonConverter.write(_element, _version));
        }
        _node.set("topics", _topicsArray);
        return _node;
    }
    
    public static class ListOffsetPartitionResponseJsonConverter {
        public static ListOffsetPartitionResponse read(JsonNode _node, short _version) {
            ListOffsetPartitionResponse _object = new ListOffsetPartitionResponse();
            JsonNode _partitionIndexNode = _node.get("partitionIndex");
            if (_partitionIndexNode == null) {
                throw new RuntimeException("ListOffsetPartitionResponse: unable to locate field 'partitionIndex', which is mandatory in version " + _version);
            } else {
                _object.partitionIndex = MessageUtil.jsonNodeToInt(_partitionIndexNode, "ListOffsetPartitionResponse");
            }
            JsonNode _errorCodeNode = _node.get("errorCode");
            if (_errorCodeNode == null) {
                throw new RuntimeException("ListOffsetPartitionResponse: unable to locate field 'errorCode', which is mandatory in version " + _version);
            } else {
                _object.errorCode = MessageUtil.jsonNodeToShort(_errorCodeNode, "ListOffsetPartitionResponse");
            }
            JsonNode _oldStyleOffsetsNode = _node.get("oldStyleOffsets");
            if (_oldStyleOffsetsNode == null) {
                if (_version <= 0) {
                    throw new RuntimeException("ListOffsetPartitionResponse: unable to locate field 'oldStyleOffsets', which is mandatory in version " + _version);
                } else {
                    _object.oldStyleOffsets = new ArrayList<Long>(0);
                }
            } else {
                if (!_oldStyleOffsetsNode.isArray()) {
                    throw new RuntimeException("ListOffsetPartitionResponse expected a JSON array, but got " + _node.getNodeType());
                }
                ArrayList<Long> _collection = new ArrayList<Long>();
                _object.oldStyleOffsets = _collection;
                for (JsonNode _element : _oldStyleOffsetsNode) {
                    _collection.add(MessageUtil.jsonNodeToLong(_element, "ListOffsetPartitionResponse element"));
                }
            }
            JsonNode _timestampNode = _node.get("timestamp");
            if (_timestampNode == null) {
                if (_version >= 1) {
                    throw new RuntimeException("ListOffsetPartitionResponse: unable to locate field 'timestamp', which is mandatory in version " + _version);
                } else {
                    _object.timestamp = -1L;
                }
            } else {
                _object.timestamp = MessageUtil.jsonNodeToLong(_timestampNode, "ListOffsetPartitionResponse");
            }
            JsonNode _offsetNode = _node.get("offset");
            if (_offsetNode == null) {
                if (_version >= 1) {
                    throw new RuntimeException("ListOffsetPartitionResponse: unable to locate field 'offset', which is mandatory in version " + _version);
                } else {
                    _object.offset = -1L;
                }
            } else {
                _object.offset = MessageUtil.jsonNodeToLong(_offsetNode, "ListOffsetPartitionResponse");
            }
            JsonNode _leaderEpochNode = _node.get("leaderEpoch");
            if (_leaderEpochNode == null) {
                if (_version >= 4) {
                    throw new RuntimeException("ListOffsetPartitionResponse: unable to locate field 'leaderEpoch', which is mandatory in version " + _version);
                } else {
                    _object.leaderEpoch = -1;
                }
            } else {
                _object.leaderEpoch = MessageUtil.jsonNodeToInt(_leaderEpochNode, "ListOffsetPartitionResponse");
            }
            return _object;
        }
        public static JsonNode write(ListOffsetPartitionResponse _object, short _version) {
            ObjectNode _node = new ObjectNode(JsonNodeFactory.instance);
            _node.set("partitionIndex", new IntNode(_object.partitionIndex));
            _node.set("errorCode", new ShortNode(_object.errorCode));
            if (_version <= 0) {
                ArrayNode _oldStyleOffsetsArray = new ArrayNode(JsonNodeFactory.instance);
                for (Long _element : _object.oldStyleOffsets) {
                    _oldStyleOffsetsArray.add(new LongNode(_element));
                }
                _node.set("oldStyleOffsets", _oldStyleOffsetsArray);
            } else {
                if (!_object.oldStyleOffsets.isEmpty()) {
                    throw new UnsupportedVersionException("Attempted to write a non-default oldStyleOffsets at version " + _version);
                }
            }
            if (_version >= 1) {
                _node.set("timestamp", new LongNode(_object.timestamp));
            } else {
                if (_object.timestamp != -1L) {
                    throw new UnsupportedVersionException("Attempted to write a non-default timestamp at version " + _version);
                }
            }
            if (_version >= 1) {
                _node.set("offset", new LongNode(_object.offset));
            } else {
                if (_object.offset != -1L) {
                    throw new UnsupportedVersionException("Attempted to write a non-default offset at version " + _version);
                }
            }
            if (_version >= 4) {
                _node.set("leaderEpoch", new IntNode(_object.leaderEpoch));
            } else {
                if (_object.leaderEpoch != -1) {
                    throw new UnsupportedVersionException("Attempted to write a non-default leaderEpoch at version " + _version);
                }
            }
            return _node;
        }
    }
    
    public static class ListOffsetTopicResponseJsonConverter {
        public static ListOffsetTopicResponse read(JsonNode _node, short _version) {
            ListOffsetTopicResponse _object = new ListOffsetTopicResponse();
            JsonNode _nameNode = _node.get("name");
            if (_nameNode == null) {
                throw new RuntimeException("ListOffsetTopicResponse: unable to locate field 'name', which is mandatory in version " + _version);
            } else {
                if (!_nameNode.isTextual()) {
                    throw new RuntimeException("ListOffsetTopicResponse expected a string type, but got " + _node.getNodeType());
                }
                _object.name = _nameNode.asText();
            }
            JsonNode _partitionsNode = _node.get("partitions");
            if (_partitionsNode == null) {
                throw new RuntimeException("ListOffsetTopicResponse: unable to locate field 'partitions', which is mandatory in version " + _version);
            } else {
                if (!_partitionsNode.isArray()) {
                    throw new RuntimeException("ListOffsetTopicResponse expected a JSON array, but got " + _node.getNodeType());
                }
                ArrayList<ListOffsetPartitionResponse> _collection = new ArrayList<ListOffsetPartitionResponse>();
                _object.partitions = _collection;
                for (JsonNode _element : _partitionsNode) {
                    _collection.add(ListOffsetPartitionResponseJsonConverter.read(_element, _version));
                }
            }
            return _object;
        }
        public static JsonNode write(ListOffsetTopicResponse _object, short _version) {
            ObjectNode _node = new ObjectNode(JsonNodeFactory.instance);
            _node.set("name", new TextNode(_object.name));
            ArrayNode _partitionsArray = new ArrayNode(JsonNodeFactory.instance);
            for (ListOffsetPartitionResponse _element : _object.partitions) {
                _partitionsArray.add(ListOffsetPartitionResponseJsonConverter.write(_element, _version));
            }
            _node.set("partitions", _partitionsArray);
            return _node;
        }
    }
}
