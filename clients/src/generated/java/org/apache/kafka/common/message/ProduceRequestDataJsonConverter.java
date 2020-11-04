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
import com.fasterxml.jackson.databind.node.BinaryNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ShortNode;
import com.fasterxml.jackson.databind.node.TextNode;
import java.util.ArrayList;
import java.util.Arrays;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.protocol.MessageUtil;

import static org.apache.kafka.common.message.ProduceRequestData.*;

public class ProduceRequestDataJsonConverter {
    public static ProduceRequestData read(JsonNode _node, short _version) {
        ProduceRequestData _object = new ProduceRequestData();
        JsonNode _transactionalIdNode = _node.get("transactionalId");
        if (_transactionalIdNode == null) {
            if (_version >= 3) {
                throw new RuntimeException("ProduceRequestData: unable to locate field 'transactionalId', which is mandatory in version " + _version);
            } else {
                _object.transactionalId = "";
            }
        } else {
            if (_transactionalIdNode.isNull()) {
                _object.transactionalId = null;
            } else {
                if (!_transactionalIdNode.isTextual()) {
                    throw new RuntimeException("ProduceRequestData expected a string type, but got " + _node.getNodeType());
                }
                _object.transactionalId = _transactionalIdNode.asText();
            }
        }
        JsonNode _acksNode = _node.get("acks");
        if (_acksNode == null) {
            throw new RuntimeException("ProduceRequestData: unable to locate field 'acks', which is mandatory in version " + _version);
        } else {
            _object.acks = MessageUtil.jsonNodeToShort(_acksNode, "ProduceRequestData");
        }
        JsonNode _timeoutMsNode = _node.get("timeoutMs");
        if (_timeoutMsNode == null) {
            throw new RuntimeException("ProduceRequestData: unable to locate field 'timeoutMs', which is mandatory in version " + _version);
        } else {
            _object.timeoutMs = MessageUtil.jsonNodeToInt(_timeoutMsNode, "ProduceRequestData");
        }
        JsonNode _topicsNode = _node.get("topics");
        if (_topicsNode == null) {
            throw new RuntimeException("ProduceRequestData: unable to locate field 'topics', which is mandatory in version " + _version);
        } else {
            if (!_topicsNode.isArray()) {
                throw new RuntimeException("ProduceRequestData expected a JSON array, but got " + _node.getNodeType());
            }
            ArrayList<TopicProduceData> _collection = new ArrayList<TopicProduceData>();
            _object.topics = _collection;
            for (JsonNode _element : _topicsNode) {
                _collection.add(TopicProduceDataJsonConverter.read(_element, _version));
            }
        }
        return _object;
    }
    public static JsonNode write(ProduceRequestData _object, short _version) {
        ObjectNode _node = new ObjectNode(JsonNodeFactory.instance);
        if (_version >= 3) {
            if (_object.transactionalId == null) {
                _node.set("transactionalId", NullNode.instance);
            } else {
                _node.set("transactionalId", new TextNode(_object.transactionalId));
            }
        } else {
            if (_object.transactionalId == null || !_object.transactionalId.equals("")) {
                throw new UnsupportedVersionException("Attempted to write a non-default transactionalId at version " + _version);
            }
        }
        _node.set("acks", new ShortNode(_object.acks));
        _node.set("timeoutMs", new IntNode(_object.timeoutMs));
        ArrayNode _topicsArray = new ArrayNode(JsonNodeFactory.instance);
        for (TopicProduceData _element : _object.topics) {
            _topicsArray.add(TopicProduceDataJsonConverter.write(_element, _version));
        }
        _node.set("topics", _topicsArray);
        return _node;
    }
    
    public static class PartitionProduceDataJsonConverter {
        public static PartitionProduceData read(JsonNode _node, short _version) {
            PartitionProduceData _object = new PartitionProduceData();
            JsonNode _partitionIndexNode = _node.get("partitionIndex");
            if (_partitionIndexNode == null) {
                throw new RuntimeException("PartitionProduceData: unable to locate field 'partitionIndex', which is mandatory in version " + _version);
            } else {
                _object.partitionIndex = MessageUtil.jsonNodeToInt(_partitionIndexNode, "PartitionProduceData");
            }
            JsonNode _recordsNode = _node.get("records");
            if (_recordsNode == null) {
                throw new RuntimeException("PartitionProduceData: unable to locate field 'records', which is mandatory in version " + _version);
            } else {
                if (_recordsNode.isNull()) {
                    _object.records = null;
                } else {
                    _object.records = MessageUtil.jsonNodeToBinary(_recordsNode, "PartitionProduceData");
                }
            }
            return _object;
        }
        public static JsonNode write(PartitionProduceData _object, short _version) {
            ObjectNode _node = new ObjectNode(JsonNodeFactory.instance);
            _node.set("partitionIndex", new IntNode(_object.partitionIndex));
            if (_object.records == null) {
                _node.set("records", NullNode.instance);
            } else {
                _node.set("records", new BinaryNode(Arrays.copyOf(_object.records, _object.records.length)));
            }
            return _node;
        }
    }
    
    public static class TopicProduceDataJsonConverter {
        public static TopicProduceData read(JsonNode _node, short _version) {
            TopicProduceData _object = new TopicProduceData();
            JsonNode _nameNode = _node.get("name");
            if (_nameNode == null) {
                throw new RuntimeException("TopicProduceData: unable to locate field 'name', which is mandatory in version " + _version);
            } else {
                if (!_nameNode.isTextual()) {
                    throw new RuntimeException("TopicProduceData expected a string type, but got " + _node.getNodeType());
                }
                _object.name = _nameNode.asText();
            }
            JsonNode _partitionsNode = _node.get("partitions");
            if (_partitionsNode == null) {
                throw new RuntimeException("TopicProduceData: unable to locate field 'partitions', which is mandatory in version " + _version);
            } else {
                if (!_partitionsNode.isArray()) {
                    throw new RuntimeException("TopicProduceData expected a JSON array, but got " + _node.getNodeType());
                }
                ArrayList<PartitionProduceData> _collection = new ArrayList<PartitionProduceData>();
                _object.partitions = _collection;
                for (JsonNode _element : _partitionsNode) {
                    _collection.add(PartitionProduceDataJsonConverter.read(_element, _version));
                }
            }
            return _object;
        }
        public static JsonNode write(TopicProduceData _object, short _version) {
            ObjectNode _node = new ObjectNode(JsonNodeFactory.instance);
            _node.set("name", new TextNode(_object.name));
            ArrayNode _partitionsArray = new ArrayNode(JsonNodeFactory.instance);
            for (PartitionProduceData _element : _object.partitions) {
                _partitionsArray.add(PartitionProduceDataJsonConverter.write(_element, _version));
            }
            _node.set("partitions", _partitionsArray);
            return _node;
        }
    }
}
