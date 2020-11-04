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
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import java.util.ArrayList;
import org.apache.kafka.common.protocol.MessageUtil;

import static org.apache.kafka.common.message.DeleteTopicsRequestData.*;

public class DeleteTopicsRequestDataJsonConverter {
    public static DeleteTopicsRequestData read(JsonNode _node, short _version) {
        DeleteTopicsRequestData _object = new DeleteTopicsRequestData();
        JsonNode _topicNamesNode = _node.get("topicNames");
        if (_topicNamesNode == null) {
            throw new RuntimeException("DeleteTopicsRequestData: unable to locate field 'topicNames', which is mandatory in version " + _version);
        } else {
            if (!_topicNamesNode.isArray()) {
                throw new RuntimeException("DeleteTopicsRequestData expected a JSON array, but got " + _node.getNodeType());
            }
            ArrayList<String> _collection = new ArrayList<String>();
            _object.topicNames = _collection;
            for (JsonNode _element : _topicNamesNode) {
                if (!_element.isTextual()) {
                    throw new RuntimeException("DeleteTopicsRequestData element expected a string type, but got " + _node.getNodeType());
                }
                _collection.add(_element.asText());
            }
        }
        JsonNode _timeoutMsNode = _node.get("timeoutMs");
        if (_timeoutMsNode == null) {
            throw new RuntimeException("DeleteTopicsRequestData: unable to locate field 'timeoutMs', which is mandatory in version " + _version);
        } else {
            _object.timeoutMs = MessageUtil.jsonNodeToInt(_timeoutMsNode, "DeleteTopicsRequestData");
        }
        return _object;
    }
    public static JsonNode write(DeleteTopicsRequestData _object, short _version) {
        ObjectNode _node = new ObjectNode(JsonNodeFactory.instance);
        ArrayNode _topicNamesArray = new ArrayNode(JsonNodeFactory.instance);
        for (String _element : _object.topicNames) {
            _topicNamesArray.add(new TextNode(_element));
        }
        _node.set("topicNames", _topicNamesArray);
        _node.set("timeoutMs", new IntNode(_object.timeoutMs));
        return _node;
    }
}
