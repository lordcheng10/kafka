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

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.TreeMap;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Message;
import org.apache.kafka.common.protocol.MessageUtil;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.protocol.Readable;
import org.apache.kafka.common.protocol.RecordsReadable;
import org.apache.kafka.common.protocol.RecordsWritable;
import org.apache.kafka.common.protocol.Writable;
import org.apache.kafka.common.protocol.types.ArrayOf;
import org.apache.kafka.common.protocol.types.CompactArrayOf;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.RawTaggedField;
import org.apache.kafka.common.protocol.types.RawTaggedFieldWriter;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.protocol.types.Type;
import org.apache.kafka.common.record.BaseRecords;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.utils.ByteUtils;

import static java.util.Map.Entry;
import static org.apache.kafka.common.protocol.types.Field.TaggedFieldsSection;


public class FetchResponseData implements ApiMessage {
    int throttleTimeMs;
    short errorCode;
    int sessionId;
    List<FetchableTopicResponse> responses;
    private List<RawTaggedField> _unknownTaggedFields;
    
    public static final Schema SCHEMA_0 =
        new Schema(
            new Field("responses", new ArrayOf(FetchableTopicResponse.SCHEMA_0), "The response topics.")
        );
    
    public static final Schema SCHEMA_1 =
        new Schema(
            new Field("throttle_time_ms", Type.INT32, "The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota."),
            new Field("responses", new ArrayOf(FetchableTopicResponse.SCHEMA_0), "The response topics.")
        );
    
    public static final Schema SCHEMA_2 = SCHEMA_1;
    
    public static final Schema SCHEMA_3 = SCHEMA_2;
    
    public static final Schema SCHEMA_4 =
        new Schema(
            new Field("throttle_time_ms", Type.INT32, "The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota."),
            new Field("responses", new ArrayOf(FetchableTopicResponse.SCHEMA_4), "The response topics.")
        );
    
    public static final Schema SCHEMA_5 =
        new Schema(
            new Field("throttle_time_ms", Type.INT32, "The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota."),
            new Field("responses", new ArrayOf(FetchableTopicResponse.SCHEMA_5), "The response topics.")
        );
    
    public static final Schema SCHEMA_6 = SCHEMA_5;
    
    public static final Schema SCHEMA_7 =
        new Schema(
            new Field("throttle_time_ms", Type.INT32, "The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota."),
            new Field("error_code", Type.INT16, "The top level response error code."),
            new Field("session_id", Type.INT32, "The fetch session ID, or 0 if this is not part of a fetch session."),
            new Field("responses", new ArrayOf(FetchableTopicResponse.SCHEMA_5), "The response topics.")
        );
    
    public static final Schema SCHEMA_8 = SCHEMA_7;
    
    public static final Schema SCHEMA_9 = SCHEMA_8;
    
    public static final Schema SCHEMA_10 = SCHEMA_9;
    
    public static final Schema SCHEMA_11 =
        new Schema(
            new Field("throttle_time_ms", Type.INT32, "The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota."),
            new Field("error_code", Type.INT16, "The top level response error code."),
            new Field("session_id", Type.INT32, "The fetch session ID, or 0 if this is not part of a fetch session."),
            new Field("responses", new ArrayOf(FetchableTopicResponse.SCHEMA_11), "The response topics.")
        );
    
    public static final Schema SCHEMA_12 =
        new Schema(
            new Field("throttle_time_ms", Type.INT32, "The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota."),
            new Field("error_code", Type.INT16, "The top level response error code."),
            new Field("session_id", Type.INT32, "The fetch session ID, or 0 if this is not part of a fetch session."),
            new Field("responses", new CompactArrayOf(FetchableTopicResponse.SCHEMA_12), "The response topics."),
            TaggedFieldsSection.of(
            )
        );
    
    public static final Schema[] SCHEMAS = new Schema[] {
        SCHEMA_0,
        SCHEMA_1,
        SCHEMA_2,
        SCHEMA_3,
        SCHEMA_4,
        SCHEMA_5,
        SCHEMA_6,
        SCHEMA_7,
        SCHEMA_8,
        SCHEMA_9,
        SCHEMA_10,
        SCHEMA_11,
        SCHEMA_12
    };
    
    public static final short LOWEST_SUPPORTED_VERSION = 0;
    public static final short HIGHEST_SUPPORTED_VERSION = 12;
    
    public FetchResponseData(Readable _readable, short _version) {
        read(_readable, _version);
    }
    
    public FetchResponseData(Struct _struct, short _version) {
        fromStruct(_struct, _version);
    }
    
    public FetchResponseData() {
        this.throttleTimeMs = 0;
        this.errorCode = (short) 0;
        this.sessionId = 0;
        this.responses = new ArrayList<FetchableTopicResponse>(0);
    }
    
    @Override
    public short apiKey() {
        return 1;
    }
    
    @Override
    public short lowestSupportedVersion() {
        return 0;
    }
    
    @Override
    public short highestSupportedVersion() {
        return 12;
    }
    
    @Override
    public void read(Readable _readable, short _version) {
        if (_version >= 1) {
            this.throttleTimeMs = _readable.readInt();
        } else {
            this.throttleTimeMs = 0;
        }
        if (_version >= 7) {
            this.errorCode = _readable.readShort();
        } else {
            this.errorCode = (short) 0;
        }
        if (_version >= 7) {
            this.sessionId = _readable.readInt();
        } else {
            this.sessionId = 0;
        }
        {
            if (_version >= 12) {
                int arrayLength;
                arrayLength = _readable.readUnsignedVarint() - 1;
                if (arrayLength < 0) {
                    throw new RuntimeException("non-nullable field responses was serialized as null");
                } else {
                    ArrayList<FetchableTopicResponse> newCollection = new ArrayList<FetchableTopicResponse>(arrayLength);
                    for (int i = 0; i < arrayLength; i++) {
                        newCollection.add(new FetchableTopicResponse(_readable, _version));
                    }
                    this.responses = newCollection;
                }
            } else {
                int arrayLength;
                arrayLength = _readable.readInt();
                if (arrayLength < 0) {
                    throw new RuntimeException("non-nullable field responses was serialized as null");
                } else {
                    ArrayList<FetchableTopicResponse> newCollection = new ArrayList<FetchableTopicResponse>(arrayLength);
                    for (int i = 0; i < arrayLength; i++) {
                        newCollection.add(new FetchableTopicResponse(_readable, _version));
                    }
                    this.responses = newCollection;
                }
            }
        }
        this._unknownTaggedFields = null;
        if (_version >= 12) {
            int _numTaggedFields = _readable.readUnsignedVarint();
            for (int _i = 0; _i < _numTaggedFields; _i++) {
                int _tag = _readable.readUnsignedVarint();
                int _size = _readable.readUnsignedVarint();
                switch (_tag) {
                    default:
                        this._unknownTaggedFields = _readable.readUnknownTaggedField(this._unknownTaggedFields, _tag, _size);
                        break;
                }
            }
        }
    }
    
    @Override
    public void write(Writable _writable, ObjectSerializationCache _cache, short _version) {
        int _numTaggedFields = 0;
        if (_version >= 1) {
            _writable.writeInt(throttleTimeMs);
        }
        if (_version >= 7) {
            _writable.writeShort(errorCode);
        }
        if (_version >= 7) {
            _writable.writeInt(sessionId);
        } else {
            if (this.sessionId != 0) {
                throw new UnsupportedVersionException("Attempted to write a non-default sessionId at version " + _version);
            }
        }
        if (_version >= 12) {
            _writable.writeUnsignedVarint(responses.size() + 1);
            for (FetchableTopicResponse responsesElement : responses) {
                responsesElement.write(_writable, _cache, _version);
            }
        } else {
            _writable.writeInt(responses.size());
            for (FetchableTopicResponse responsesElement : responses) {
                responsesElement.write(_writable, _cache, _version);
            }
        }
        RawTaggedFieldWriter _rawWriter = RawTaggedFieldWriter.forFields(_unknownTaggedFields);
        _numTaggedFields += _rawWriter.numFields();
        if (_version >= 12) {
            _writable.writeUnsignedVarint(_numTaggedFields);
            _rawWriter.writeRawTags(_writable, Integer.MAX_VALUE);
        } else {
            if (_numTaggedFields > 0) {
                throw new UnsupportedVersionException("Tagged fields were set, but version " + _version + " of this message does not support them.");
            }
        }
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public void fromStruct(Struct struct, short _version) {
        NavigableMap<Integer, Object> _taggedFields = null;
        this._unknownTaggedFields = null;
        if (_version >= 12) {
            _taggedFields = (NavigableMap<Integer, Object>) struct.get("_tagged_fields");
        }
        if (_version >= 1) {
            this.throttleTimeMs = struct.getInt("throttle_time_ms");
        } else {
            this.throttleTimeMs = 0;
        }
        if (_version >= 7) {
            this.errorCode = struct.getShort("error_code");
        } else {
            this.errorCode = (short) 0;
        }
        if (_version >= 7) {
            this.sessionId = struct.getInt("session_id");
        } else {
            this.sessionId = 0;
        }
        {
            Object[] _nestedObjects = struct.getArray("responses");
            this.responses = new ArrayList<FetchableTopicResponse>(_nestedObjects.length);
            for (Object nestedObject : _nestedObjects) {
                this.responses.add(new FetchableTopicResponse((Struct) nestedObject, _version));
            }
        }
        if (_version >= 12) {
            if (!_taggedFields.isEmpty()) {
                this._unknownTaggedFields = new ArrayList<>(_taggedFields.size());
                for (Entry<Integer, Object> entry : _taggedFields.entrySet()) {
                    this._unknownTaggedFields.add((RawTaggedField) entry.getValue());
                }
            }
        }
    }
    
    @Override
    public Struct toStruct(short _version) {
        TreeMap<Integer, Object> _taggedFields = null;
        if (_version >= 12) {
            _taggedFields = new TreeMap<>();
        }
        Struct struct = new Struct(SCHEMAS[_version]);
        if (_version >= 1) {
            struct.set("throttle_time_ms", this.throttleTimeMs);
        }
        if (_version >= 7) {
            struct.set("error_code", this.errorCode);
        }
        if (_version >= 7) {
            struct.set("session_id", this.sessionId);
        } else {
            if (this.sessionId != 0) {
                throw new UnsupportedVersionException("Attempted to write a non-default sessionId at version " + _version);
            }
        }
        {
            Struct[] _nestedObjects = new Struct[responses.size()];
            int i = 0;
            for (FetchableTopicResponse element : this.responses) {
                _nestedObjects[i++] = element.toStruct(_version);
            }
            struct.set("responses", (Object[]) _nestedObjects);
        }
        if (_version >= 12) {
            struct.set("_tagged_fields", _taggedFields);
        }
        return struct;
    }
    
    @Override
    public int size(ObjectSerializationCache _cache, short _version) {
        int _size = 0, _numTaggedFields = 0;
        if (_version >= 1) {
            _size += 4;
        }
        if (_version >= 7) {
            _size += 2;
        }
        if (_version >= 7) {
            _size += 4;
        }
        {
            int _arraySize = 0;
            if (_version >= 12) {
                _arraySize += ByteUtils.sizeOfUnsignedVarint(responses.size() + 1);
            } else {
                _arraySize += 4;
            }
            for (FetchableTopicResponse responsesElement : responses) {
                _arraySize += responsesElement.size(_cache, _version);
            }
            _size += _arraySize;
        }
        if (_unknownTaggedFields != null) {
            _numTaggedFields += _unknownTaggedFields.size();
            for (RawTaggedField _field : _unknownTaggedFields) {
                _size += ByteUtils.sizeOfUnsignedVarint(_field.tag());
                _size += ByteUtils.sizeOfUnsignedVarint(_field.size());
                _size += _field.size();
            }
        }
        if (_version >= 12) {
            _size += ByteUtils.sizeOfUnsignedVarint(_numTaggedFields);
        } else {
            if (_numTaggedFields > 0) {
                throw new UnsupportedVersionException("Tagged fields were set, but version " + _version + " of this message does not support them.");
            }
        }
        return _size;
    }
    
    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof FetchResponseData)) return false;
        FetchResponseData other = (FetchResponseData) obj;
        if (throttleTimeMs != other.throttleTimeMs) return false;
        if (errorCode != other.errorCode) return false;
        if (sessionId != other.sessionId) return false;
        if (this.responses == null) {
            if (other.responses != null) return false;
        } else {
            if (!this.responses.equals(other.responses)) return false;
        }
        return MessageUtil.compareRawTaggedFields(_unknownTaggedFields, other._unknownTaggedFields);
    }
    
    @Override
    public int hashCode() {
        int hashCode = 0;
        hashCode = 31 * hashCode + throttleTimeMs;
        hashCode = 31 * hashCode + errorCode;
        hashCode = 31 * hashCode + sessionId;
        hashCode = 31 * hashCode + (responses == null ? 0 : responses.hashCode());
        return hashCode;
    }
    
    @Override
    public FetchResponseData duplicate() {
        FetchResponseData _duplicate = new FetchResponseData();
        _duplicate.throttleTimeMs = throttleTimeMs;
        _duplicate.errorCode = errorCode;
        _duplicate.sessionId = sessionId;
        ArrayList<FetchableTopicResponse> newResponses = new ArrayList<FetchableTopicResponse>(responses.size());
        for (FetchableTopicResponse _element : responses) {
            newResponses.add(_element.duplicate());
        }
        _duplicate.responses = newResponses;
        return _duplicate;
    }
    
    @Override
    public String toString() {
        return "FetchResponseData("
            + "throttleTimeMs=" + throttleTimeMs
            + ", errorCode=" + errorCode
            + ", sessionId=" + sessionId
            + ", responses=" + MessageUtil.deepToString(responses.iterator())
            + ")";
    }
    
    public int throttleTimeMs() {
        return this.throttleTimeMs;
    }
    
    public short errorCode() {
        return this.errorCode;
    }
    
    public int sessionId() {
        return this.sessionId;
    }
    
    public List<FetchableTopicResponse> responses() {
        return this.responses;
    }
    
    @Override
    public List<RawTaggedField> unknownTaggedFields() {
        if (_unknownTaggedFields == null) {
            _unknownTaggedFields = new ArrayList<>(0);
        }
        return _unknownTaggedFields;
    }
    
    public FetchResponseData setThrottleTimeMs(int v) {
        this.throttleTimeMs = v;
        return this;
    }
    
    public FetchResponseData setErrorCode(short v) {
        this.errorCode = v;
        return this;
    }
    
    public FetchResponseData setSessionId(int v) {
        this.sessionId = v;
        return this;
    }
    
    public FetchResponseData setResponses(List<FetchableTopicResponse> v) {
        this.responses = v;
        return this;
    }
    
    public static class FetchableTopicResponse implements Message {
        String topic;
        List<FetchablePartitionResponse> partitionResponses;
        private List<RawTaggedField> _unknownTaggedFields;
        
        public static final Schema SCHEMA_0 =
            new Schema(
                new Field("topic", Type.STRING, "The topic name."),
                new Field("partition_responses", new ArrayOf(FetchablePartitionResponse.SCHEMA_0), "The topic partitions.")
            );
        
        public static final Schema SCHEMA_1 = SCHEMA_0;
        
        public static final Schema SCHEMA_2 = SCHEMA_1;
        
        public static final Schema SCHEMA_3 = SCHEMA_2;
        
        public static final Schema SCHEMA_4 =
            new Schema(
                new Field("topic", Type.STRING, "The topic name."),
                new Field("partition_responses", new ArrayOf(FetchablePartitionResponse.SCHEMA_4), "The topic partitions.")
            );
        
        public static final Schema SCHEMA_5 =
            new Schema(
                new Field("topic", Type.STRING, "The topic name."),
                new Field("partition_responses", new ArrayOf(FetchablePartitionResponse.SCHEMA_5), "The topic partitions.")
            );
        
        public static final Schema SCHEMA_6 = SCHEMA_5;
        
        public static final Schema SCHEMA_7 = SCHEMA_6;
        
        public static final Schema SCHEMA_8 = SCHEMA_7;
        
        public static final Schema SCHEMA_9 = SCHEMA_8;
        
        public static final Schema SCHEMA_10 = SCHEMA_9;
        
        public static final Schema SCHEMA_11 =
            new Schema(
                new Field("topic", Type.STRING, "The topic name."),
                new Field("partition_responses", new ArrayOf(FetchablePartitionResponse.SCHEMA_11), "The topic partitions.")
            );
        
        public static final Schema SCHEMA_12 =
            new Schema(
                new Field("topic", Type.COMPACT_STRING, "The topic name."),
                new Field("partition_responses", new CompactArrayOf(FetchablePartitionResponse.SCHEMA_12), "The topic partitions."),
                TaggedFieldsSection.of(
                )
            );
        
        public static final Schema[] SCHEMAS = new Schema[] {
            SCHEMA_0,
            SCHEMA_1,
            SCHEMA_2,
            SCHEMA_3,
            SCHEMA_4,
            SCHEMA_5,
            SCHEMA_6,
            SCHEMA_7,
            SCHEMA_8,
            SCHEMA_9,
            SCHEMA_10,
            SCHEMA_11,
            SCHEMA_12
        };
        
        public static final short LOWEST_SUPPORTED_VERSION = 0;
        public static final short HIGHEST_SUPPORTED_VERSION = 12;
        
        public FetchableTopicResponse(Readable _readable, short _version) {
            read(_readable, _version);
        }
        
        public FetchableTopicResponse(Struct _struct, short _version) {
            fromStruct(_struct, _version);
        }
        
        public FetchableTopicResponse() {
            this.topic = "";
            this.partitionResponses = new ArrayList<FetchablePartitionResponse>(0);
        }
        
        
        @Override
        public short lowestSupportedVersion() {
            return 0;
        }
        
        @Override
        public short highestSupportedVersion() {
            return 12;
        }
        
        @Override
        public void read(Readable _readable, short _version) {
            if (_version > 12) {
                throw new UnsupportedVersionException("Can't read version " + _version + " of FetchableTopicResponse");
            }
            {
                int length;
                if (_version >= 12) {
                    length = _readable.readUnsignedVarint() - 1;
                } else {
                    length = _readable.readShort();
                }
                if (length < 0) {
                    throw new RuntimeException("non-nullable field topic was serialized as null");
                } else if (length > 0x7fff) {
                    throw new RuntimeException("string field topic had invalid length " + length);
                } else {
                    this.topic = _readable.readString(length);
                }
            }
            {
                if (_version >= 12) {
                    int arrayLength;
                    arrayLength = _readable.readUnsignedVarint() - 1;
                    if (arrayLength < 0) {
                        throw new RuntimeException("non-nullable field partitionResponses was serialized as null");
                    } else {
                        ArrayList<FetchablePartitionResponse> newCollection = new ArrayList<FetchablePartitionResponse>(arrayLength);
                        for (int i = 0; i < arrayLength; i++) {
                            newCollection.add(new FetchablePartitionResponse(_readable, _version));
                        }
                        this.partitionResponses = newCollection;
                    }
                } else {
                    int arrayLength;
                    arrayLength = _readable.readInt();
                    if (arrayLength < 0) {
                        throw new RuntimeException("non-nullable field partitionResponses was serialized as null");
                    } else {
                        ArrayList<FetchablePartitionResponse> newCollection = new ArrayList<FetchablePartitionResponse>(arrayLength);
                        for (int i = 0; i < arrayLength; i++) {
                            newCollection.add(new FetchablePartitionResponse(_readable, _version));
                        }
                        this.partitionResponses = newCollection;
                    }
                }
            }
            this._unknownTaggedFields = null;
            if (_version >= 12) {
                int _numTaggedFields = _readable.readUnsignedVarint();
                for (int _i = 0; _i < _numTaggedFields; _i++) {
                    int _tag = _readable.readUnsignedVarint();
                    int _size = _readable.readUnsignedVarint();
                    switch (_tag) {
                        default:
                            this._unknownTaggedFields = _readable.readUnknownTaggedField(this._unknownTaggedFields, _tag, _size);
                            break;
                    }
                }
            }
        }
        
        @Override
        public void write(Writable _writable, ObjectSerializationCache _cache, short _version) {
            int _numTaggedFields = 0;
            {
                byte[] _stringBytes = _cache.getSerializedValue(topic);
                if (_version >= 12) {
                    _writable.writeUnsignedVarint(_stringBytes.length + 1);
                } else {
                    _writable.writeShort((short) _stringBytes.length);
                }
                _writable.writeByteArray(_stringBytes);
            }
            if (_version >= 12) {
                _writable.writeUnsignedVarint(partitionResponses.size() + 1);
                for (FetchablePartitionResponse partitionResponsesElement : partitionResponses) {
                    partitionResponsesElement.write(_writable, _cache, _version);
                }
            } else {
                _writable.writeInt(partitionResponses.size());
                for (FetchablePartitionResponse partitionResponsesElement : partitionResponses) {
                    partitionResponsesElement.write(_writable, _cache, _version);
                }
            }
            RawTaggedFieldWriter _rawWriter = RawTaggedFieldWriter.forFields(_unknownTaggedFields);
            _numTaggedFields += _rawWriter.numFields();
            if (_version >= 12) {
                _writable.writeUnsignedVarint(_numTaggedFields);
                _rawWriter.writeRawTags(_writable, Integer.MAX_VALUE);
            } else {
                if (_numTaggedFields > 0) {
                    throw new UnsupportedVersionException("Tagged fields were set, but version " + _version + " of this message does not support them.");
                }
            }
        }
        
        @SuppressWarnings("unchecked")
        @Override
        public void fromStruct(Struct struct, short _version) {
            if (_version > 12) {
                throw new UnsupportedVersionException("Can't read version " + _version + " of FetchableTopicResponse");
            }
            NavigableMap<Integer, Object> _taggedFields = null;
            this._unknownTaggedFields = null;
            if (_version >= 12) {
                _taggedFields = (NavigableMap<Integer, Object>) struct.get("_tagged_fields");
            }
            this.topic = struct.getString("topic");
            {
                Object[] _nestedObjects = struct.getArray("partition_responses");
                this.partitionResponses = new ArrayList<FetchablePartitionResponse>(_nestedObjects.length);
                for (Object nestedObject : _nestedObjects) {
                    this.partitionResponses.add(new FetchablePartitionResponse((Struct) nestedObject, _version));
                }
            }
            if (_version >= 12) {
                if (!_taggedFields.isEmpty()) {
                    this._unknownTaggedFields = new ArrayList<>(_taggedFields.size());
                    for (Entry<Integer, Object> entry : _taggedFields.entrySet()) {
                        this._unknownTaggedFields.add((RawTaggedField) entry.getValue());
                    }
                }
            }
        }
        
        @Override
        public Struct toStruct(short _version) {
            if (_version > 12) {
                throw new UnsupportedVersionException("Can't write version " + _version + " of FetchableTopicResponse");
            }
            TreeMap<Integer, Object> _taggedFields = null;
            if (_version >= 12) {
                _taggedFields = new TreeMap<>();
            }
            Struct struct = new Struct(SCHEMAS[_version]);
            struct.set("topic", this.topic);
            {
                Struct[] _nestedObjects = new Struct[partitionResponses.size()];
                int i = 0;
                for (FetchablePartitionResponse element : this.partitionResponses) {
                    _nestedObjects[i++] = element.toStruct(_version);
                }
                struct.set("partition_responses", (Object[]) _nestedObjects);
            }
            if (_version >= 12) {
                struct.set("_tagged_fields", _taggedFields);
            }
            return struct;
        }
        
        @Override
        public int size(ObjectSerializationCache _cache, short _version) {
            int _size = 0, _numTaggedFields = 0;
            if (_version > 12) {
                throw new UnsupportedVersionException("Can't size version " + _version + " of FetchableTopicResponse");
            }
            {
                byte[] _stringBytes = topic.getBytes(StandardCharsets.UTF_8);
                if (_stringBytes.length > 0x7fff) {
                    throw new RuntimeException("'topic' field is too long to be serialized");
                }
                _cache.cacheSerializedValue(topic, _stringBytes);
                if (_version >= 12) {
                    _size += _stringBytes.length + ByteUtils.sizeOfUnsignedVarint(_stringBytes.length + 1);
                } else {
                    _size += _stringBytes.length + 2;
                }
            }
            {
                int _arraySize = 0;
                if (_version >= 12) {
                    _arraySize += ByteUtils.sizeOfUnsignedVarint(partitionResponses.size() + 1);
                } else {
                    _arraySize += 4;
                }
                for (FetchablePartitionResponse partitionResponsesElement : partitionResponses) {
                    _arraySize += partitionResponsesElement.size(_cache, _version);
                }
                _size += _arraySize;
            }
            if (_unknownTaggedFields != null) {
                _numTaggedFields += _unknownTaggedFields.size();
                for (RawTaggedField _field : _unknownTaggedFields) {
                    _size += ByteUtils.sizeOfUnsignedVarint(_field.tag());
                    _size += ByteUtils.sizeOfUnsignedVarint(_field.size());
                    _size += _field.size();
                }
            }
            if (_version >= 12) {
                _size += ByteUtils.sizeOfUnsignedVarint(_numTaggedFields);
            } else {
                if (_numTaggedFields > 0) {
                    throw new UnsupportedVersionException("Tagged fields were set, but version " + _version + " of this message does not support them.");
                }
            }
            return _size;
        }
        
        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof FetchableTopicResponse)) return false;
            FetchableTopicResponse other = (FetchableTopicResponse) obj;
            if (this.topic == null) {
                if (other.topic != null) return false;
            } else {
                if (!this.topic.equals(other.topic)) return false;
            }
            if (this.partitionResponses == null) {
                if (other.partitionResponses != null) return false;
            } else {
                if (!this.partitionResponses.equals(other.partitionResponses)) return false;
            }
            return MessageUtil.compareRawTaggedFields(_unknownTaggedFields, other._unknownTaggedFields);
        }
        
        @Override
        public int hashCode() {
            int hashCode = 0;
            hashCode = 31 * hashCode + (topic == null ? 0 : topic.hashCode());
            hashCode = 31 * hashCode + (partitionResponses == null ? 0 : partitionResponses.hashCode());
            return hashCode;
        }
        
        @Override
        public FetchableTopicResponse duplicate() {
            FetchableTopicResponse _duplicate = new FetchableTopicResponse();
            _duplicate.topic = topic;
            ArrayList<FetchablePartitionResponse> newPartitionResponses = new ArrayList<FetchablePartitionResponse>(partitionResponses.size());
            for (FetchablePartitionResponse _element : partitionResponses) {
                newPartitionResponses.add(_element.duplicate());
            }
            _duplicate.partitionResponses = newPartitionResponses;
            return _duplicate;
        }
        
        @Override
        public String toString() {
            return "FetchableTopicResponse("
                + "topic=" + ((topic == null) ? "null" : "'" + topic.toString() + "'")
                + ", partitionResponses=" + MessageUtil.deepToString(partitionResponses.iterator())
                + ")";
        }
        
        public String topic() {
            return this.topic;
        }
        
        public List<FetchablePartitionResponse> partitionResponses() {
            return this.partitionResponses;
        }
        
        @Override
        public List<RawTaggedField> unknownTaggedFields() {
            if (_unknownTaggedFields == null) {
                _unknownTaggedFields = new ArrayList<>(0);
            }
            return _unknownTaggedFields;
        }
        
        public FetchableTopicResponse setTopic(String v) {
            this.topic = v;
            return this;
        }
        
        public FetchableTopicResponse setPartitionResponses(List<FetchablePartitionResponse> v) {
            this.partitionResponses = v;
            return this;
        }
    }
    
    public static class FetchablePartitionResponse implements Message {
        int partition;
        short errorCode;
        long highWatermark;
        long lastStableOffset;
        long logStartOffset;
        EpochEndOffset divergingEpoch;
        LeaderIdAndEpoch currentLeader;
        List<AbortedTransaction> abortedTransactions;
        int preferredReadReplica;
        BaseRecords recordSet;
        private List<RawTaggedField> _unknownTaggedFields;
        
        public static final Schema SCHEMA_0 =
            new Schema(
                new Field("partition", Type.INT32, "The partition index."),
                new Field("error_code", Type.INT16, "The error code, or 0 if there was no fetch error."),
                new Field("high_watermark", Type.INT64, "The current high water mark."),
                new Field("record_set", Type.RECORDS, "The record data.")
            );
        
        public static final Schema SCHEMA_1 = SCHEMA_0;
        
        public static final Schema SCHEMA_2 = SCHEMA_1;
        
        public static final Schema SCHEMA_3 = SCHEMA_2;
        
        public static final Schema SCHEMA_4 =
            new Schema(
                new Field("partition", Type.INT32, "The partition index."),
                new Field("error_code", Type.INT16, "The error code, or 0 if there was no fetch error."),
                new Field("high_watermark", Type.INT64, "The current high water mark."),
                new Field("last_stable_offset", Type.INT64, "The last stable offset (or LSO) of the partition. This is the last offset such that the state of all transactional records prior to this offset have been decided (ABORTED or COMMITTED)"),
                new Field("aborted_transactions", ArrayOf.nullable(AbortedTransaction.SCHEMA_4), "The aborted transactions."),
                new Field("record_set", Type.RECORDS, "The record data.")
            );
        
        public static final Schema SCHEMA_5 =
            new Schema(
                new Field("partition", Type.INT32, "The partition index."),
                new Field("error_code", Type.INT16, "The error code, or 0 if there was no fetch error."),
                new Field("high_watermark", Type.INT64, "The current high water mark."),
                new Field("last_stable_offset", Type.INT64, "The last stable offset (or LSO) of the partition. This is the last offset such that the state of all transactional records prior to this offset have been decided (ABORTED or COMMITTED)"),
                new Field("log_start_offset", Type.INT64, "The current log start offset."),
                new Field("aborted_transactions", ArrayOf.nullable(AbortedTransaction.SCHEMA_4), "The aborted transactions."),
                new Field("record_set", Type.RECORDS, "The record data.")
            );
        
        public static final Schema SCHEMA_6 = SCHEMA_5;
        
        public static final Schema SCHEMA_7 = SCHEMA_6;
        
        public static final Schema SCHEMA_8 = SCHEMA_7;
        
        public static final Schema SCHEMA_9 = SCHEMA_8;
        
        public static final Schema SCHEMA_10 = SCHEMA_9;
        
        public static final Schema SCHEMA_11 =
            new Schema(
                new Field("partition", Type.INT32, "The partition index."),
                new Field("error_code", Type.INT16, "The error code, or 0 if there was no fetch error."),
                new Field("high_watermark", Type.INT64, "The current high water mark."),
                new Field("last_stable_offset", Type.INT64, "The last stable offset (or LSO) of the partition. This is the last offset such that the state of all transactional records prior to this offset have been decided (ABORTED or COMMITTED)"),
                new Field("log_start_offset", Type.INT64, "The current log start offset."),
                new Field("aborted_transactions", ArrayOf.nullable(AbortedTransaction.SCHEMA_4), "The aborted transactions."),
                new Field("preferred_read_replica", Type.INT32, "The preferred read replica for the consumer to use on its next fetch request"),
                new Field("record_set", Type.RECORDS, "The record data.")
            );
        
        public static final Schema SCHEMA_12 =
            new Schema(
                new Field("partition", Type.INT32, "The partition index."),
                new Field("error_code", Type.INT16, "The error code, or 0 if there was no fetch error."),
                new Field("high_watermark", Type.INT64, "The current high water mark."),
                new Field("last_stable_offset", Type.INT64, "The last stable offset (or LSO) of the partition. This is the last offset such that the state of all transactional records prior to this offset have been decided (ABORTED or COMMITTED)"),
                new Field("log_start_offset", Type.INT64, "The current log start offset."),
                new Field("aborted_transactions", CompactArrayOf.nullable(AbortedTransaction.SCHEMA_12), "The aborted transactions."),
                new Field("preferred_read_replica", Type.INT32, "The preferred read replica for the consumer to use on its next fetch request"),
                new Field("record_set", Type.COMPACT_RECORDS, "The record data."),
                TaggedFieldsSection.of(
                    0, new Field("diverging_epoch", EpochEndOffset.SCHEMA_12, "In case divergence is detected based on the `LastFetchedEpoch` and `FetchOffset` in the request, this field indicates the largest epoch and its end offset such that subsequent records are known to diverge"),
                    1, new Field("current_leader", LeaderIdAndEpoch.SCHEMA_12, "")
                )
            );
        
        public static final Schema[] SCHEMAS = new Schema[] {
            SCHEMA_0,
            SCHEMA_1,
            SCHEMA_2,
            SCHEMA_3,
            SCHEMA_4,
            SCHEMA_5,
            SCHEMA_6,
            SCHEMA_7,
            SCHEMA_8,
            SCHEMA_9,
            SCHEMA_10,
            SCHEMA_11,
            SCHEMA_12
        };
        
        public static final short LOWEST_SUPPORTED_VERSION = 0;
        public static final short HIGHEST_SUPPORTED_VERSION = 12;
        
        public FetchablePartitionResponse(Readable _readable, short _version) {
            read(_readable, _version);
        }
        
        public FetchablePartitionResponse(Struct _struct, short _version) {
            fromStruct(_struct, _version);
        }
        
        public FetchablePartitionResponse() {
            this.partition = 0;
            this.errorCode = (short) 0;
            this.highWatermark = 0L;
            this.lastStableOffset = -1L;
            this.logStartOffset = -1L;
            this.divergingEpoch = new EpochEndOffset();
            this.currentLeader = new LeaderIdAndEpoch();
            this.abortedTransactions = new ArrayList<AbortedTransaction>(0);
            this.preferredReadReplica = -1;
            this.recordSet = null;
        }
        
        
        @Override
        public short lowestSupportedVersion() {
            return 0;
        }
        
        @Override
        public short highestSupportedVersion() {
            return 12;
        }
        
        @Override
        public void read(Readable _readable, short _version) {
            if (_version > 12) {
                throw new UnsupportedVersionException("Can't read version " + _version + " of FetchablePartitionResponse");
            }
            this.partition = _readable.readInt();
            this.errorCode = _readable.readShort();
            this.highWatermark = _readable.readLong();
            if (_version >= 4) {
                this.lastStableOffset = _readable.readLong();
            } else {
                this.lastStableOffset = -1L;
            }
            if (_version >= 5) {
                this.logStartOffset = _readable.readLong();
            } else {
                this.logStartOffset = -1L;
            }
            {
                this.divergingEpoch = new EpochEndOffset();
            }
            {
                this.currentLeader = new LeaderIdAndEpoch();
            }
            if (_version >= 4) {
                if (_version >= 12) {
                    int arrayLength;
                    arrayLength = _readable.readUnsignedVarint() - 1;
                    if (arrayLength < 0) {
                        this.abortedTransactions = null;
                    } else {
                        ArrayList<AbortedTransaction> newCollection = new ArrayList<AbortedTransaction>(arrayLength);
                        for (int i = 0; i < arrayLength; i++) {
                            newCollection.add(new AbortedTransaction(_readable, _version));
                        }
                        this.abortedTransactions = newCollection;
                    }
                } else {
                    int arrayLength;
                    arrayLength = _readable.readInt();
                    if (arrayLength < 0) {
                        this.abortedTransactions = null;
                    } else {
                        ArrayList<AbortedTransaction> newCollection = new ArrayList<AbortedTransaction>(arrayLength);
                        for (int i = 0; i < arrayLength; i++) {
                            newCollection.add(new AbortedTransaction(_readable, _version));
                        }
                        this.abortedTransactions = newCollection;
                    }
                }
            } else {
                this.abortedTransactions = new ArrayList<AbortedTransaction>(0);
            }
            if (_version >= 11) {
                this.preferredReadReplica = _readable.readInt();
            } else {
                this.preferredReadReplica = -1;
            }
            {
                int length;
                if (_version >= 12) {
                    length = _readable.readUnsignedVarint() - 1;
                } else {
                    length = _readable.readInt();
                }
                if (length < 0) {
                    this.recordSet = null;
                } else {
                    if (_readable instanceof RecordsReadable) {
                        this.recordSet = ((RecordsReadable) _readable).readRecords(length);
                    } else {
                        throw new RuntimeException("Cannot read records from reader of class: " + _readable.getClass().getSimpleName());
                    }
                }
            }
            this._unknownTaggedFields = null;
            if (_version >= 12) {
                int _numTaggedFields = _readable.readUnsignedVarint();
                for (int _i = 0; _i < _numTaggedFields; _i++) {
                    int _tag = _readable.readUnsignedVarint();
                    int _size = _readable.readUnsignedVarint();
                    switch (_tag) {
                        case 0: {
                            this.divergingEpoch = new EpochEndOffset(_readable, _version);
                            break;
                        }
                        case 1: {
                            this.currentLeader = new LeaderIdAndEpoch(_readable, _version);
                            break;
                        }
                        default:
                            this._unknownTaggedFields = _readable.readUnknownTaggedField(this._unknownTaggedFields, _tag, _size);
                            break;
                    }
                }
            }
        }
        
        @Override
        public void write(Writable _writable, ObjectSerializationCache _cache, short _version) {
            int _numTaggedFields = 0;
            _writable.writeInt(partition);
            _writable.writeShort(errorCode);
            _writable.writeLong(highWatermark);
            if (_version >= 4) {
                _writable.writeLong(lastStableOffset);
            }
            if (_version >= 5) {
                _writable.writeLong(logStartOffset);
            }
            if (_version >= 12) {
                if (!this.divergingEpoch.equals(new EpochEndOffset())) {
                    _numTaggedFields++;
                }
            } else {
                if (!this.divergingEpoch.equals(new EpochEndOffset())) {
                    throw new UnsupportedVersionException("Attempted to write a non-default divergingEpoch at version " + _version);
                }
            }
            if (_version >= 12) {
                if (!this.currentLeader.equals(new LeaderIdAndEpoch())) {
                    _numTaggedFields++;
                }
            } else {
                if (!this.currentLeader.equals(new LeaderIdAndEpoch())) {
                    throw new UnsupportedVersionException("Attempted to write a non-default currentLeader at version " + _version);
                }
            }
            if (_version >= 4) {
                if (_version >= 12) {
                    if (abortedTransactions == null) {
                        _writable.writeUnsignedVarint(0);
                    } else {
                        _writable.writeUnsignedVarint(abortedTransactions.size() + 1);
                        for (AbortedTransaction abortedTransactionsElement : abortedTransactions) {
                            abortedTransactionsElement.write(_writable, _cache, _version);
                        }
                    }
                } else {
                    if (abortedTransactions == null) {
                        _writable.writeInt(-1);
                    } else {
                        _writable.writeInt(abortedTransactions.size());
                        for (AbortedTransaction abortedTransactionsElement : abortedTransactions) {
                            abortedTransactionsElement.write(_writable, _cache, _version);
                        }
                    }
                }
            }
            if (_version >= 11) {
                _writable.writeInt(preferredReadReplica);
            } else {
                if (this.preferredReadReplica != -1) {
                    throw new UnsupportedVersionException("Attempted to write a non-default preferredReadReplica at version " + _version);
                }
            }
            if (recordSet == null) {
                if (_version >= 12) {
                    _writable.writeUnsignedVarint(0);
                } else {
                    _writable.writeInt(-1);
                }
            } else {
                if (_version >= 12) {
                    _writable.writeUnsignedVarint(recordSet.sizeInBytes() + 1);
                } else {
                    _writable.writeInt(recordSet.sizeInBytes());
                }
                if (_writable instanceof RecordsWritable) {
                    ((RecordsWritable) _writable).writeRecords(recordSet);
                } else {
                    throw new RuntimeException("Cannot write records to writer of class: " + _writable.getClass().getSimpleName());
                }
            }
            RawTaggedFieldWriter _rawWriter = RawTaggedFieldWriter.forFields(_unknownTaggedFields);
            _numTaggedFields += _rawWriter.numFields();
            if (_version >= 12) {
                _writable.writeUnsignedVarint(_numTaggedFields);
                {
                    if (!this.divergingEpoch.equals(new EpochEndOffset())) {
                        _writable.writeUnsignedVarint(0);
                        _writable.writeUnsignedVarint(this.divergingEpoch.size(_cache, _version));
                        divergingEpoch.write(_writable, _cache, _version);
                    }
                }
                {
                    if (!this.currentLeader.equals(new LeaderIdAndEpoch())) {
                        _writable.writeUnsignedVarint(1);
                        _writable.writeUnsignedVarint(this.currentLeader.size(_cache, _version));
                        currentLeader.write(_writable, _cache, _version);
                    }
                }
                _rawWriter.writeRawTags(_writable, Integer.MAX_VALUE);
            } else {
                if (_numTaggedFields > 0) {
                    throw new UnsupportedVersionException("Tagged fields were set, but version " + _version + " of this message does not support them.");
                }
            }
        }
        
        @SuppressWarnings("unchecked")
        @Override
        public void fromStruct(Struct struct, short _version) {
            if (_version > 12) {
                throw new UnsupportedVersionException("Can't read version " + _version + " of FetchablePartitionResponse");
            }
            NavigableMap<Integer, Object> _taggedFields = null;
            this._unknownTaggedFields = null;
            if (_version >= 12) {
                _taggedFields = (NavigableMap<Integer, Object>) struct.get("_tagged_fields");
            }
            this.partition = struct.getInt("partition");
            this.errorCode = struct.getShort("error_code");
            this.highWatermark = struct.getLong("high_watermark");
            if (_version >= 4) {
                this.lastStableOffset = struct.getLong("last_stable_offset");
            } else {
                this.lastStableOffset = -1L;
            }
            if (_version >= 5) {
                this.logStartOffset = struct.getLong("log_start_offset");
            } else {
                this.logStartOffset = -1L;
            }
            if (_version >= 12) {
                if (_taggedFields.containsKey(0)) {
                    this.divergingEpoch = new EpochEndOffset((Struct) _taggedFields.remove(0), _version);
                } else {
                    this.divergingEpoch = new EpochEndOffset();
                }
            } else {
                this.divergingEpoch = new EpochEndOffset();
            }
            if (_version >= 12) {
                if (_taggedFields.containsKey(1)) {
                    this.currentLeader = new LeaderIdAndEpoch((Struct) _taggedFields.remove(1), _version);
                } else {
                    this.currentLeader = new LeaderIdAndEpoch();
                }
            } else {
                this.currentLeader = new LeaderIdAndEpoch();
            }
            if (_version >= 4) {
                Object[] _nestedObjects = struct.getArray("aborted_transactions");
                if (_nestedObjects == null) {
                    this.abortedTransactions = null;
                } else {
                    this.abortedTransactions = new ArrayList<AbortedTransaction>(_nestedObjects.length);
                    for (Object nestedObject : _nestedObjects) {
                        this.abortedTransactions.add(new AbortedTransaction((Struct) nestedObject, _version));
                    }
                }
            } else {
                this.abortedTransactions = new ArrayList<AbortedTransaction>(0);
            }
            if (_version >= 11) {
                this.preferredReadReplica = struct.getInt("preferred_read_replica");
            } else {
                this.preferredReadReplica = -1;
            }
            this.recordSet = struct.getRecords("record_set");
            if (_version >= 12) {
                if (!_taggedFields.isEmpty()) {
                    this._unknownTaggedFields = new ArrayList<>(_taggedFields.size());
                    for (Entry<Integer, Object> entry : _taggedFields.entrySet()) {
                        this._unknownTaggedFields.add((RawTaggedField) entry.getValue());
                    }
                }
            }
        }
        
        @Override
        public Struct toStruct(short _version) {
            if (_version > 12) {
                throw new UnsupportedVersionException("Can't write version " + _version + " of FetchablePartitionResponse");
            }
            TreeMap<Integer, Object> _taggedFields = null;
            if (_version >= 12) {
                _taggedFields = new TreeMap<>();
            }
            Struct struct = new Struct(SCHEMAS[_version]);
            struct.set("partition", this.partition);
            struct.set("error_code", this.errorCode);
            struct.set("high_watermark", this.highWatermark);
            if (_version >= 4) {
                struct.set("last_stable_offset", this.lastStableOffset);
            }
            if (_version >= 5) {
                struct.set("log_start_offset", this.logStartOffset);
            }
            if (_version >= 12) {
                if (!this.divergingEpoch.equals(new EpochEndOffset())) {
                    _taggedFields.put(0, divergingEpoch.toStruct(_version));
                }
            } else {
                if (!this.divergingEpoch.equals(new EpochEndOffset())) {
                    throw new UnsupportedVersionException("Attempted to write a non-default divergingEpoch at version " + _version);
                }
            }
            if (_version >= 12) {
                if (!this.currentLeader.equals(new LeaderIdAndEpoch())) {
                    _taggedFields.put(1, currentLeader.toStruct(_version));
                }
            } else {
                if (!this.currentLeader.equals(new LeaderIdAndEpoch())) {
                    throw new UnsupportedVersionException("Attempted to write a non-default currentLeader at version " + _version);
                }
            }
            if (_version >= 4) {
                if (abortedTransactions == null) {
                    struct.set("aborted_transactions", null);
                } else {
                    Struct[] _nestedObjects = new Struct[abortedTransactions.size()];
                    int i = 0;
                    for (AbortedTransaction element : this.abortedTransactions) {
                        _nestedObjects[i++] = element.toStruct(_version);
                    }
                    struct.set("aborted_transactions", (Object[]) _nestedObjects);
                }
            }
            if (_version >= 11) {
                struct.set("preferred_read_replica", this.preferredReadReplica);
            } else {
                if (this.preferredReadReplica != -1) {
                    throw new UnsupportedVersionException("Attempted to write a non-default preferredReadReplica at version " + _version);
                }
            }
            struct.set("record_set", this.recordSet);
            if (_version >= 12) {
                struct.set("_tagged_fields", _taggedFields);
            }
            return struct;
        }
        
        @Override
        public int size(ObjectSerializationCache _cache, short _version) {
            int _size = 0, _numTaggedFields = 0;
            if (_version > 12) {
                throw new UnsupportedVersionException("Can't size version " + _version + " of FetchablePartitionResponse");
            }
            _size += 4;
            _size += 2;
            _size += 8;
            if (_version >= 4) {
                _size += 8;
            }
            if (_version >= 5) {
                _size += 8;
            }
            if (_version >= 12) {
                {
                    if (!this.divergingEpoch.equals(new EpochEndOffset())) {
                        _numTaggedFields++;
                        _size += 1;
                        int size = this.divergingEpoch.size(_cache, _version);
                        _size += ByteUtils.sizeOfUnsignedVarint(size);
                        _size += size;
                    }
                }
            }
            if (_version >= 12) {
                {
                    if (!this.currentLeader.equals(new LeaderIdAndEpoch())) {
                        _numTaggedFields++;
                        _size += 1;
                        int size = this.currentLeader.size(_cache, _version);
                        _size += ByteUtils.sizeOfUnsignedVarint(size);
                        _size += size;
                    }
                }
            }
            if (_version >= 4) {
                if (abortedTransactions == null) {
                    if (_version >= 12) {
                        _size += 1;
                    } else {
                        _size += 4;
                    }
                } else {
                    int _arraySize = 0;
                    if (_version >= 12) {
                        _arraySize += ByteUtils.sizeOfUnsignedVarint(abortedTransactions.size() + 1);
                    } else {
                        _arraySize += 4;
                    }
                    for (AbortedTransaction abortedTransactionsElement : abortedTransactions) {
                        _arraySize += abortedTransactionsElement.size(_cache, _version);
                    }
                    _size += _arraySize;
                }
            }
            if (_version >= 11) {
                _size += 4;
            }
            if (recordSet == null) {
                if (_version >= 12) {
                    _size += 1;
                } else {
                    _size += 4;
                }
            } else {
                int _recordsSize = recordSet.sizeInBytes();
                if (_version >= 12) {
                    _recordsSize += ByteUtils.sizeOfUnsignedVarint(recordSet.sizeInBytes() + 1);
                } else {
                    _recordsSize += 4;
                }
                _size += _recordsSize;
            }
            if (_unknownTaggedFields != null) {
                _numTaggedFields += _unknownTaggedFields.size();
                for (RawTaggedField _field : _unknownTaggedFields) {
                    _size += ByteUtils.sizeOfUnsignedVarint(_field.tag());
                    _size += ByteUtils.sizeOfUnsignedVarint(_field.size());
                    _size += _field.size();
                }
            }
            if (_version >= 12) {
                _size += ByteUtils.sizeOfUnsignedVarint(_numTaggedFields);
            } else {
                if (_numTaggedFields > 0) {
                    throw new UnsupportedVersionException("Tagged fields were set, but version " + _version + " of this message does not support them.");
                }
            }
            return _size;
        }
        
        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof FetchablePartitionResponse)) return false;
            FetchablePartitionResponse other = (FetchablePartitionResponse) obj;
            if (partition != other.partition) return false;
            if (errorCode != other.errorCode) return false;
            if (highWatermark != other.highWatermark) return false;
            if (lastStableOffset != other.lastStableOffset) return false;
            if (logStartOffset != other.logStartOffset) return false;
            if (this.divergingEpoch == null) {
                if (other.divergingEpoch != null) return false;
            } else {
                if (!this.divergingEpoch.equals(other.divergingEpoch)) return false;
            }
            if (this.currentLeader == null) {
                if (other.currentLeader != null) return false;
            } else {
                if (!this.currentLeader.equals(other.currentLeader)) return false;
            }
            if (this.abortedTransactions == null) {
                if (other.abortedTransactions != null) return false;
            } else {
                if (!this.abortedTransactions.equals(other.abortedTransactions)) return false;
            }
            if (preferredReadReplica != other.preferredReadReplica) return false;
            if (!Objects.equals(this.recordSet, other.recordSet)) return false;
            return MessageUtil.compareRawTaggedFields(_unknownTaggedFields, other._unknownTaggedFields);
        }
        
        @Override
        public int hashCode() {
            int hashCode = 0;
            hashCode = 31 * hashCode + partition;
            hashCode = 31 * hashCode + errorCode;
            hashCode = 31 * hashCode + ((int) (highWatermark >> 32) ^ (int) highWatermark);
            hashCode = 31 * hashCode + ((int) (lastStableOffset >> 32) ^ (int) lastStableOffset);
            hashCode = 31 * hashCode + ((int) (logStartOffset >> 32) ^ (int) logStartOffset);
            hashCode = 31 * hashCode + (divergingEpoch == null ? 0 : divergingEpoch.hashCode());
            hashCode = 31 * hashCode + (currentLeader == null ? 0 : currentLeader.hashCode());
            hashCode = 31 * hashCode + (abortedTransactions == null ? 0 : abortedTransactions.hashCode());
            hashCode = 31 * hashCode + preferredReadReplica;
            hashCode = 31 * hashCode + Objects.hashCode(recordSet);
            return hashCode;
        }
        
        @Override
        public FetchablePartitionResponse duplicate() {
            FetchablePartitionResponse _duplicate = new FetchablePartitionResponse();
            _duplicate.partition = partition;
            _duplicate.errorCode = errorCode;
            _duplicate.highWatermark = highWatermark;
            _duplicate.lastStableOffset = lastStableOffset;
            _duplicate.logStartOffset = logStartOffset;
            _duplicate.divergingEpoch = divergingEpoch.duplicate();
            _duplicate.currentLeader = currentLeader.duplicate();
            if (abortedTransactions == null) {
                _duplicate.abortedTransactions = null;
            } else {
                ArrayList<AbortedTransaction> newAbortedTransactions = new ArrayList<AbortedTransaction>(abortedTransactions.size());
                for (AbortedTransaction _element : abortedTransactions) {
                    newAbortedTransactions.add(_element.duplicate());
                }
                _duplicate.abortedTransactions = newAbortedTransactions;
            }
            _duplicate.preferredReadReplica = preferredReadReplica;
            if (recordSet == null) {
                _duplicate.recordSet = null;
            } else {
                _duplicate.recordSet = MemoryRecords.readableRecords(((MemoryRecords) recordSet).buffer().duplicate());
            }
            return _duplicate;
        }
        
        @Override
        public String toString() {
            return "FetchablePartitionResponse("
                + "partition=" + partition
                + ", errorCode=" + errorCode
                + ", highWatermark=" + highWatermark
                + ", lastStableOffset=" + lastStableOffset
                + ", logStartOffset=" + logStartOffset
                + ", abortedTransactions=" + ((abortedTransactions == null) ? "null" : MessageUtil.deepToString(abortedTransactions.iterator()))
                + ", preferredReadReplica=" + preferredReadReplica
                + ", recordSet=" + recordSet
                + ")";
        }
        
        public int partition() {
            return this.partition;
        }
        
        public short errorCode() {
            return this.errorCode;
        }
        
        public long highWatermark() {
            return this.highWatermark;
        }
        
        public long lastStableOffset() {
            return this.lastStableOffset;
        }
        
        public long logStartOffset() {
            return this.logStartOffset;
        }
        
        public EpochEndOffset divergingEpoch() {
            return this.divergingEpoch;
        }
        
        public LeaderIdAndEpoch currentLeader() {
            return this.currentLeader;
        }
        
        public List<AbortedTransaction> abortedTransactions() {
            return this.abortedTransactions;
        }
        
        public int preferredReadReplica() {
            return this.preferredReadReplica;
        }
        
        public BaseRecords recordSet() {
            return this.recordSet;
        }
        
        @Override
        public List<RawTaggedField> unknownTaggedFields() {
            if (_unknownTaggedFields == null) {
                _unknownTaggedFields = new ArrayList<>(0);
            }
            return _unknownTaggedFields;
        }
        
        public FetchablePartitionResponse setPartition(int v) {
            this.partition = v;
            return this;
        }
        
        public FetchablePartitionResponse setErrorCode(short v) {
            this.errorCode = v;
            return this;
        }
        
        public FetchablePartitionResponse setHighWatermark(long v) {
            this.highWatermark = v;
            return this;
        }
        
        public FetchablePartitionResponse setLastStableOffset(long v) {
            this.lastStableOffset = v;
            return this;
        }
        
        public FetchablePartitionResponse setLogStartOffset(long v) {
            this.logStartOffset = v;
            return this;
        }
        
        public FetchablePartitionResponse setDivergingEpoch(EpochEndOffset v) {
            this.divergingEpoch = v;
            return this;
        }
        
        public FetchablePartitionResponse setCurrentLeader(LeaderIdAndEpoch v) {
            this.currentLeader = v;
            return this;
        }
        
        public FetchablePartitionResponse setAbortedTransactions(List<AbortedTransaction> v) {
            this.abortedTransactions = v;
            return this;
        }
        
        public FetchablePartitionResponse setPreferredReadReplica(int v) {
            this.preferredReadReplica = v;
            return this;
        }
        
        public FetchablePartitionResponse setRecordSet(BaseRecords v) {
            this.recordSet = v;
            return this;
        }
    }
    
    public static class EpochEndOffset implements Message {
        int epoch;
        long endOffset;
        private List<RawTaggedField> _unknownTaggedFields;
        
        public static final Schema SCHEMA_12 =
            new Schema(
                new Field("epoch", Type.INT32, ""),
                new Field("end_offset", Type.INT64, ""),
                TaggedFieldsSection.of(
                )
            );
        
        public static final Schema[] SCHEMAS = new Schema[] {
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            SCHEMA_12
        };
        
        public static final short LOWEST_SUPPORTED_VERSION = 12;
        public static final short HIGHEST_SUPPORTED_VERSION = 12;
        
        public EpochEndOffset(Readable _readable, short _version) {
            read(_readable, _version);
        }
        
        public EpochEndOffset(Struct _struct, short _version) {
            fromStruct(_struct, _version);
        }
        
        public EpochEndOffset() {
            this.epoch = -1;
            this.endOffset = -1L;
        }
        
        
        @Override
        public short lowestSupportedVersion() {
            return 0;
        }
        
        @Override
        public short highestSupportedVersion() {
            return 12;
        }
        
        @Override
        public void read(Readable _readable, short _version) {
            if (_version > 12) {
                throw new UnsupportedVersionException("Can't read version " + _version + " of EpochEndOffset");
            }
            this.epoch = _readable.readInt();
            this.endOffset = _readable.readLong();
            this._unknownTaggedFields = null;
            int _numTaggedFields = _readable.readUnsignedVarint();
            for (int _i = 0; _i < _numTaggedFields; _i++) {
                int _tag = _readable.readUnsignedVarint();
                int _size = _readable.readUnsignedVarint();
                switch (_tag) {
                    default:
                        this._unknownTaggedFields = _readable.readUnknownTaggedField(this._unknownTaggedFields, _tag, _size);
                        break;
                }
            }
        }
        
        @Override
        public void write(Writable _writable, ObjectSerializationCache _cache, short _version) {
            if (_version < 12) {
                throw new UnsupportedVersionException("Can't write version " + _version + " of EpochEndOffset");
            }
            int _numTaggedFields = 0;
            _writable.writeInt(epoch);
            _writable.writeLong(endOffset);
            RawTaggedFieldWriter _rawWriter = RawTaggedFieldWriter.forFields(_unknownTaggedFields);
            _numTaggedFields += _rawWriter.numFields();
            _writable.writeUnsignedVarint(_numTaggedFields);
            _rawWriter.writeRawTags(_writable, Integer.MAX_VALUE);
        }
        
        @SuppressWarnings("unchecked")
        @Override
        public void fromStruct(Struct struct, short _version) {
            if (_version > 12) {
                throw new UnsupportedVersionException("Can't read version " + _version + " of EpochEndOffset");
            }
            NavigableMap<Integer, Object> _taggedFields = null;
            this._unknownTaggedFields = null;
            _taggedFields = (NavigableMap<Integer, Object>) struct.get("_tagged_fields");
            this.epoch = struct.getInt("epoch");
            this.endOffset = struct.getLong("end_offset");
            if (!_taggedFields.isEmpty()) {
                this._unknownTaggedFields = new ArrayList<>(_taggedFields.size());
                for (Entry<Integer, Object> entry : _taggedFields.entrySet()) {
                    this._unknownTaggedFields.add((RawTaggedField) entry.getValue());
                }
            }
        }
        
        @Override
        public Struct toStruct(short _version) {
            if (_version > 12) {
                throw new UnsupportedVersionException("Can't write version " + _version + " of EpochEndOffset");
            }
            TreeMap<Integer, Object> _taggedFields = null;
            _taggedFields = new TreeMap<>();
            Struct struct = new Struct(SCHEMAS[_version]);
            struct.set("epoch", this.epoch);
            struct.set("end_offset", this.endOffset);
            struct.set("_tagged_fields", _taggedFields);
            return struct;
        }
        
        @Override
        public int size(ObjectSerializationCache _cache, short _version) {
            int _size = 0, _numTaggedFields = 0;
            if (_version > 12) {
                throw new UnsupportedVersionException("Can't size version " + _version + " of EpochEndOffset");
            }
            _size += 4;
            _size += 8;
            if (_unknownTaggedFields != null) {
                _numTaggedFields += _unknownTaggedFields.size();
                for (RawTaggedField _field : _unknownTaggedFields) {
                    _size += ByteUtils.sizeOfUnsignedVarint(_field.tag());
                    _size += ByteUtils.sizeOfUnsignedVarint(_field.size());
                    _size += _field.size();
                }
            }
            _size += ByteUtils.sizeOfUnsignedVarint(_numTaggedFields);
            return _size;
        }
        
        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof EpochEndOffset)) return false;
            EpochEndOffset other = (EpochEndOffset) obj;
            if (epoch != other.epoch) return false;
            if (endOffset != other.endOffset) return false;
            return MessageUtil.compareRawTaggedFields(_unknownTaggedFields, other._unknownTaggedFields);
        }
        
        @Override
        public int hashCode() {
            int hashCode = 0;
            hashCode = 31 * hashCode + epoch;
            hashCode = 31 * hashCode + ((int) (endOffset >> 32) ^ (int) endOffset);
            return hashCode;
        }
        
        @Override
        public EpochEndOffset duplicate() {
            EpochEndOffset _duplicate = new EpochEndOffset();
            _duplicate.epoch = epoch;
            _duplicate.endOffset = endOffset;
            return _duplicate;
        }
        
        @Override
        public String toString() {
            return "EpochEndOffset("
                + "epoch=" + epoch
                + ", endOffset=" + endOffset
                + ")";
        }
        
        public int epoch() {
            return this.epoch;
        }
        
        public long endOffset() {
            return this.endOffset;
        }
        
        @Override
        public List<RawTaggedField> unknownTaggedFields() {
            if (_unknownTaggedFields == null) {
                _unknownTaggedFields = new ArrayList<>(0);
            }
            return _unknownTaggedFields;
        }
        
        public EpochEndOffset setEpoch(int v) {
            this.epoch = v;
            return this;
        }
        
        public EpochEndOffset setEndOffset(long v) {
            this.endOffset = v;
            return this;
        }
    }
    
    public static class LeaderIdAndEpoch implements Message {
        int leaderId;
        int leaderEpoch;
        private List<RawTaggedField> _unknownTaggedFields;
        
        public static final Schema SCHEMA_12 =
            new Schema(
                new Field("leader_id", Type.INT32, "The ID of the current leader or -1 if the leader is unknown."),
                new Field("leader_epoch", Type.INT32, "The latest known leader epoch"),
                TaggedFieldsSection.of(
                )
            );
        
        public static final Schema[] SCHEMAS = new Schema[] {
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            SCHEMA_12
        };
        
        public static final short LOWEST_SUPPORTED_VERSION = 12;
        public static final short HIGHEST_SUPPORTED_VERSION = 12;
        
        public LeaderIdAndEpoch(Readable _readable, short _version) {
            read(_readable, _version);
        }
        
        public LeaderIdAndEpoch(Struct _struct, short _version) {
            fromStruct(_struct, _version);
        }
        
        public LeaderIdAndEpoch() {
            this.leaderId = -1;
            this.leaderEpoch = -1;
        }
        
        
        @Override
        public short lowestSupportedVersion() {
            return 0;
        }
        
        @Override
        public short highestSupportedVersion() {
            return 12;
        }
        
        @Override
        public void read(Readable _readable, short _version) {
            if (_version > 12) {
                throw new UnsupportedVersionException("Can't read version " + _version + " of LeaderIdAndEpoch");
            }
            this.leaderId = _readable.readInt();
            this.leaderEpoch = _readable.readInt();
            this._unknownTaggedFields = null;
            int _numTaggedFields = _readable.readUnsignedVarint();
            for (int _i = 0; _i < _numTaggedFields; _i++) {
                int _tag = _readable.readUnsignedVarint();
                int _size = _readable.readUnsignedVarint();
                switch (_tag) {
                    default:
                        this._unknownTaggedFields = _readable.readUnknownTaggedField(this._unknownTaggedFields, _tag, _size);
                        break;
                }
            }
        }
        
        @Override
        public void write(Writable _writable, ObjectSerializationCache _cache, short _version) {
            if (_version < 12) {
                throw new UnsupportedVersionException("Can't write version " + _version + " of LeaderIdAndEpoch");
            }
            int _numTaggedFields = 0;
            _writable.writeInt(leaderId);
            _writable.writeInt(leaderEpoch);
            RawTaggedFieldWriter _rawWriter = RawTaggedFieldWriter.forFields(_unknownTaggedFields);
            _numTaggedFields += _rawWriter.numFields();
            _writable.writeUnsignedVarint(_numTaggedFields);
            _rawWriter.writeRawTags(_writable, Integer.MAX_VALUE);
        }
        
        @SuppressWarnings("unchecked")
        @Override
        public void fromStruct(Struct struct, short _version) {
            if (_version > 12) {
                throw new UnsupportedVersionException("Can't read version " + _version + " of LeaderIdAndEpoch");
            }
            NavigableMap<Integer, Object> _taggedFields = null;
            this._unknownTaggedFields = null;
            _taggedFields = (NavigableMap<Integer, Object>) struct.get("_tagged_fields");
            this.leaderId = struct.getInt("leader_id");
            this.leaderEpoch = struct.getInt("leader_epoch");
            if (!_taggedFields.isEmpty()) {
                this._unknownTaggedFields = new ArrayList<>(_taggedFields.size());
                for (Entry<Integer, Object> entry : _taggedFields.entrySet()) {
                    this._unknownTaggedFields.add((RawTaggedField) entry.getValue());
                }
            }
        }
        
        @Override
        public Struct toStruct(short _version) {
            if (_version > 12) {
                throw new UnsupportedVersionException("Can't write version " + _version + " of LeaderIdAndEpoch");
            }
            TreeMap<Integer, Object> _taggedFields = null;
            _taggedFields = new TreeMap<>();
            Struct struct = new Struct(SCHEMAS[_version]);
            struct.set("leader_id", this.leaderId);
            struct.set("leader_epoch", this.leaderEpoch);
            struct.set("_tagged_fields", _taggedFields);
            return struct;
        }
        
        @Override
        public int size(ObjectSerializationCache _cache, short _version) {
            int _size = 0, _numTaggedFields = 0;
            if (_version > 12) {
                throw new UnsupportedVersionException("Can't size version " + _version + " of LeaderIdAndEpoch");
            }
            _size += 4;
            _size += 4;
            if (_unknownTaggedFields != null) {
                _numTaggedFields += _unknownTaggedFields.size();
                for (RawTaggedField _field : _unknownTaggedFields) {
                    _size += ByteUtils.sizeOfUnsignedVarint(_field.tag());
                    _size += ByteUtils.sizeOfUnsignedVarint(_field.size());
                    _size += _field.size();
                }
            }
            _size += ByteUtils.sizeOfUnsignedVarint(_numTaggedFields);
            return _size;
        }
        
        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof LeaderIdAndEpoch)) return false;
            LeaderIdAndEpoch other = (LeaderIdAndEpoch) obj;
            if (leaderId != other.leaderId) return false;
            if (leaderEpoch != other.leaderEpoch) return false;
            return MessageUtil.compareRawTaggedFields(_unknownTaggedFields, other._unknownTaggedFields);
        }
        
        @Override
        public int hashCode() {
            int hashCode = 0;
            hashCode = 31 * hashCode + leaderId;
            hashCode = 31 * hashCode + leaderEpoch;
            return hashCode;
        }
        
        @Override
        public LeaderIdAndEpoch duplicate() {
            LeaderIdAndEpoch _duplicate = new LeaderIdAndEpoch();
            _duplicate.leaderId = leaderId;
            _duplicate.leaderEpoch = leaderEpoch;
            return _duplicate;
        }
        
        @Override
        public String toString() {
            return "LeaderIdAndEpoch("
                + "leaderId=" + leaderId
                + ", leaderEpoch=" + leaderEpoch
                + ")";
        }
        
        public int leaderId() {
            return this.leaderId;
        }
        
        public int leaderEpoch() {
            return this.leaderEpoch;
        }
        
        @Override
        public List<RawTaggedField> unknownTaggedFields() {
            if (_unknownTaggedFields == null) {
                _unknownTaggedFields = new ArrayList<>(0);
            }
            return _unknownTaggedFields;
        }
        
        public LeaderIdAndEpoch setLeaderId(int v) {
            this.leaderId = v;
            return this;
        }
        
        public LeaderIdAndEpoch setLeaderEpoch(int v) {
            this.leaderEpoch = v;
            return this;
        }
    }
    
    public static class AbortedTransaction implements Message {
        long producerId;
        long firstOffset;
        private List<RawTaggedField> _unknownTaggedFields;
        
        public static final Schema SCHEMA_4 =
            new Schema(
                new Field("producer_id", Type.INT64, "The producer id associated with the aborted transaction."),
                new Field("first_offset", Type.INT64, "The first offset in the aborted transaction.")
            );
        
        public static final Schema SCHEMA_5 = SCHEMA_4;
        
        public static final Schema SCHEMA_6 = SCHEMA_5;
        
        public static final Schema SCHEMA_7 = SCHEMA_6;
        
        public static final Schema SCHEMA_8 = SCHEMA_7;
        
        public static final Schema SCHEMA_9 = SCHEMA_8;
        
        public static final Schema SCHEMA_10 = SCHEMA_9;
        
        public static final Schema SCHEMA_11 = SCHEMA_10;
        
        public static final Schema SCHEMA_12 =
            new Schema(
                new Field("producer_id", Type.INT64, "The producer id associated with the aborted transaction."),
                new Field("first_offset", Type.INT64, "The first offset in the aborted transaction."),
                TaggedFieldsSection.of(
                )
            );
        
        public static final Schema[] SCHEMAS = new Schema[] {
            null,
            null,
            null,
            null,
            SCHEMA_4,
            SCHEMA_5,
            SCHEMA_6,
            SCHEMA_7,
            SCHEMA_8,
            SCHEMA_9,
            SCHEMA_10,
            SCHEMA_11,
            SCHEMA_12
        };
        
        public static final short LOWEST_SUPPORTED_VERSION = 4;
        public static final short HIGHEST_SUPPORTED_VERSION = 12;
        
        public AbortedTransaction(Readable _readable, short _version) {
            read(_readable, _version);
        }
        
        public AbortedTransaction(Struct _struct, short _version) {
            fromStruct(_struct, _version);
        }
        
        public AbortedTransaction() {
            this.producerId = 0L;
            this.firstOffset = 0L;
        }
        
        
        @Override
        public short lowestSupportedVersion() {
            return 0;
        }
        
        @Override
        public short highestSupportedVersion() {
            return 12;
        }
        
        @Override
        public void read(Readable _readable, short _version) {
            if (_version > 12) {
                throw new UnsupportedVersionException("Can't read version " + _version + " of AbortedTransaction");
            }
            this.producerId = _readable.readLong();
            this.firstOffset = _readable.readLong();
            this._unknownTaggedFields = null;
            if (_version >= 12) {
                int _numTaggedFields = _readable.readUnsignedVarint();
                for (int _i = 0; _i < _numTaggedFields; _i++) {
                    int _tag = _readable.readUnsignedVarint();
                    int _size = _readable.readUnsignedVarint();
                    switch (_tag) {
                        default:
                            this._unknownTaggedFields = _readable.readUnknownTaggedField(this._unknownTaggedFields, _tag, _size);
                            break;
                    }
                }
            }
        }
        
        @Override
        public void write(Writable _writable, ObjectSerializationCache _cache, short _version) {
            if (_version < 4) {
                throw new UnsupportedVersionException("Can't write version " + _version + " of AbortedTransaction");
            }
            int _numTaggedFields = 0;
            _writable.writeLong(producerId);
            _writable.writeLong(firstOffset);
            RawTaggedFieldWriter _rawWriter = RawTaggedFieldWriter.forFields(_unknownTaggedFields);
            _numTaggedFields += _rawWriter.numFields();
            if (_version >= 12) {
                _writable.writeUnsignedVarint(_numTaggedFields);
                _rawWriter.writeRawTags(_writable, Integer.MAX_VALUE);
            } else {
                if (_numTaggedFields > 0) {
                    throw new UnsupportedVersionException("Tagged fields were set, but version " + _version + " of this message does not support them.");
                }
            }
        }
        
        @SuppressWarnings("unchecked")
        @Override
        public void fromStruct(Struct struct, short _version) {
            if (_version > 12) {
                throw new UnsupportedVersionException("Can't read version " + _version + " of AbortedTransaction");
            }
            NavigableMap<Integer, Object> _taggedFields = null;
            this._unknownTaggedFields = null;
            if (_version >= 12) {
                _taggedFields = (NavigableMap<Integer, Object>) struct.get("_tagged_fields");
            }
            this.producerId = struct.getLong("producer_id");
            this.firstOffset = struct.getLong("first_offset");
            if (_version >= 12) {
                if (!_taggedFields.isEmpty()) {
                    this._unknownTaggedFields = new ArrayList<>(_taggedFields.size());
                    for (Entry<Integer, Object> entry : _taggedFields.entrySet()) {
                        this._unknownTaggedFields.add((RawTaggedField) entry.getValue());
                    }
                }
            }
        }
        
        @Override
        public Struct toStruct(short _version) {
            if (_version > 12) {
                throw new UnsupportedVersionException("Can't write version " + _version + " of AbortedTransaction");
            }
            TreeMap<Integer, Object> _taggedFields = null;
            if (_version >= 12) {
                _taggedFields = new TreeMap<>();
            }
            Struct struct = new Struct(SCHEMAS[_version]);
            struct.set("producer_id", this.producerId);
            struct.set("first_offset", this.firstOffset);
            if (_version >= 12) {
                struct.set("_tagged_fields", _taggedFields);
            }
            return struct;
        }
        
        @Override
        public int size(ObjectSerializationCache _cache, short _version) {
            int _size = 0, _numTaggedFields = 0;
            if (_version > 12) {
                throw new UnsupportedVersionException("Can't size version " + _version + " of AbortedTransaction");
            }
            _size += 8;
            _size += 8;
            if (_unknownTaggedFields != null) {
                _numTaggedFields += _unknownTaggedFields.size();
                for (RawTaggedField _field : _unknownTaggedFields) {
                    _size += ByteUtils.sizeOfUnsignedVarint(_field.tag());
                    _size += ByteUtils.sizeOfUnsignedVarint(_field.size());
                    _size += _field.size();
                }
            }
            if (_version >= 12) {
                _size += ByteUtils.sizeOfUnsignedVarint(_numTaggedFields);
            } else {
                if (_numTaggedFields > 0) {
                    throw new UnsupportedVersionException("Tagged fields were set, but version " + _version + " of this message does not support them.");
                }
            }
            return _size;
        }
        
        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof AbortedTransaction)) return false;
            AbortedTransaction other = (AbortedTransaction) obj;
            if (producerId != other.producerId) return false;
            if (firstOffset != other.firstOffset) return false;
            return MessageUtil.compareRawTaggedFields(_unknownTaggedFields, other._unknownTaggedFields);
        }
        
        @Override
        public int hashCode() {
            int hashCode = 0;
            hashCode = 31 * hashCode + ((int) (producerId >> 32) ^ (int) producerId);
            hashCode = 31 * hashCode + ((int) (firstOffset >> 32) ^ (int) firstOffset);
            return hashCode;
        }
        
        @Override
        public AbortedTransaction duplicate() {
            AbortedTransaction _duplicate = new AbortedTransaction();
            _duplicate.producerId = producerId;
            _duplicate.firstOffset = firstOffset;
            return _duplicate;
        }
        
        @Override
        public String toString() {
            return "AbortedTransaction("
                + "producerId=" + producerId
                + ", firstOffset=" + firstOffset
                + ")";
        }
        
        public long producerId() {
            return this.producerId;
        }
        
        public long firstOffset() {
            return this.firstOffset;
        }
        
        @Override
        public List<RawTaggedField> unknownTaggedFields() {
            if (_unknownTaggedFields == null) {
                _unknownTaggedFields = new ArrayList<>(0);
            }
            return _unknownTaggedFields;
        }
        
        public AbortedTransaction setProducerId(long v) {
            this.producerId = v;
            return this;
        }
        
        public AbortedTransaction setFirstOffset(long v) {
            this.firstOffset = v;
            return this;
        }
    }
}
