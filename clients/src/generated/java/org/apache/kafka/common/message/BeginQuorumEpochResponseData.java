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
import java.util.TreeMap;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Message;
import org.apache.kafka.common.protocol.MessageUtil;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.protocol.Readable;
import org.apache.kafka.common.protocol.Writable;
import org.apache.kafka.common.protocol.types.ArrayOf;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.RawTaggedField;
import org.apache.kafka.common.protocol.types.RawTaggedFieldWriter;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.protocol.types.Type;
import org.apache.kafka.common.utils.ByteUtils;


public class BeginQuorumEpochResponseData implements ApiMessage {
    short errorCode;
    List<TopicData> topics;
    private List<RawTaggedField> _unknownTaggedFields;
    
    public static final Schema SCHEMA_0 =
        new Schema(
            new Field("error_code", Type.INT16, "The top level error code."),
            new Field("topics", new ArrayOf(TopicData.SCHEMA_0), "")
        );
    
    public static final Schema[] SCHEMAS = new Schema[] {
        SCHEMA_0
    };
    
    public static final short LOWEST_SUPPORTED_VERSION = 0;
    public static final short HIGHEST_SUPPORTED_VERSION = 0;
    
    public BeginQuorumEpochResponseData(Readable _readable, short _version) {
        read(_readable, _version);
    }
    
    public BeginQuorumEpochResponseData(Struct _struct, short _version) {
        fromStruct(_struct, _version);
    }
    
    public BeginQuorumEpochResponseData() {
        this.errorCode = (short) 0;
        this.topics = new ArrayList<TopicData>(0);
    }
    
    @Override
    public short apiKey() {
        return 53;
    }
    
    @Override
    public short lowestSupportedVersion() {
        return 0;
    }
    
    @Override
    public short highestSupportedVersion() {
        return 0;
    }
    
    @Override
    public void read(Readable _readable, short _version) {
        this.errorCode = _readable.readShort();
        {
            int arrayLength;
            arrayLength = _readable.readInt();
            if (arrayLength < 0) {
                throw new RuntimeException("non-nullable field topics was serialized as null");
            } else {
                ArrayList<TopicData> newCollection = new ArrayList<TopicData>(arrayLength);
                for (int i = 0; i < arrayLength; i++) {
                    newCollection.add(new TopicData(_readable, _version));
                }
                this.topics = newCollection;
            }
        }
        this._unknownTaggedFields = null;
    }
    
    @Override
    public void write(Writable _writable, ObjectSerializationCache _cache, short _version) {
        int _numTaggedFields = 0;
        _writable.writeShort(errorCode);
        _writable.writeInt(topics.size());
        for (TopicData topicsElement : topics) {
            topicsElement.write(_writable, _cache, _version);
        }
        RawTaggedFieldWriter _rawWriter = RawTaggedFieldWriter.forFields(_unknownTaggedFields);
        _numTaggedFields += _rawWriter.numFields();
        if (_numTaggedFields > 0) {
            throw new UnsupportedVersionException("Tagged fields were set, but version " + _version + " of this message does not support them.");
        }
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public void fromStruct(Struct struct, short _version) {
        this._unknownTaggedFields = null;
        this.errorCode = struct.getShort("error_code");
        {
            Object[] _nestedObjects = struct.getArray("topics");
            this.topics = new ArrayList<TopicData>(_nestedObjects.length);
            for (Object nestedObject : _nestedObjects) {
                this.topics.add(new TopicData((Struct) nestedObject, _version));
            }
        }
    }
    
    @Override
    public Struct toStruct(short _version) {
        TreeMap<Integer, Object> _taggedFields = null;
        Struct struct = new Struct(SCHEMAS[_version]);
        struct.set("error_code", this.errorCode);
        {
            Struct[] _nestedObjects = new Struct[topics.size()];
            int i = 0;
            for (TopicData element : this.topics) {
                _nestedObjects[i++] = element.toStruct(_version);
            }
            struct.set("topics", (Object[]) _nestedObjects);
        }
        return struct;
    }
    
    @Override
    public int size(ObjectSerializationCache _cache, short _version) {
        int _size = 0, _numTaggedFields = 0;
        _size += 2;
        {
            int _arraySize = 0;
            _arraySize += 4;
            for (TopicData topicsElement : topics) {
                _arraySize += topicsElement.size(_cache, _version);
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
        if (_numTaggedFields > 0) {
            throw new UnsupportedVersionException("Tagged fields were set, but version " + _version + " of this message does not support them.");
        }
        return _size;
    }
    
    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof BeginQuorumEpochResponseData)) return false;
        BeginQuorumEpochResponseData other = (BeginQuorumEpochResponseData) obj;
        if (errorCode != other.errorCode) return false;
        if (this.topics == null) {
            if (other.topics != null) return false;
        } else {
            if (!this.topics.equals(other.topics)) return false;
        }
        return MessageUtil.compareRawTaggedFields(_unknownTaggedFields, other._unknownTaggedFields);
    }
    
    @Override
    public int hashCode() {
        int hashCode = 0;
        hashCode = 31 * hashCode + errorCode;
        hashCode = 31 * hashCode + (topics == null ? 0 : topics.hashCode());
        return hashCode;
    }
    
    @Override
    public BeginQuorumEpochResponseData duplicate() {
        BeginQuorumEpochResponseData _duplicate = new BeginQuorumEpochResponseData();
        _duplicate.errorCode = errorCode;
        ArrayList<TopicData> newTopics = new ArrayList<TopicData>(topics.size());
        for (TopicData _element : topics) {
            newTopics.add(_element.duplicate());
        }
        _duplicate.topics = newTopics;
        return _duplicate;
    }
    
    @Override
    public String toString() {
        return "BeginQuorumEpochResponseData("
            + "errorCode=" + errorCode
            + ", topics=" + MessageUtil.deepToString(topics.iterator())
            + ")";
    }
    
    public short errorCode() {
        return this.errorCode;
    }
    
    public List<TopicData> topics() {
        return this.topics;
    }
    
    @Override
    public List<RawTaggedField> unknownTaggedFields() {
        if (_unknownTaggedFields == null) {
            _unknownTaggedFields = new ArrayList<>(0);
        }
        return _unknownTaggedFields;
    }
    
    public BeginQuorumEpochResponseData setErrorCode(short v) {
        this.errorCode = v;
        return this;
    }
    
    public BeginQuorumEpochResponseData setTopics(List<TopicData> v) {
        this.topics = v;
        return this;
    }
    
    public static class TopicData implements Message {
        String topicName;
        List<PartitionData> partitions;
        private List<RawTaggedField> _unknownTaggedFields;
        
        public static final Schema SCHEMA_0 =
            new Schema(
                new Field("topic_name", Type.STRING, "The topic name."),
                new Field("partitions", new ArrayOf(PartitionData.SCHEMA_0), "")
            );
        
        public static final Schema[] SCHEMAS = new Schema[] {
            SCHEMA_0
        };
        
        public static final short LOWEST_SUPPORTED_VERSION = 0;
        public static final short HIGHEST_SUPPORTED_VERSION = 0;
        
        public TopicData(Readable _readable, short _version) {
            read(_readable, _version);
        }
        
        public TopicData(Struct _struct, short _version) {
            fromStruct(_struct, _version);
        }
        
        public TopicData() {
            this.topicName = "";
            this.partitions = new ArrayList<PartitionData>(0);
        }
        
        
        @Override
        public short lowestSupportedVersion() {
            return 0;
        }
        
        @Override
        public short highestSupportedVersion() {
            return 0;
        }
        
        @Override
        public void read(Readable _readable, short _version) {
            if (_version > 0) {
                throw new UnsupportedVersionException("Can't read version " + _version + " of TopicData");
            }
            {
                int length;
                length = _readable.readShort();
                if (length < 0) {
                    throw new RuntimeException("non-nullable field topicName was serialized as null");
                } else if (length > 0x7fff) {
                    throw new RuntimeException("string field topicName had invalid length " + length);
                } else {
                    this.topicName = _readable.readString(length);
                }
            }
            {
                int arrayLength;
                arrayLength = _readable.readInt();
                if (arrayLength < 0) {
                    throw new RuntimeException("non-nullable field partitions was serialized as null");
                } else {
                    ArrayList<PartitionData> newCollection = new ArrayList<PartitionData>(arrayLength);
                    for (int i = 0; i < arrayLength; i++) {
                        newCollection.add(new PartitionData(_readable, _version));
                    }
                    this.partitions = newCollection;
                }
            }
            this._unknownTaggedFields = null;
        }
        
        @Override
        public void write(Writable _writable, ObjectSerializationCache _cache, short _version) {
            int _numTaggedFields = 0;
            {
                byte[] _stringBytes = _cache.getSerializedValue(topicName);
                _writable.writeShort((short) _stringBytes.length);
                _writable.writeByteArray(_stringBytes);
            }
            _writable.writeInt(partitions.size());
            for (PartitionData partitionsElement : partitions) {
                partitionsElement.write(_writable, _cache, _version);
            }
            RawTaggedFieldWriter _rawWriter = RawTaggedFieldWriter.forFields(_unknownTaggedFields);
            _numTaggedFields += _rawWriter.numFields();
            if (_numTaggedFields > 0) {
                throw new UnsupportedVersionException("Tagged fields were set, but version " + _version + " of this message does not support them.");
            }
        }
        
        @SuppressWarnings("unchecked")
        @Override
        public void fromStruct(Struct struct, short _version) {
            if (_version > 0) {
                throw new UnsupportedVersionException("Can't read version " + _version + " of TopicData");
            }
            this._unknownTaggedFields = null;
            this.topicName = struct.getString("topic_name");
            {
                Object[] _nestedObjects = struct.getArray("partitions");
                this.partitions = new ArrayList<PartitionData>(_nestedObjects.length);
                for (Object nestedObject : _nestedObjects) {
                    this.partitions.add(new PartitionData((Struct) nestedObject, _version));
                }
            }
        }
        
        @Override
        public Struct toStruct(short _version) {
            if (_version > 0) {
                throw new UnsupportedVersionException("Can't write version " + _version + " of TopicData");
            }
            TreeMap<Integer, Object> _taggedFields = null;
            Struct struct = new Struct(SCHEMAS[_version]);
            struct.set("topic_name", this.topicName);
            {
                Struct[] _nestedObjects = new Struct[partitions.size()];
                int i = 0;
                for (PartitionData element : this.partitions) {
                    _nestedObjects[i++] = element.toStruct(_version);
                }
                struct.set("partitions", (Object[]) _nestedObjects);
            }
            return struct;
        }
        
        @Override
        public int size(ObjectSerializationCache _cache, short _version) {
            int _size = 0, _numTaggedFields = 0;
            if (_version > 0) {
                throw new UnsupportedVersionException("Can't size version " + _version + " of TopicData");
            }
            {
                byte[] _stringBytes = topicName.getBytes(StandardCharsets.UTF_8);
                if (_stringBytes.length > 0x7fff) {
                    throw new RuntimeException("'topicName' field is too long to be serialized");
                }
                _cache.cacheSerializedValue(topicName, _stringBytes);
                _size += _stringBytes.length + 2;
            }
            {
                int _arraySize = 0;
                _arraySize += 4;
                for (PartitionData partitionsElement : partitions) {
                    _arraySize += partitionsElement.size(_cache, _version);
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
            if (_numTaggedFields > 0) {
                throw new UnsupportedVersionException("Tagged fields were set, but version " + _version + " of this message does not support them.");
            }
            return _size;
        }
        
        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof TopicData)) return false;
            TopicData other = (TopicData) obj;
            if (this.topicName == null) {
                if (other.topicName != null) return false;
            } else {
                if (!this.topicName.equals(other.topicName)) return false;
            }
            if (this.partitions == null) {
                if (other.partitions != null) return false;
            } else {
                if (!this.partitions.equals(other.partitions)) return false;
            }
            return MessageUtil.compareRawTaggedFields(_unknownTaggedFields, other._unknownTaggedFields);
        }
        
        @Override
        public int hashCode() {
            int hashCode = 0;
            hashCode = 31 * hashCode + (topicName == null ? 0 : topicName.hashCode());
            hashCode = 31 * hashCode + (partitions == null ? 0 : partitions.hashCode());
            return hashCode;
        }
        
        @Override
        public TopicData duplicate() {
            TopicData _duplicate = new TopicData();
            _duplicate.topicName = topicName;
            ArrayList<PartitionData> newPartitions = new ArrayList<PartitionData>(partitions.size());
            for (PartitionData _element : partitions) {
                newPartitions.add(_element.duplicate());
            }
            _duplicate.partitions = newPartitions;
            return _duplicate;
        }
        
        @Override
        public String toString() {
            return "TopicData("
                + "topicName=" + ((topicName == null) ? "null" : "'" + topicName.toString() + "'")
                + ", partitions=" + MessageUtil.deepToString(partitions.iterator())
                + ")";
        }
        
        public String topicName() {
            return this.topicName;
        }
        
        public List<PartitionData> partitions() {
            return this.partitions;
        }
        
        @Override
        public List<RawTaggedField> unknownTaggedFields() {
            if (_unknownTaggedFields == null) {
                _unknownTaggedFields = new ArrayList<>(0);
            }
            return _unknownTaggedFields;
        }
        
        public TopicData setTopicName(String v) {
            this.topicName = v;
            return this;
        }
        
        public TopicData setPartitions(List<PartitionData> v) {
            this.partitions = v;
            return this;
        }
    }
    
    public static class PartitionData implements Message {
        int partitionIndex;
        short errorCode;
        int leaderId;
        int leaderEpoch;
        private List<RawTaggedField> _unknownTaggedFields;
        
        public static final Schema SCHEMA_0 =
            new Schema(
                new Field("partition_index", Type.INT32, "The partition index."),
                new Field("error_code", Type.INT16, ""),
                new Field("leader_id", Type.INT32, "The ID of the current leader or -1 if the leader is unknown."),
                new Field("leader_epoch", Type.INT32, "The latest known leader epoch")
            );
        
        public static final Schema[] SCHEMAS = new Schema[] {
            SCHEMA_0
        };
        
        public static final short LOWEST_SUPPORTED_VERSION = 0;
        public static final short HIGHEST_SUPPORTED_VERSION = 0;
        
        public PartitionData(Readable _readable, short _version) {
            read(_readable, _version);
        }
        
        public PartitionData(Struct _struct, short _version) {
            fromStruct(_struct, _version);
        }
        
        public PartitionData() {
            this.partitionIndex = 0;
            this.errorCode = (short) 0;
            this.leaderId = 0;
            this.leaderEpoch = 0;
        }
        
        
        @Override
        public short lowestSupportedVersion() {
            return 0;
        }
        
        @Override
        public short highestSupportedVersion() {
            return 0;
        }
        
        @Override
        public void read(Readable _readable, short _version) {
            if (_version > 0) {
                throw new UnsupportedVersionException("Can't read version " + _version + " of PartitionData");
            }
            this.partitionIndex = _readable.readInt();
            this.errorCode = _readable.readShort();
            this.leaderId = _readable.readInt();
            this.leaderEpoch = _readable.readInt();
            this._unknownTaggedFields = null;
        }
        
        @Override
        public void write(Writable _writable, ObjectSerializationCache _cache, short _version) {
            int _numTaggedFields = 0;
            _writable.writeInt(partitionIndex);
            _writable.writeShort(errorCode);
            _writable.writeInt(leaderId);
            _writable.writeInt(leaderEpoch);
            RawTaggedFieldWriter _rawWriter = RawTaggedFieldWriter.forFields(_unknownTaggedFields);
            _numTaggedFields += _rawWriter.numFields();
            if (_numTaggedFields > 0) {
                throw new UnsupportedVersionException("Tagged fields were set, but version " + _version + " of this message does not support them.");
            }
        }
        
        @SuppressWarnings("unchecked")
        @Override
        public void fromStruct(Struct struct, short _version) {
            if (_version > 0) {
                throw new UnsupportedVersionException("Can't read version " + _version + " of PartitionData");
            }
            this._unknownTaggedFields = null;
            this.partitionIndex = struct.getInt("partition_index");
            this.errorCode = struct.getShort("error_code");
            this.leaderId = struct.getInt("leader_id");
            this.leaderEpoch = struct.getInt("leader_epoch");
        }
        
        @Override
        public Struct toStruct(short _version) {
            if (_version > 0) {
                throw new UnsupportedVersionException("Can't write version " + _version + " of PartitionData");
            }
            TreeMap<Integer, Object> _taggedFields = null;
            Struct struct = new Struct(SCHEMAS[_version]);
            struct.set("partition_index", this.partitionIndex);
            struct.set("error_code", this.errorCode);
            struct.set("leader_id", this.leaderId);
            struct.set("leader_epoch", this.leaderEpoch);
            return struct;
        }
        
        @Override
        public int size(ObjectSerializationCache _cache, short _version) {
            int _size = 0, _numTaggedFields = 0;
            if (_version > 0) {
                throw new UnsupportedVersionException("Can't size version " + _version + " of PartitionData");
            }
            _size += 4;
            _size += 2;
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
            if (_numTaggedFields > 0) {
                throw new UnsupportedVersionException("Tagged fields were set, but version " + _version + " of this message does not support them.");
            }
            return _size;
        }
        
        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof PartitionData)) return false;
            PartitionData other = (PartitionData) obj;
            if (partitionIndex != other.partitionIndex) return false;
            if (errorCode != other.errorCode) return false;
            if (leaderId != other.leaderId) return false;
            if (leaderEpoch != other.leaderEpoch) return false;
            return MessageUtil.compareRawTaggedFields(_unknownTaggedFields, other._unknownTaggedFields);
        }
        
        @Override
        public int hashCode() {
            int hashCode = 0;
            hashCode = 31 * hashCode + partitionIndex;
            hashCode = 31 * hashCode + errorCode;
            hashCode = 31 * hashCode + leaderId;
            hashCode = 31 * hashCode + leaderEpoch;
            return hashCode;
        }
        
        @Override
        public PartitionData duplicate() {
            PartitionData _duplicate = new PartitionData();
            _duplicate.partitionIndex = partitionIndex;
            _duplicate.errorCode = errorCode;
            _duplicate.leaderId = leaderId;
            _duplicate.leaderEpoch = leaderEpoch;
            return _duplicate;
        }
        
        @Override
        public String toString() {
            return "PartitionData("
                + "partitionIndex=" + partitionIndex
                + ", errorCode=" + errorCode
                + ", leaderId=" + leaderId
                + ", leaderEpoch=" + leaderEpoch
                + ")";
        }
        
        public int partitionIndex() {
            return this.partitionIndex;
        }
        
        public short errorCode() {
            return this.errorCode;
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
        
        public PartitionData setPartitionIndex(int v) {
            this.partitionIndex = v;
            return this;
        }
        
        public PartitionData setErrorCode(short v) {
            this.errorCode = v;
            return this;
        }
        
        public PartitionData setLeaderId(int v) {
            this.leaderId = v;
            return this;
        }
        
        public PartitionData setLeaderEpoch(int v) {
            this.leaderEpoch = v;
            return this;
        }
    }
}
