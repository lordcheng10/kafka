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
import java.util.TreeMap;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Message;
import org.apache.kafka.common.protocol.MessageUtil;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.protocol.Readable;
import org.apache.kafka.common.protocol.Writable;
import org.apache.kafka.common.protocol.types.CompactArrayOf;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.RawTaggedField;
import org.apache.kafka.common.protocol.types.RawTaggedFieldWriter;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.protocol.types.Type;
import org.apache.kafka.common.utils.ByteUtils;

import static java.util.Map.Entry;
import static org.apache.kafka.common.protocol.types.Field.TaggedFieldsSection;


public class AlterIsrRequestData implements ApiMessage {
    int brokerId;
    long brokerEpoch;
    List<TopicData> topics;
    private List<RawTaggedField> _unknownTaggedFields;
    
    public static final Schema SCHEMA_0 =
        new Schema(
            new Field("broker_id", Type.INT32, "The ID of the requesting broker"),
            new Field("broker_epoch", Type.INT64, "The epoch of the requesting broker"),
            new Field("topics", new CompactArrayOf(TopicData.SCHEMA_0), ""),
            TaggedFieldsSection.of(
            )
        );
    
    public static final Schema[] SCHEMAS = new Schema[] {
        SCHEMA_0
    };
    
    public static final short LOWEST_SUPPORTED_VERSION = 0;
    public static final short HIGHEST_SUPPORTED_VERSION = 0;
    
    public AlterIsrRequestData(Readable _readable, short _version) {
        read(_readable, _version);
    }
    
    public AlterIsrRequestData(Struct _struct, short _version) {
        fromStruct(_struct, _version);
    }
    
    public AlterIsrRequestData() {
        this.brokerId = 0;
        this.brokerEpoch = -1L;
        this.topics = new ArrayList<TopicData>(0);
    }
    
    @Override
    public short apiKey() {
        return 56;
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
        this.brokerId = _readable.readInt();
        this.brokerEpoch = _readable.readLong();
        {
            int arrayLength;
            arrayLength = _readable.readUnsignedVarint() - 1;
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
        int _numTaggedFields = 0;
        _writable.writeInt(brokerId);
        _writable.writeLong(brokerEpoch);
        _writable.writeUnsignedVarint(topics.size() + 1);
        for (TopicData topicsElement : topics) {
            topicsElement.write(_writable, _cache, _version);
        }
        RawTaggedFieldWriter _rawWriter = RawTaggedFieldWriter.forFields(_unknownTaggedFields);
        _numTaggedFields += _rawWriter.numFields();
        _writable.writeUnsignedVarint(_numTaggedFields);
        _rawWriter.writeRawTags(_writable, Integer.MAX_VALUE);
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public void fromStruct(Struct struct, short _version) {
        NavigableMap<Integer, Object> _taggedFields = null;
        this._unknownTaggedFields = null;
        _taggedFields = (NavigableMap<Integer, Object>) struct.get("_tagged_fields");
        this.brokerId = struct.getInt("broker_id");
        this.brokerEpoch = struct.getLong("broker_epoch");
        {
            Object[] _nestedObjects = struct.getArray("topics");
            this.topics = new ArrayList<TopicData>(_nestedObjects.length);
            for (Object nestedObject : _nestedObjects) {
                this.topics.add(new TopicData((Struct) nestedObject, _version));
            }
        }
        if (!_taggedFields.isEmpty()) {
            this._unknownTaggedFields = new ArrayList<>(_taggedFields.size());
            for (Entry<Integer, Object> entry : _taggedFields.entrySet()) {
                this._unknownTaggedFields.add((RawTaggedField) entry.getValue());
            }
        }
    }
    
    @Override
    public Struct toStruct(short _version) {
        TreeMap<Integer, Object> _taggedFields = null;
        _taggedFields = new TreeMap<>();
        Struct struct = new Struct(SCHEMAS[_version]);
        struct.set("broker_id", this.brokerId);
        struct.set("broker_epoch", this.brokerEpoch);
        {
            Struct[] _nestedObjects = new Struct[topics.size()];
            int i = 0;
            for (TopicData element : this.topics) {
                _nestedObjects[i++] = element.toStruct(_version);
            }
            struct.set("topics", (Object[]) _nestedObjects);
        }
        struct.set("_tagged_fields", _taggedFields);
        return struct;
    }
    
    @Override
    public int size(ObjectSerializationCache _cache, short _version) {
        int _size = 0, _numTaggedFields = 0;
        _size += 4;
        _size += 8;
        {
            int _arraySize = 0;
            _arraySize += ByteUtils.sizeOfUnsignedVarint(topics.size() + 1);
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
        _size += ByteUtils.sizeOfUnsignedVarint(_numTaggedFields);
        return _size;
    }
    
    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof AlterIsrRequestData)) return false;
        AlterIsrRequestData other = (AlterIsrRequestData) obj;
        if (brokerId != other.brokerId) return false;
        if (brokerEpoch != other.brokerEpoch) return false;
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
        hashCode = 31 * hashCode + brokerId;
        hashCode = 31 * hashCode + ((int) (brokerEpoch >> 32) ^ (int) brokerEpoch);
        hashCode = 31 * hashCode + (topics == null ? 0 : topics.hashCode());
        return hashCode;
    }
    
    @Override
    public AlterIsrRequestData duplicate() {
        AlterIsrRequestData _duplicate = new AlterIsrRequestData();
        _duplicate.brokerId = brokerId;
        _duplicate.brokerEpoch = brokerEpoch;
        ArrayList<TopicData> newTopics = new ArrayList<TopicData>(topics.size());
        for (TopicData _element : topics) {
            newTopics.add(_element.duplicate());
        }
        _duplicate.topics = newTopics;
        return _duplicate;
    }
    
    @Override
    public String toString() {
        return "AlterIsrRequestData("
            + "brokerId=" + brokerId
            + ", brokerEpoch=" + brokerEpoch
            + ", topics=" + MessageUtil.deepToString(topics.iterator())
            + ")";
    }
    
    public int brokerId() {
        return this.brokerId;
    }
    
    public long brokerEpoch() {
        return this.brokerEpoch;
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
    
    public AlterIsrRequestData setBrokerId(int v) {
        this.brokerId = v;
        return this;
    }
    
    public AlterIsrRequestData setBrokerEpoch(long v) {
        this.brokerEpoch = v;
        return this;
    }
    
    public AlterIsrRequestData setTopics(List<TopicData> v) {
        this.topics = v;
        return this;
    }
    
    public static class TopicData implements Message {
        String name;
        List<PartitionData> partitions;
        private List<RawTaggedField> _unknownTaggedFields;
        
        public static final Schema SCHEMA_0 =
            new Schema(
                new Field("name", Type.COMPACT_STRING, "The name of the topic to alter ISRs for"),
                new Field("partitions", new CompactArrayOf(PartitionData.SCHEMA_0), ""),
                TaggedFieldsSection.of(
                )
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
            this.name = "";
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
                length = _readable.readUnsignedVarint() - 1;
                if (length < 0) {
                    throw new RuntimeException("non-nullable field name was serialized as null");
                } else if (length > 0x7fff) {
                    throw new RuntimeException("string field name had invalid length " + length);
                } else {
                    this.name = _readable.readString(length);
                }
            }
            {
                int arrayLength;
                arrayLength = _readable.readUnsignedVarint() - 1;
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
            int _numTaggedFields = 0;
            {
                byte[] _stringBytes = _cache.getSerializedValue(name);
                _writable.writeUnsignedVarint(_stringBytes.length + 1);
                _writable.writeByteArray(_stringBytes);
            }
            _writable.writeUnsignedVarint(partitions.size() + 1);
            for (PartitionData partitionsElement : partitions) {
                partitionsElement.write(_writable, _cache, _version);
            }
            RawTaggedFieldWriter _rawWriter = RawTaggedFieldWriter.forFields(_unknownTaggedFields);
            _numTaggedFields += _rawWriter.numFields();
            _writable.writeUnsignedVarint(_numTaggedFields);
            _rawWriter.writeRawTags(_writable, Integer.MAX_VALUE);
        }
        
        @SuppressWarnings("unchecked")
        @Override
        public void fromStruct(Struct struct, short _version) {
            if (_version > 0) {
                throw new UnsupportedVersionException("Can't read version " + _version + " of TopicData");
            }
            NavigableMap<Integer, Object> _taggedFields = null;
            this._unknownTaggedFields = null;
            _taggedFields = (NavigableMap<Integer, Object>) struct.get("_tagged_fields");
            this.name = struct.getString("name");
            {
                Object[] _nestedObjects = struct.getArray("partitions");
                this.partitions = new ArrayList<PartitionData>(_nestedObjects.length);
                for (Object nestedObject : _nestedObjects) {
                    this.partitions.add(new PartitionData((Struct) nestedObject, _version));
                }
            }
            if (!_taggedFields.isEmpty()) {
                this._unknownTaggedFields = new ArrayList<>(_taggedFields.size());
                for (Entry<Integer, Object> entry : _taggedFields.entrySet()) {
                    this._unknownTaggedFields.add((RawTaggedField) entry.getValue());
                }
            }
        }
        
        @Override
        public Struct toStruct(short _version) {
            if (_version > 0) {
                throw new UnsupportedVersionException("Can't write version " + _version + " of TopicData");
            }
            TreeMap<Integer, Object> _taggedFields = null;
            _taggedFields = new TreeMap<>();
            Struct struct = new Struct(SCHEMAS[_version]);
            struct.set("name", this.name);
            {
                Struct[] _nestedObjects = new Struct[partitions.size()];
                int i = 0;
                for (PartitionData element : this.partitions) {
                    _nestedObjects[i++] = element.toStruct(_version);
                }
                struct.set("partitions", (Object[]) _nestedObjects);
            }
            struct.set("_tagged_fields", _taggedFields);
            return struct;
        }
        
        @Override
        public int size(ObjectSerializationCache _cache, short _version) {
            int _size = 0, _numTaggedFields = 0;
            if (_version > 0) {
                throw new UnsupportedVersionException("Can't size version " + _version + " of TopicData");
            }
            {
                byte[] _stringBytes = name.getBytes(StandardCharsets.UTF_8);
                if (_stringBytes.length > 0x7fff) {
                    throw new RuntimeException("'name' field is too long to be serialized");
                }
                _cache.cacheSerializedValue(name, _stringBytes);
                _size += _stringBytes.length + ByteUtils.sizeOfUnsignedVarint(_stringBytes.length + 1);
            }
            {
                int _arraySize = 0;
                _arraySize += ByteUtils.sizeOfUnsignedVarint(partitions.size() + 1);
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
            _size += ByteUtils.sizeOfUnsignedVarint(_numTaggedFields);
            return _size;
        }
        
        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof TopicData)) return false;
            TopicData other = (TopicData) obj;
            if (this.name == null) {
                if (other.name != null) return false;
            } else {
                if (!this.name.equals(other.name)) return false;
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
            hashCode = 31 * hashCode + (name == null ? 0 : name.hashCode());
            hashCode = 31 * hashCode + (partitions == null ? 0 : partitions.hashCode());
            return hashCode;
        }
        
        @Override
        public TopicData duplicate() {
            TopicData _duplicate = new TopicData();
            _duplicate.name = name;
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
                + "name=" + ((name == null) ? "null" : "'" + name.toString() + "'")
                + ", partitions=" + MessageUtil.deepToString(partitions.iterator())
                + ")";
        }
        
        public String name() {
            return this.name;
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
        
        public TopicData setName(String v) {
            this.name = v;
            return this;
        }
        
        public TopicData setPartitions(List<PartitionData> v) {
            this.partitions = v;
            return this;
        }
    }
    
    public static class PartitionData implements Message {
        int partitionIndex;
        int leaderEpoch;
        List<Integer> newIsr;
        int currentIsrVersion;
        private List<RawTaggedField> _unknownTaggedFields;
        
        public static final Schema SCHEMA_0 =
            new Schema(
                new Field("partition_index", Type.INT32, "The partition index"),
                new Field("leader_epoch", Type.INT32, "The leader epoch of this partition"),
                new Field("new_isr", new CompactArrayOf(Type.INT32), "The ISR for this partition"),
                new Field("current_isr_version", Type.INT32, "The expected version of ISR which is being updated"),
                TaggedFieldsSection.of(
                )
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
            this.leaderEpoch = 0;
            this.newIsr = new ArrayList<Integer>(0);
            this.currentIsrVersion = 0;
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
            this.leaderEpoch = _readable.readInt();
            {
                int arrayLength;
                arrayLength = _readable.readUnsignedVarint() - 1;
                if (arrayLength < 0) {
                    throw new RuntimeException("non-nullable field newIsr was serialized as null");
                } else {
                    ArrayList<Integer> newCollection = new ArrayList<Integer>(arrayLength);
                    for (int i = 0; i < arrayLength; i++) {
                        newCollection.add(_readable.readInt());
                    }
                    this.newIsr = newCollection;
                }
            }
            this.currentIsrVersion = _readable.readInt();
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
            int _numTaggedFields = 0;
            _writable.writeInt(partitionIndex);
            _writable.writeInt(leaderEpoch);
            _writable.writeUnsignedVarint(newIsr.size() + 1);
            for (Integer newIsrElement : newIsr) {
                _writable.writeInt(newIsrElement);
            }
            _writable.writeInt(currentIsrVersion);
            RawTaggedFieldWriter _rawWriter = RawTaggedFieldWriter.forFields(_unknownTaggedFields);
            _numTaggedFields += _rawWriter.numFields();
            _writable.writeUnsignedVarint(_numTaggedFields);
            _rawWriter.writeRawTags(_writable, Integer.MAX_VALUE);
        }
        
        @SuppressWarnings("unchecked")
        @Override
        public void fromStruct(Struct struct, short _version) {
            if (_version > 0) {
                throw new UnsupportedVersionException("Can't read version " + _version + " of PartitionData");
            }
            NavigableMap<Integer, Object> _taggedFields = null;
            this._unknownTaggedFields = null;
            _taggedFields = (NavigableMap<Integer, Object>) struct.get("_tagged_fields");
            this.partitionIndex = struct.getInt("partition_index");
            this.leaderEpoch = struct.getInt("leader_epoch");
            {
                Object[] _nestedObjects = struct.getArray("new_isr");
                this.newIsr = new ArrayList<Integer>(_nestedObjects.length);
                for (Object nestedObject : _nestedObjects) {
                    this.newIsr.add((Integer) nestedObject);
                }
            }
            this.currentIsrVersion = struct.getInt("current_isr_version");
            if (!_taggedFields.isEmpty()) {
                this._unknownTaggedFields = new ArrayList<>(_taggedFields.size());
                for (Entry<Integer, Object> entry : _taggedFields.entrySet()) {
                    this._unknownTaggedFields.add((RawTaggedField) entry.getValue());
                }
            }
        }
        
        @Override
        public Struct toStruct(short _version) {
            if (_version > 0) {
                throw new UnsupportedVersionException("Can't write version " + _version + " of PartitionData");
            }
            TreeMap<Integer, Object> _taggedFields = null;
            _taggedFields = new TreeMap<>();
            Struct struct = new Struct(SCHEMAS[_version]);
            struct.set("partition_index", this.partitionIndex);
            struct.set("leader_epoch", this.leaderEpoch);
            {
                Integer[] _nestedObjects = new Integer[newIsr.size()];
                int i = 0;
                for (Integer element : this.newIsr) {
                    _nestedObjects[i++] = element;
                }
                struct.set("new_isr", (Object[]) _nestedObjects);
            }
            struct.set("current_isr_version", this.currentIsrVersion);
            struct.set("_tagged_fields", _taggedFields);
            return struct;
        }
        
        @Override
        public int size(ObjectSerializationCache _cache, short _version) {
            int _size = 0, _numTaggedFields = 0;
            if (_version > 0) {
                throw new UnsupportedVersionException("Can't size version " + _version + " of PartitionData");
            }
            _size += 4;
            _size += 4;
            {
                int _arraySize = 0;
                _arraySize += ByteUtils.sizeOfUnsignedVarint(newIsr.size() + 1);
                _arraySize += newIsr.size() * 4;
                _size += _arraySize;
            }
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
            if (!(obj instanceof PartitionData)) return false;
            PartitionData other = (PartitionData) obj;
            if (partitionIndex != other.partitionIndex) return false;
            if (leaderEpoch != other.leaderEpoch) return false;
            if (this.newIsr == null) {
                if (other.newIsr != null) return false;
            } else {
                if (!this.newIsr.equals(other.newIsr)) return false;
            }
            if (currentIsrVersion != other.currentIsrVersion) return false;
            return MessageUtil.compareRawTaggedFields(_unknownTaggedFields, other._unknownTaggedFields);
        }
        
        @Override
        public int hashCode() {
            int hashCode = 0;
            hashCode = 31 * hashCode + partitionIndex;
            hashCode = 31 * hashCode + leaderEpoch;
            hashCode = 31 * hashCode + (newIsr == null ? 0 : newIsr.hashCode());
            hashCode = 31 * hashCode + currentIsrVersion;
            return hashCode;
        }
        
        @Override
        public PartitionData duplicate() {
            PartitionData _duplicate = new PartitionData();
            _duplicate.partitionIndex = partitionIndex;
            _duplicate.leaderEpoch = leaderEpoch;
            ArrayList<Integer> newNewIsr = new ArrayList<Integer>(newIsr.size());
            for (Integer _element : newIsr) {
                newNewIsr.add(_element);
            }
            _duplicate.newIsr = newNewIsr;
            _duplicate.currentIsrVersion = currentIsrVersion;
            return _duplicate;
        }
        
        @Override
        public String toString() {
            return "PartitionData("
                + "partitionIndex=" + partitionIndex
                + ", leaderEpoch=" + leaderEpoch
                + ", newIsr=" + MessageUtil.deepToString(newIsr.iterator())
                + ", currentIsrVersion=" + currentIsrVersion
                + ")";
        }
        
        public int partitionIndex() {
            return this.partitionIndex;
        }
        
        public int leaderEpoch() {
            return this.leaderEpoch;
        }
        
        public List<Integer> newIsr() {
            return this.newIsr;
        }
        
        public int currentIsrVersion() {
            return this.currentIsrVersion;
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
        
        public PartitionData setLeaderEpoch(int v) {
            this.leaderEpoch = v;
            return this;
        }
        
        public PartitionData setNewIsr(List<Integer> v) {
            this.newIsr = v;
            return this;
        }
        
        public PartitionData setCurrentIsrVersion(int v) {
            this.currentIsrVersion = v;
            return this;
        }
    }
}
