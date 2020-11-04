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

import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;
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


public class LeaderChangeMessage implements ApiMessage {
    int leaderId;
    List<Voter> voters;
    private List<RawTaggedField> _unknownTaggedFields;
    
    public static final Schema SCHEMA_0 =
        new Schema(
            new Field("leader_id", Type.INT32, "The ID of the newly elected leader"),
            new Field("voters", new CompactArrayOf(Voter.SCHEMA_0), "The voters who voted for the current leader"),
            TaggedFieldsSection.of(
            )
        );
    
    public static final Schema[] SCHEMAS = new Schema[] {
        SCHEMA_0
    };
    
    public static final short LOWEST_SUPPORTED_VERSION = 0;
    public static final short HIGHEST_SUPPORTED_VERSION = 0;
    
    public LeaderChangeMessage(Readable _readable, short _version) {
        read(_readable, _version);
    }
    
    public LeaderChangeMessage(Struct _struct, short _version) {
        fromStruct(_struct, _version);
    }
    
    public LeaderChangeMessage() {
        this.leaderId = 0;
        this.voters = new ArrayList<Voter>(0);
    }
    
    @Override
    public short apiKey() {
        return -1;
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
        this.leaderId = _readable.readInt();
        {
            int arrayLength;
            arrayLength = _readable.readUnsignedVarint() - 1;
            if (arrayLength < 0) {
                throw new RuntimeException("non-nullable field voters was serialized as null");
            } else {
                ArrayList<Voter> newCollection = new ArrayList<Voter>(arrayLength);
                for (int i = 0; i < arrayLength; i++) {
                    newCollection.add(new Voter(_readable, _version));
                }
                this.voters = newCollection;
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
        _writable.writeInt(leaderId);
        _writable.writeUnsignedVarint(voters.size() + 1);
        for (Voter votersElement : voters) {
            votersElement.write(_writable, _cache, _version);
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
        this.leaderId = struct.getInt("leader_id");
        {
            Object[] _nestedObjects = struct.getArray("voters");
            this.voters = new ArrayList<Voter>(_nestedObjects.length);
            for (Object nestedObject : _nestedObjects) {
                this.voters.add(new Voter((Struct) nestedObject, _version));
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
        struct.set("leader_id", this.leaderId);
        {
            Struct[] _nestedObjects = new Struct[voters.size()];
            int i = 0;
            for (Voter element : this.voters) {
                _nestedObjects[i++] = element.toStruct(_version);
            }
            struct.set("voters", (Object[]) _nestedObjects);
        }
        struct.set("_tagged_fields", _taggedFields);
        return struct;
    }
    
    @Override
    public int size(ObjectSerializationCache _cache, short _version) {
        int _size = 0, _numTaggedFields = 0;
        _size += 4;
        {
            int _arraySize = 0;
            _arraySize += ByteUtils.sizeOfUnsignedVarint(voters.size() + 1);
            for (Voter votersElement : voters) {
                _arraySize += votersElement.size(_cache, _version);
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
        if (!(obj instanceof LeaderChangeMessage)) return false;
        LeaderChangeMessage other = (LeaderChangeMessage) obj;
        if (leaderId != other.leaderId) return false;
        if (this.voters == null) {
            if (other.voters != null) return false;
        } else {
            if (!this.voters.equals(other.voters)) return false;
        }
        return MessageUtil.compareRawTaggedFields(_unknownTaggedFields, other._unknownTaggedFields);
    }
    
    @Override
    public int hashCode() {
        int hashCode = 0;
        hashCode = 31 * hashCode + leaderId;
        hashCode = 31 * hashCode + (voters == null ? 0 : voters.hashCode());
        return hashCode;
    }
    
    @Override
    public LeaderChangeMessage duplicate() {
        LeaderChangeMessage _duplicate = new LeaderChangeMessage();
        _duplicate.leaderId = leaderId;
        ArrayList<Voter> newVoters = new ArrayList<Voter>(voters.size());
        for (Voter _element : voters) {
            newVoters.add(_element.duplicate());
        }
        _duplicate.voters = newVoters;
        return _duplicate;
    }
    
    @Override
    public String toString() {
        return "LeaderChangeMessage("
            + "leaderId=" + leaderId
            + ", voters=" + MessageUtil.deepToString(voters.iterator())
            + ")";
    }
    
    public int leaderId() {
        return this.leaderId;
    }
    
    public List<Voter> voters() {
        return this.voters;
    }
    
    @Override
    public List<RawTaggedField> unknownTaggedFields() {
        if (_unknownTaggedFields == null) {
            _unknownTaggedFields = new ArrayList<>(0);
        }
        return _unknownTaggedFields;
    }
    
    public LeaderChangeMessage setLeaderId(int v) {
        this.leaderId = v;
        return this;
    }
    
    public LeaderChangeMessage setVoters(List<Voter> v) {
        this.voters = v;
        return this;
    }
    
    public static class Voter implements Message {
        int voterId;
        private List<RawTaggedField> _unknownTaggedFields;
        
        public static final Schema SCHEMA_0 =
            new Schema(
                new Field("voter_id", Type.INT32, ""),
                TaggedFieldsSection.of(
                )
            );
        
        public static final Schema[] SCHEMAS = new Schema[] {
            SCHEMA_0
        };
        
        public static final short LOWEST_SUPPORTED_VERSION = 0;
        public static final short HIGHEST_SUPPORTED_VERSION = 0;
        
        public Voter(Readable _readable, short _version) {
            read(_readable, _version);
        }
        
        public Voter(Struct _struct, short _version) {
            fromStruct(_struct, _version);
        }
        
        public Voter() {
            this.voterId = 0;
        }
        
        
        @Override
        public short lowestSupportedVersion() {
            return 0;
        }
        
        @Override
        public short highestSupportedVersion() {
            return 32767;
        }
        
        @Override
        public void read(Readable _readable, short _version) {
            this.voterId = _readable.readInt();
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
            _writable.writeInt(voterId);
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
            this.voterId = struct.getInt("voter_id");
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
            struct.set("voter_id", this.voterId);
            struct.set("_tagged_fields", _taggedFields);
            return struct;
        }
        
        @Override
        public int size(ObjectSerializationCache _cache, short _version) {
            int _size = 0, _numTaggedFields = 0;
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
            if (!(obj instanceof Voter)) return false;
            Voter other = (Voter) obj;
            if (voterId != other.voterId) return false;
            return MessageUtil.compareRawTaggedFields(_unknownTaggedFields, other._unknownTaggedFields);
        }
        
        @Override
        public int hashCode() {
            int hashCode = 0;
            hashCode = 31 * hashCode + voterId;
            return hashCode;
        }
        
        @Override
        public Voter duplicate() {
            Voter _duplicate = new Voter();
            _duplicate.voterId = voterId;
            return _duplicate;
        }
        
        @Override
        public String toString() {
            return "Voter("
                + "voterId=" + voterId
                + ")";
        }
        
        public int voterId() {
            return this.voterId;
        }
        
        @Override
        public List<RawTaggedField> unknownTaggedFields() {
            if (_unknownTaggedFields == null) {
                _unknownTaggedFields = new ArrayList<>(0);
            }
            return _unknownTaggedFields;
        }
        
        public Voter setVoterId(int v) {
            this.voterId = v;
            return this;
        }
    }
}
