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
import java.util.Iterator;
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
import org.apache.kafka.common.protocol.types.ArrayOf;
import org.apache.kafka.common.protocol.types.CompactArrayOf;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.RawTaggedField;
import org.apache.kafka.common.protocol.types.RawTaggedFieldWriter;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.protocol.types.Type;
import org.apache.kafka.common.utils.ByteUtils;
import org.apache.kafka.common.utils.ImplicitLinkedHashCollection;
import org.apache.kafka.common.utils.ImplicitLinkedHashMultiCollection;

import static java.util.Map.Entry;
import static org.apache.kafka.common.protocol.types.Field.TaggedFieldsSection;


public class ApiVersionsResponseData implements ApiMessage {
    short errorCode;
    ApiVersionsResponseKeyCollection apiKeys;
    int throttleTimeMs;
    SupportedFeatureKeyCollection supportedFeatures;
    long finalizedFeaturesEpoch;
    FinalizedFeatureKeyCollection finalizedFeatures;
    private List<RawTaggedField> _unknownTaggedFields;
    
    public static final Schema SCHEMA_0 =
        new Schema(
            new Field("error_code", Type.INT16, "The top-level error code."),
            new Field("api_keys", new ArrayOf(ApiVersionsResponseKey.SCHEMA_0), "The APIs supported by the broker.")
        );
    
    public static final Schema SCHEMA_1 =
        new Schema(
            new Field("error_code", Type.INT16, "The top-level error code."),
            new Field("api_keys", new ArrayOf(ApiVersionsResponseKey.SCHEMA_0), "The APIs supported by the broker."),
            new Field("throttle_time_ms", Type.INT32, "The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.")
        );
    
    public static final Schema SCHEMA_2 = SCHEMA_1;
    
    public static final Schema SCHEMA_3 =
        new Schema(
            new Field("error_code", Type.INT16, "The top-level error code."),
            new Field("api_keys", new CompactArrayOf(ApiVersionsResponseKey.SCHEMA_3), "The APIs supported by the broker."),
            new Field("throttle_time_ms", Type.INT32, "The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota."),
            TaggedFieldsSection.of(
                0, new Field("supported_features", new CompactArrayOf(SupportedFeatureKey.SCHEMA_3), "Features supported by the broker."),
                1, new Field("finalized_features_epoch", Type.INT64, "The monotonically increasing epoch for the finalized features information. Valid values are >= 0. A value of -1 is special and represents unknown epoch."),
                2, new Field("finalized_features", new CompactArrayOf(FinalizedFeatureKey.SCHEMA_3), "List of cluster-wide finalized features. The information is valid only if FinalizedFeaturesEpoch >= 0.")
            )
        );
    
    public static final Schema[] SCHEMAS = new Schema[] {
        SCHEMA_0,
        SCHEMA_1,
        SCHEMA_2,
        SCHEMA_3
    };
    
    public static final short LOWEST_SUPPORTED_VERSION = 0;
    public static final short HIGHEST_SUPPORTED_VERSION = 3;
    
    public ApiVersionsResponseData(Readable _readable, short _version) {
        read(_readable, _version);
    }
    
    public ApiVersionsResponseData(Struct _struct, short _version) {
        fromStruct(_struct, _version);
    }
    
    public ApiVersionsResponseData() {
        this.errorCode = (short) 0;
        this.apiKeys = new ApiVersionsResponseKeyCollection(0);
        this.throttleTimeMs = 0;
        this.supportedFeatures = new SupportedFeatureKeyCollection(0);
        this.finalizedFeaturesEpoch = -1L;
        this.finalizedFeatures = new FinalizedFeatureKeyCollection(0);
    }
    
    @Override
    public short apiKey() {
        return 18;
    }
    
    @Override
    public short lowestSupportedVersion() {
        return 0;
    }
    
    @Override
    public short highestSupportedVersion() {
        return 3;
    }
    
    @Override
    public void read(Readable _readable, short _version) {
        this.errorCode = _readable.readShort();
        {
            if (_version >= 3) {
                int arrayLength;
                arrayLength = _readable.readUnsignedVarint() - 1;
                if (arrayLength < 0) {
                    throw new RuntimeException("non-nullable field apiKeys was serialized as null");
                } else {
                    ApiVersionsResponseKeyCollection newCollection = new ApiVersionsResponseKeyCollection(arrayLength);
                    for (int i = 0; i < arrayLength; i++) {
                        newCollection.add(new ApiVersionsResponseKey(_readable, _version));
                    }
                    this.apiKeys = newCollection;
                }
            } else {
                int arrayLength;
                arrayLength = _readable.readInt();
                if (arrayLength < 0) {
                    throw new RuntimeException("non-nullable field apiKeys was serialized as null");
                } else {
                    ApiVersionsResponseKeyCollection newCollection = new ApiVersionsResponseKeyCollection(arrayLength);
                    for (int i = 0; i < arrayLength; i++) {
                        newCollection.add(new ApiVersionsResponseKey(_readable, _version));
                    }
                    this.apiKeys = newCollection;
                }
            }
        }
        if (_version >= 1) {
            this.throttleTimeMs = _readable.readInt();
        } else {
            this.throttleTimeMs = 0;
        }
        {
            this.supportedFeatures = new SupportedFeatureKeyCollection(0);
        }
        this.finalizedFeaturesEpoch = -1L;
        {
            this.finalizedFeatures = new FinalizedFeatureKeyCollection(0);
        }
        this._unknownTaggedFields = null;
        if (_version >= 3) {
            int _numTaggedFields = _readable.readUnsignedVarint();
            for (int _i = 0; _i < _numTaggedFields; _i++) {
                int _tag = _readable.readUnsignedVarint();
                int _size = _readable.readUnsignedVarint();
                switch (_tag) {
                    case 0: {
                        int arrayLength;
                        arrayLength = _readable.readUnsignedVarint() - 1;
                        if (arrayLength < 0) {
                            throw new RuntimeException("non-nullable field supportedFeatures was serialized as null");
                        } else {
                            SupportedFeatureKeyCollection newCollection = new SupportedFeatureKeyCollection(arrayLength);
                            for (int i = 0; i < arrayLength; i++) {
                                newCollection.add(new SupportedFeatureKey(_readable, _version));
                            }
                            this.supportedFeatures = newCollection;
                        }
                        break;
                    }
                    case 1: {
                        this.finalizedFeaturesEpoch = _readable.readLong();
                        break;
                    }
                    case 2: {
                        int arrayLength;
                        arrayLength = _readable.readUnsignedVarint() - 1;
                        if (arrayLength < 0) {
                            throw new RuntimeException("non-nullable field finalizedFeatures was serialized as null");
                        } else {
                            FinalizedFeatureKeyCollection newCollection = new FinalizedFeatureKeyCollection(arrayLength);
                            for (int i = 0; i < arrayLength; i++) {
                                newCollection.add(new FinalizedFeatureKey(_readable, _version));
                            }
                            this.finalizedFeatures = newCollection;
                        }
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
        _writable.writeShort(errorCode);
        if (_version >= 3) {
            _writable.writeUnsignedVarint(apiKeys.size() + 1);
            for (ApiVersionsResponseKey apiKeysElement : apiKeys) {
                apiKeysElement.write(_writable, _cache, _version);
            }
        } else {
            _writable.writeInt(apiKeys.size());
            for (ApiVersionsResponseKey apiKeysElement : apiKeys) {
                apiKeysElement.write(_writable, _cache, _version);
            }
        }
        if (_version >= 1) {
            _writable.writeInt(throttleTimeMs);
        }
        if (_version >= 3) {
            if (!this.supportedFeatures.isEmpty()) {
                _numTaggedFields++;
            }
        } else {
            if (!this.supportedFeatures.isEmpty()) {
                throw new UnsupportedVersionException("Attempted to write a non-default supportedFeatures at version " + _version);
            }
        }
        if (_version >= 3) {
            if (this.finalizedFeaturesEpoch != -1L) {
                _numTaggedFields++;
            }
        }
        if (_version >= 3) {
            if (!this.finalizedFeatures.isEmpty()) {
                _numTaggedFields++;
            }
        } else {
            if (!this.finalizedFeatures.isEmpty()) {
                throw new UnsupportedVersionException("Attempted to write a non-default finalizedFeatures at version " + _version);
            }
        }
        RawTaggedFieldWriter _rawWriter = RawTaggedFieldWriter.forFields(_unknownTaggedFields);
        _numTaggedFields += _rawWriter.numFields();
        if (_version >= 3) {
            _writable.writeUnsignedVarint(_numTaggedFields);
            {
                if (!this.supportedFeatures.isEmpty()) {
                    _writable.writeUnsignedVarint(0);
                    _writable.writeUnsignedVarint(_cache.getArraySizeInBytes(this.supportedFeatures));
                    _writable.writeUnsignedVarint(supportedFeatures.size() + 1);
                    for (SupportedFeatureKey supportedFeaturesElement : supportedFeatures) {
                        supportedFeaturesElement.write(_writable, _cache, _version);
                    }
                }
            }
            {
                if (this.finalizedFeaturesEpoch != -1L) {
                    _writable.writeUnsignedVarint(1);
                    _writable.writeUnsignedVarint(8);
                    _writable.writeLong(finalizedFeaturesEpoch);
                }
            }
            {
                if (!this.finalizedFeatures.isEmpty()) {
                    _writable.writeUnsignedVarint(2);
                    _writable.writeUnsignedVarint(_cache.getArraySizeInBytes(this.finalizedFeatures));
                    _writable.writeUnsignedVarint(finalizedFeatures.size() + 1);
                    for (FinalizedFeatureKey finalizedFeaturesElement : finalizedFeatures) {
                        finalizedFeaturesElement.write(_writable, _cache, _version);
                    }
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
        NavigableMap<Integer, Object> _taggedFields = null;
        this._unknownTaggedFields = null;
        if (_version >= 3) {
            _taggedFields = (NavigableMap<Integer, Object>) struct.get("_tagged_fields");
        }
        this.errorCode = struct.getShort("error_code");
        {
            Object[] _nestedObjects = struct.getArray("api_keys");
            this.apiKeys = new ApiVersionsResponseKeyCollection(_nestedObjects.length);
            for (Object nestedObject : _nestedObjects) {
                this.apiKeys.add(new ApiVersionsResponseKey((Struct) nestedObject, _version));
            }
        }
        if (_version >= 1) {
            this.throttleTimeMs = struct.getInt("throttle_time_ms");
        } else {
            this.throttleTimeMs = 0;
        }
        if (_version >= 3) {
            if (_taggedFields.containsKey(0)) {
                Object[] _nestedObjects = (Object[]) _taggedFields.remove(0);
                this.supportedFeatures = new SupportedFeatureKeyCollection(_nestedObjects.length);
                for (Object nestedObject : _nestedObjects) {
                    this.supportedFeatures.add(new SupportedFeatureKey((Struct) nestedObject, _version));
                }
            } else {
                this.supportedFeatures = new SupportedFeatureKeyCollection(0);
            }
        } else {
            this.supportedFeatures = new SupportedFeatureKeyCollection(0);
        }
        if (_version >= 3) {
            if (_taggedFields.containsKey(1)) {
                this.finalizedFeaturesEpoch = (Long) _taggedFields.remove(1);
            } else {
                this.finalizedFeaturesEpoch = -1L;
            }
        } else {
            this.finalizedFeaturesEpoch = -1L;
        }
        if (_version >= 3) {
            if (_taggedFields.containsKey(2)) {
                Object[] _nestedObjects = (Object[]) _taggedFields.remove(2);
                this.finalizedFeatures = new FinalizedFeatureKeyCollection(_nestedObjects.length);
                for (Object nestedObject : _nestedObjects) {
                    this.finalizedFeatures.add(new FinalizedFeatureKey((Struct) nestedObject, _version));
                }
            } else {
                this.finalizedFeatures = new FinalizedFeatureKeyCollection(0);
            }
        } else {
            this.finalizedFeatures = new FinalizedFeatureKeyCollection(0);
        }
        if (_version >= 3) {
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
        if (_version >= 3) {
            _taggedFields = new TreeMap<>();
        }
        Struct struct = new Struct(SCHEMAS[_version]);
        struct.set("error_code", this.errorCode);
        {
            Struct[] _nestedObjects = new Struct[apiKeys.size()];
            int i = 0;
            for (ApiVersionsResponseKey element : this.apiKeys) {
                _nestedObjects[i++] = element.toStruct(_version);
            }
            struct.set("api_keys", (Object[]) _nestedObjects);
        }
        if (_version >= 1) {
            struct.set("throttle_time_ms", this.throttleTimeMs);
        }
        if (_version >= 3) {
            if (!this.supportedFeatures.isEmpty()) {
                Struct[] _nestedObjects = new Struct[supportedFeatures.size()];
                int i = 0;
                for (SupportedFeatureKey element : this.supportedFeatures) {
                    _nestedObjects[i++] = element.toStruct(_version);
                }
                _taggedFields.put(0, _nestedObjects);
            }
        } else {
            if (!this.supportedFeatures.isEmpty()) {
                throw new UnsupportedVersionException("Attempted to write a non-default supportedFeatures at version " + _version);
            }
        }
        if (_version >= 3) {
            if (this.finalizedFeaturesEpoch != -1L) {
                _taggedFields.put(1, finalizedFeaturesEpoch);
            }
        }
        if (_version >= 3) {
            if (!this.finalizedFeatures.isEmpty()) {
                Struct[] _nestedObjects = new Struct[finalizedFeatures.size()];
                int i = 0;
                for (FinalizedFeatureKey element : this.finalizedFeatures) {
                    _nestedObjects[i++] = element.toStruct(_version);
                }
                _taggedFields.put(2, _nestedObjects);
            }
        } else {
            if (!this.finalizedFeatures.isEmpty()) {
                throw new UnsupportedVersionException("Attempted to write a non-default finalizedFeatures at version " + _version);
            }
        }
        if (_version >= 3) {
            struct.set("_tagged_fields", _taggedFields);
        }
        return struct;
    }
    
    @Override
    public int size(ObjectSerializationCache _cache, short _version) {
        int _size = 0, _numTaggedFields = 0;
        _size += 2;
        {
            int _arraySize = 0;
            if (_version >= 3) {
                _arraySize += ByteUtils.sizeOfUnsignedVarint(apiKeys.size() + 1);
            } else {
                _arraySize += 4;
            }
            for (ApiVersionsResponseKey apiKeysElement : apiKeys) {
                _arraySize += apiKeysElement.size(_cache, _version);
            }
            _size += _arraySize;
        }
        if (_version >= 1) {
            _size += 4;
        }
        if (_version >= 3) {
            {
                if (!this.supportedFeatures.isEmpty()) {
                    _numTaggedFields++;
                    _size += 1;
                    int _arraySize = 0;
                    _arraySize += ByteUtils.sizeOfUnsignedVarint(supportedFeatures.size() + 1);
                    for (SupportedFeatureKey supportedFeaturesElement : supportedFeatures) {
                        _arraySize += supportedFeaturesElement.size(_cache, _version);
                    }
                    _cache.setArraySizeInBytes(supportedFeatures, _arraySize);
                    _size += _arraySize + ByteUtils.sizeOfUnsignedVarint(_arraySize);
                }
            }
        }
        if (_version >= 3) {
            if (this.finalizedFeaturesEpoch != -1L) {
                _numTaggedFields++;
                _size += 1;
                _size += 1;
                _size += 8;
            }
        }
        if (_version >= 3) {
            {
                if (!this.finalizedFeatures.isEmpty()) {
                    _numTaggedFields++;
                    _size += 1;
                    int _arraySize = 0;
                    _arraySize += ByteUtils.sizeOfUnsignedVarint(finalizedFeatures.size() + 1);
                    for (FinalizedFeatureKey finalizedFeaturesElement : finalizedFeatures) {
                        _arraySize += finalizedFeaturesElement.size(_cache, _version);
                    }
                    _cache.setArraySizeInBytes(finalizedFeatures, _arraySize);
                    _size += _arraySize + ByteUtils.sizeOfUnsignedVarint(_arraySize);
                }
            }
        }
        if (_unknownTaggedFields != null) {
            _numTaggedFields += _unknownTaggedFields.size();
            for (RawTaggedField _field : _unknownTaggedFields) {
                _size += ByteUtils.sizeOfUnsignedVarint(_field.tag());
                _size += ByteUtils.sizeOfUnsignedVarint(_field.size());
                _size += _field.size();
            }
        }
        if (_version >= 3) {
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
        if (!(obj instanceof ApiVersionsResponseData)) return false;
        ApiVersionsResponseData other = (ApiVersionsResponseData) obj;
        if (errorCode != other.errorCode) return false;
        if (this.apiKeys == null) {
            if (other.apiKeys != null) return false;
        } else {
            if (!this.apiKeys.equals(other.apiKeys)) return false;
        }
        if (throttleTimeMs != other.throttleTimeMs) return false;
        if (this.supportedFeatures == null) {
            if (other.supportedFeatures != null) return false;
        } else {
            if (!this.supportedFeatures.equals(other.supportedFeatures)) return false;
        }
        if (finalizedFeaturesEpoch != other.finalizedFeaturesEpoch) return false;
        if (this.finalizedFeatures == null) {
            if (other.finalizedFeatures != null) return false;
        } else {
            if (!this.finalizedFeatures.equals(other.finalizedFeatures)) return false;
        }
        return MessageUtil.compareRawTaggedFields(_unknownTaggedFields, other._unknownTaggedFields);
    }
    
    @Override
    public int hashCode() {
        int hashCode = 0;
        hashCode = 31 * hashCode + errorCode;
        hashCode = 31 * hashCode + (apiKeys == null ? 0 : apiKeys.hashCode());
        hashCode = 31 * hashCode + throttleTimeMs;
        hashCode = 31 * hashCode + (supportedFeatures == null ? 0 : supportedFeatures.hashCode());
        hashCode = 31 * hashCode + ((int) (finalizedFeaturesEpoch >> 32) ^ (int) finalizedFeaturesEpoch);
        hashCode = 31 * hashCode + (finalizedFeatures == null ? 0 : finalizedFeatures.hashCode());
        return hashCode;
    }
    
    @Override
    public ApiVersionsResponseData duplicate() {
        ApiVersionsResponseData _duplicate = new ApiVersionsResponseData();
        _duplicate.errorCode = errorCode;
        ApiVersionsResponseKeyCollection newApiKeys = new ApiVersionsResponseKeyCollection(apiKeys.size());
        for (ApiVersionsResponseKey _element : apiKeys) {
            newApiKeys.add(_element.duplicate());
        }
        _duplicate.apiKeys = newApiKeys;
        _duplicate.throttleTimeMs = throttleTimeMs;
        SupportedFeatureKeyCollection newSupportedFeatures = new SupportedFeatureKeyCollection(supportedFeatures.size());
        for (SupportedFeatureKey _element : supportedFeatures) {
            newSupportedFeatures.add(_element.duplicate());
        }
        _duplicate.supportedFeatures = newSupportedFeatures;
        _duplicate.finalizedFeaturesEpoch = finalizedFeaturesEpoch;
        FinalizedFeatureKeyCollection newFinalizedFeatures = new FinalizedFeatureKeyCollection(finalizedFeatures.size());
        for (FinalizedFeatureKey _element : finalizedFeatures) {
            newFinalizedFeatures.add(_element.duplicate());
        }
        _duplicate.finalizedFeatures = newFinalizedFeatures;
        return _duplicate;
    }
    
    @Override
    public String toString() {
        return "ApiVersionsResponseData("
            + "errorCode=" + errorCode
            + ", apiKeys=" + MessageUtil.deepToString(apiKeys.iterator())
            + ", throttleTimeMs=" + throttleTimeMs
            + ", supportedFeatures=" + MessageUtil.deepToString(supportedFeatures.iterator())
            + ", finalizedFeaturesEpoch=" + finalizedFeaturesEpoch
            + ", finalizedFeatures=" + MessageUtil.deepToString(finalizedFeatures.iterator())
            + ")";
    }
    
    public short errorCode() {
        return this.errorCode;
    }
    
    public ApiVersionsResponseKeyCollection apiKeys() {
        return this.apiKeys;
    }
    
    public int throttleTimeMs() {
        return this.throttleTimeMs;
    }
    
    public SupportedFeatureKeyCollection supportedFeatures() {
        return this.supportedFeatures;
    }
    
    public long finalizedFeaturesEpoch() {
        return this.finalizedFeaturesEpoch;
    }
    
    public FinalizedFeatureKeyCollection finalizedFeatures() {
        return this.finalizedFeatures;
    }
    
    @Override
    public List<RawTaggedField> unknownTaggedFields() {
        if (_unknownTaggedFields == null) {
            _unknownTaggedFields = new ArrayList<>(0);
        }
        return _unknownTaggedFields;
    }
    
    public ApiVersionsResponseData setErrorCode(short v) {
        this.errorCode = v;
        return this;
    }
    
    public ApiVersionsResponseData setApiKeys(ApiVersionsResponseKeyCollection v) {
        this.apiKeys = v;
        return this;
    }
    
    public ApiVersionsResponseData setThrottleTimeMs(int v) {
        this.throttleTimeMs = v;
        return this;
    }
    
    public ApiVersionsResponseData setSupportedFeatures(SupportedFeatureKeyCollection v) {
        this.supportedFeatures = v;
        return this;
    }
    
    public ApiVersionsResponseData setFinalizedFeaturesEpoch(long v) {
        this.finalizedFeaturesEpoch = v;
        return this;
    }
    
    public ApiVersionsResponseData setFinalizedFeatures(FinalizedFeatureKeyCollection v) {
        this.finalizedFeatures = v;
        return this;
    }
    
    public static class ApiVersionsResponseKey implements Message, ImplicitLinkedHashMultiCollection.Element {
        short apiKey;
        short minVersion;
        short maxVersion;
        private List<RawTaggedField> _unknownTaggedFields;
        private int next;
        private int prev;
        
        public static final Schema SCHEMA_0 =
            new Schema(
                new Field("api_key", Type.INT16, "The API index."),
                new Field("min_version", Type.INT16, "The minimum supported version, inclusive."),
                new Field("max_version", Type.INT16, "The maximum supported version, inclusive.")
            );
        
        public static final Schema SCHEMA_1 = SCHEMA_0;
        
        public static final Schema SCHEMA_2 = SCHEMA_1;
        
        public static final Schema SCHEMA_3 =
            new Schema(
                new Field("api_key", Type.INT16, "The API index."),
                new Field("min_version", Type.INT16, "The minimum supported version, inclusive."),
                new Field("max_version", Type.INT16, "The maximum supported version, inclusive."),
                TaggedFieldsSection.of(
                )
            );
        
        public static final Schema[] SCHEMAS = new Schema[] {
            SCHEMA_0,
            SCHEMA_1,
            SCHEMA_2,
            SCHEMA_3
        };
        
        public static final short LOWEST_SUPPORTED_VERSION = 0;
        public static final short HIGHEST_SUPPORTED_VERSION = 3;
        
        public ApiVersionsResponseKey(Readable _readable, short _version) {
            read(_readable, _version);
            this.prev = ImplicitLinkedHashCollection.INVALID_INDEX;
            this.next = ImplicitLinkedHashCollection.INVALID_INDEX;
        }
        
        public ApiVersionsResponseKey(Struct _struct, short _version) {
            fromStruct(_struct, _version);
            this.prev = ImplicitLinkedHashCollection.INVALID_INDEX;
            this.next = ImplicitLinkedHashCollection.INVALID_INDEX;
        }
        
        public ApiVersionsResponseKey() {
            this.apiKey = (short) 0;
            this.minVersion = (short) 0;
            this.maxVersion = (short) 0;
            this.prev = ImplicitLinkedHashCollection.INVALID_INDEX;
            this.next = ImplicitLinkedHashCollection.INVALID_INDEX;
        }
        
        
        @Override
        public short lowestSupportedVersion() {
            return 0;
        }
        
        @Override
        public short highestSupportedVersion() {
            return 3;
        }
        
        @Override
        public void read(Readable _readable, short _version) {
            if (_version > 3) {
                throw new UnsupportedVersionException("Can't read version " + _version + " of ApiVersionsResponseKey");
            }
            this.apiKey = _readable.readShort();
            this.minVersion = _readable.readShort();
            this.maxVersion = _readable.readShort();
            this._unknownTaggedFields = null;
            if (_version >= 3) {
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
            _writable.writeShort(apiKey);
            _writable.writeShort(minVersion);
            _writable.writeShort(maxVersion);
            RawTaggedFieldWriter _rawWriter = RawTaggedFieldWriter.forFields(_unknownTaggedFields);
            _numTaggedFields += _rawWriter.numFields();
            if (_version >= 3) {
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
            if (_version > 3) {
                throw new UnsupportedVersionException("Can't read version " + _version + " of ApiVersionsResponseKey");
            }
            NavigableMap<Integer, Object> _taggedFields = null;
            this._unknownTaggedFields = null;
            if (_version >= 3) {
                _taggedFields = (NavigableMap<Integer, Object>) struct.get("_tagged_fields");
            }
            this.apiKey = struct.getShort("api_key");
            this.minVersion = struct.getShort("min_version");
            this.maxVersion = struct.getShort("max_version");
            if (_version >= 3) {
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
            if (_version > 3) {
                throw new UnsupportedVersionException("Can't write version " + _version + " of ApiVersionsResponseKey");
            }
            TreeMap<Integer, Object> _taggedFields = null;
            if (_version >= 3) {
                _taggedFields = new TreeMap<>();
            }
            Struct struct = new Struct(SCHEMAS[_version]);
            struct.set("api_key", this.apiKey);
            struct.set("min_version", this.minVersion);
            struct.set("max_version", this.maxVersion);
            if (_version >= 3) {
                struct.set("_tagged_fields", _taggedFields);
            }
            return struct;
        }
        
        @Override
        public int size(ObjectSerializationCache _cache, short _version) {
            int _size = 0, _numTaggedFields = 0;
            if (_version > 3) {
                throw new UnsupportedVersionException("Can't size version " + _version + " of ApiVersionsResponseKey");
            }
            _size += 2;
            _size += 2;
            _size += 2;
            if (_unknownTaggedFields != null) {
                _numTaggedFields += _unknownTaggedFields.size();
                for (RawTaggedField _field : _unknownTaggedFields) {
                    _size += ByteUtils.sizeOfUnsignedVarint(_field.tag());
                    _size += ByteUtils.sizeOfUnsignedVarint(_field.size());
                    _size += _field.size();
                }
            }
            if (_version >= 3) {
                _size += ByteUtils.sizeOfUnsignedVarint(_numTaggedFields);
            } else {
                if (_numTaggedFields > 0) {
                    throw new UnsupportedVersionException("Tagged fields were set, but version " + _version + " of this message does not support them.");
                }
            }
            return _size;
        }
        
        @Override
        public boolean elementKeysAreEqual(Object obj) {
            if (!(obj instanceof ApiVersionsResponseKey)) return false;
            ApiVersionsResponseKey other = (ApiVersionsResponseKey) obj;
            if (apiKey != other.apiKey) return false;
            return true;
        }
        
        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof ApiVersionsResponseKey)) return false;
            ApiVersionsResponseKey other = (ApiVersionsResponseKey) obj;
            if (apiKey != other.apiKey) return false;
            if (minVersion != other.minVersion) return false;
            if (maxVersion != other.maxVersion) return false;
            return MessageUtil.compareRawTaggedFields(_unknownTaggedFields, other._unknownTaggedFields);
        }
        
        @Override
        public int hashCode() {
            int hashCode = 0;
            hashCode = 31 * hashCode + apiKey;
            return hashCode;
        }
        
        @Override
        public ApiVersionsResponseKey duplicate() {
            ApiVersionsResponseKey _duplicate = new ApiVersionsResponseKey();
            _duplicate.apiKey = apiKey;
            _duplicate.minVersion = minVersion;
            _duplicate.maxVersion = maxVersion;
            return _duplicate;
        }
        
        @Override
        public String toString() {
            return "ApiVersionsResponseKey("
                + "apiKey=" + apiKey
                + ", minVersion=" + minVersion
                + ", maxVersion=" + maxVersion
                + ")";
        }
        
        public short apiKey() {
            return this.apiKey;
        }
        
        public short minVersion() {
            return this.minVersion;
        }
        
        public short maxVersion() {
            return this.maxVersion;
        }
        
        @Override
        public int next() {
            return this.next;
        }
        
        @Override
        public int prev() {
            return this.prev;
        }
        
        @Override
        public List<RawTaggedField> unknownTaggedFields() {
            if (_unknownTaggedFields == null) {
                _unknownTaggedFields = new ArrayList<>(0);
            }
            return _unknownTaggedFields;
        }
        
        public ApiVersionsResponseKey setApiKey(short v) {
            this.apiKey = v;
            return this;
        }
        
        public ApiVersionsResponseKey setMinVersion(short v) {
            this.minVersion = v;
            return this;
        }
        
        public ApiVersionsResponseKey setMaxVersion(short v) {
            this.maxVersion = v;
            return this;
        }
        
        @Override
        public void setNext(int v) {
            this.next = v;
        }
        
        @Override
        public void setPrev(int v) {
            this.prev = v;
        }
    }
    
    public static class ApiVersionsResponseKeyCollection extends ImplicitLinkedHashMultiCollection<ApiVersionsResponseKey> {
        public ApiVersionsResponseKeyCollection() {
            super();
        }
        
        public ApiVersionsResponseKeyCollection(int expectedNumElements) {
            super(expectedNumElements);
        }
        
        public ApiVersionsResponseKeyCollection(Iterator<ApiVersionsResponseKey> iterator) {
            super(iterator);
        }
        
        public ApiVersionsResponseKey find(short apiKey) {
            ApiVersionsResponseKey _key = new ApiVersionsResponseKey();
            _key.setApiKey(apiKey);
            return find(_key);
        }
        
        public List<ApiVersionsResponseKey> findAll(short apiKey) {
            ApiVersionsResponseKey _key = new ApiVersionsResponseKey();
            _key.setApiKey(apiKey);
            return findAll(_key);
        }
        
        public ApiVersionsResponseKeyCollection duplicate() {
            ApiVersionsResponseKeyCollection _duplicate = new ApiVersionsResponseKeyCollection(size());
            for (ApiVersionsResponseKey _element : this) {
                _duplicate.add(_element.duplicate());
            }
            return _duplicate;
        }
    }
    
    public static class SupportedFeatureKey implements Message, ImplicitLinkedHashMultiCollection.Element {
        String name;
        short minVersion;
        short maxVersion;
        private List<RawTaggedField> _unknownTaggedFields;
        private int next;
        private int prev;
        
        public static final Schema SCHEMA_3 =
            new Schema(
                new Field("name", Type.COMPACT_STRING, "The name of the feature."),
                new Field("min_version", Type.INT16, "The minimum supported version for the feature."),
                new Field("max_version", Type.INT16, "The maximum supported version for the feature."),
                TaggedFieldsSection.of(
                )
            );
        
        public static final Schema[] SCHEMAS = new Schema[] {
            null,
            null,
            null,
            SCHEMA_3
        };
        
        public static final short LOWEST_SUPPORTED_VERSION = 3;
        public static final short HIGHEST_SUPPORTED_VERSION = 3;
        
        public SupportedFeatureKey(Readable _readable, short _version) {
            read(_readable, _version);
            this.prev = ImplicitLinkedHashCollection.INVALID_INDEX;
            this.next = ImplicitLinkedHashCollection.INVALID_INDEX;
        }
        
        public SupportedFeatureKey(Struct _struct, short _version) {
            fromStruct(_struct, _version);
            this.prev = ImplicitLinkedHashCollection.INVALID_INDEX;
            this.next = ImplicitLinkedHashCollection.INVALID_INDEX;
        }
        
        public SupportedFeatureKey() {
            this.name = "";
            this.minVersion = (short) 0;
            this.maxVersion = (short) 0;
            this.prev = ImplicitLinkedHashCollection.INVALID_INDEX;
            this.next = ImplicitLinkedHashCollection.INVALID_INDEX;
        }
        
        
        @Override
        public short lowestSupportedVersion() {
            return 0;
        }
        
        @Override
        public short highestSupportedVersion() {
            return 3;
        }
        
        @Override
        public void read(Readable _readable, short _version) {
            if (_version > 3) {
                throw new UnsupportedVersionException("Can't read version " + _version + " of SupportedFeatureKey");
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
            this.minVersion = _readable.readShort();
            this.maxVersion = _readable.readShort();
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
            if (_version < 3) {
                throw new UnsupportedVersionException("Can't write version " + _version + " of SupportedFeatureKey");
            }
            int _numTaggedFields = 0;
            {
                byte[] _stringBytes = _cache.getSerializedValue(name);
                _writable.writeUnsignedVarint(_stringBytes.length + 1);
                _writable.writeByteArray(_stringBytes);
            }
            _writable.writeShort(minVersion);
            _writable.writeShort(maxVersion);
            RawTaggedFieldWriter _rawWriter = RawTaggedFieldWriter.forFields(_unknownTaggedFields);
            _numTaggedFields += _rawWriter.numFields();
            _writable.writeUnsignedVarint(_numTaggedFields);
            _rawWriter.writeRawTags(_writable, Integer.MAX_VALUE);
        }
        
        @SuppressWarnings("unchecked")
        @Override
        public void fromStruct(Struct struct, short _version) {
            if (_version > 3) {
                throw new UnsupportedVersionException("Can't read version " + _version + " of SupportedFeatureKey");
            }
            NavigableMap<Integer, Object> _taggedFields = null;
            this._unknownTaggedFields = null;
            _taggedFields = (NavigableMap<Integer, Object>) struct.get("_tagged_fields");
            this.name = struct.getString("name");
            this.minVersion = struct.getShort("min_version");
            this.maxVersion = struct.getShort("max_version");
            if (!_taggedFields.isEmpty()) {
                this._unknownTaggedFields = new ArrayList<>(_taggedFields.size());
                for (Entry<Integer, Object> entry : _taggedFields.entrySet()) {
                    this._unknownTaggedFields.add((RawTaggedField) entry.getValue());
                }
            }
        }
        
        @Override
        public Struct toStruct(short _version) {
            if (_version > 3) {
                throw new UnsupportedVersionException("Can't write version " + _version + " of SupportedFeatureKey");
            }
            TreeMap<Integer, Object> _taggedFields = null;
            _taggedFields = new TreeMap<>();
            Struct struct = new Struct(SCHEMAS[_version]);
            struct.set("name", this.name);
            struct.set("min_version", this.minVersion);
            struct.set("max_version", this.maxVersion);
            struct.set("_tagged_fields", _taggedFields);
            return struct;
        }
        
        @Override
        public int size(ObjectSerializationCache _cache, short _version) {
            int _size = 0, _numTaggedFields = 0;
            if (_version > 3) {
                throw new UnsupportedVersionException("Can't size version " + _version + " of SupportedFeatureKey");
            }
            {
                byte[] _stringBytes = name.getBytes(StandardCharsets.UTF_8);
                if (_stringBytes.length > 0x7fff) {
                    throw new RuntimeException("'name' field is too long to be serialized");
                }
                _cache.cacheSerializedValue(name, _stringBytes);
                _size += _stringBytes.length + ByteUtils.sizeOfUnsignedVarint(_stringBytes.length + 1);
            }
            _size += 2;
            _size += 2;
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
        public boolean elementKeysAreEqual(Object obj) {
            if (!(obj instanceof SupportedFeatureKey)) return false;
            SupportedFeatureKey other = (SupportedFeatureKey) obj;
            if (this.name == null) {
                if (other.name != null) return false;
            } else {
                if (!this.name.equals(other.name)) return false;
            }
            return true;
        }
        
        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof SupportedFeatureKey)) return false;
            SupportedFeatureKey other = (SupportedFeatureKey) obj;
            if (this.name == null) {
                if (other.name != null) return false;
            } else {
                if (!this.name.equals(other.name)) return false;
            }
            if (minVersion != other.minVersion) return false;
            if (maxVersion != other.maxVersion) return false;
            return MessageUtil.compareRawTaggedFields(_unknownTaggedFields, other._unknownTaggedFields);
        }
        
        @Override
        public int hashCode() {
            int hashCode = 0;
            hashCode = 31 * hashCode + (name == null ? 0 : name.hashCode());
            return hashCode;
        }
        
        @Override
        public SupportedFeatureKey duplicate() {
            SupportedFeatureKey _duplicate = new SupportedFeatureKey();
            _duplicate.name = name;
            _duplicate.minVersion = minVersion;
            _duplicate.maxVersion = maxVersion;
            return _duplicate;
        }
        
        @Override
        public String toString() {
            return "SupportedFeatureKey("
                + "name=" + ((name == null) ? "null" : "'" + name.toString() + "'")
                + ", minVersion=" + minVersion
                + ", maxVersion=" + maxVersion
                + ")";
        }
        
        public String name() {
            return this.name;
        }
        
        public short minVersion() {
            return this.minVersion;
        }
        
        public short maxVersion() {
            return this.maxVersion;
        }
        
        @Override
        public int next() {
            return this.next;
        }
        
        @Override
        public int prev() {
            return this.prev;
        }
        
        @Override
        public List<RawTaggedField> unknownTaggedFields() {
            if (_unknownTaggedFields == null) {
                _unknownTaggedFields = new ArrayList<>(0);
            }
            return _unknownTaggedFields;
        }
        
        public SupportedFeatureKey setName(String v) {
            this.name = v;
            return this;
        }
        
        public SupportedFeatureKey setMinVersion(short v) {
            this.minVersion = v;
            return this;
        }
        
        public SupportedFeatureKey setMaxVersion(short v) {
            this.maxVersion = v;
            return this;
        }
        
        @Override
        public void setNext(int v) {
            this.next = v;
        }
        
        @Override
        public void setPrev(int v) {
            this.prev = v;
        }
    }
    
    public static class SupportedFeatureKeyCollection extends ImplicitLinkedHashMultiCollection<SupportedFeatureKey> {
        public SupportedFeatureKeyCollection() {
            super();
        }
        
        public SupportedFeatureKeyCollection(int expectedNumElements) {
            super(expectedNumElements);
        }
        
        public SupportedFeatureKeyCollection(Iterator<SupportedFeatureKey> iterator) {
            super(iterator);
        }
        
        public SupportedFeatureKey find(String name) {
            SupportedFeatureKey _key = new SupportedFeatureKey();
            _key.setName(name);
            return find(_key);
        }
        
        public List<SupportedFeatureKey> findAll(String name) {
            SupportedFeatureKey _key = new SupportedFeatureKey();
            _key.setName(name);
            return findAll(_key);
        }
        
        public SupportedFeatureKeyCollection duplicate() {
            SupportedFeatureKeyCollection _duplicate = new SupportedFeatureKeyCollection(size());
            for (SupportedFeatureKey _element : this) {
                _duplicate.add(_element.duplicate());
            }
            return _duplicate;
        }
    }
    
    public static class FinalizedFeatureKey implements Message, ImplicitLinkedHashMultiCollection.Element {
        String name;
        short maxVersionLevel;
        short minVersionLevel;
        private List<RawTaggedField> _unknownTaggedFields;
        private int next;
        private int prev;
        
        public static final Schema SCHEMA_3 =
            new Schema(
                new Field("name", Type.COMPACT_STRING, "The name of the feature."),
                new Field("max_version_level", Type.INT16, "The cluster-wide finalized max version level for the feature."),
                new Field("min_version_level", Type.INT16, "The cluster-wide finalized min version level for the feature."),
                TaggedFieldsSection.of(
                )
            );
        
        public static final Schema[] SCHEMAS = new Schema[] {
            null,
            null,
            null,
            SCHEMA_3
        };
        
        public static final short LOWEST_SUPPORTED_VERSION = 3;
        public static final short HIGHEST_SUPPORTED_VERSION = 3;
        
        public FinalizedFeatureKey(Readable _readable, short _version) {
            read(_readable, _version);
            this.prev = ImplicitLinkedHashCollection.INVALID_INDEX;
            this.next = ImplicitLinkedHashCollection.INVALID_INDEX;
        }
        
        public FinalizedFeatureKey(Struct _struct, short _version) {
            fromStruct(_struct, _version);
            this.prev = ImplicitLinkedHashCollection.INVALID_INDEX;
            this.next = ImplicitLinkedHashCollection.INVALID_INDEX;
        }
        
        public FinalizedFeatureKey() {
            this.name = "";
            this.maxVersionLevel = (short) 0;
            this.minVersionLevel = (short) 0;
            this.prev = ImplicitLinkedHashCollection.INVALID_INDEX;
            this.next = ImplicitLinkedHashCollection.INVALID_INDEX;
        }
        
        
        @Override
        public short lowestSupportedVersion() {
            return 0;
        }
        
        @Override
        public short highestSupportedVersion() {
            return 3;
        }
        
        @Override
        public void read(Readable _readable, short _version) {
            if (_version > 3) {
                throw new UnsupportedVersionException("Can't read version " + _version + " of FinalizedFeatureKey");
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
            this.maxVersionLevel = _readable.readShort();
            this.minVersionLevel = _readable.readShort();
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
            if (_version < 3) {
                throw new UnsupportedVersionException("Can't write version " + _version + " of FinalizedFeatureKey");
            }
            int _numTaggedFields = 0;
            {
                byte[] _stringBytes = _cache.getSerializedValue(name);
                _writable.writeUnsignedVarint(_stringBytes.length + 1);
                _writable.writeByteArray(_stringBytes);
            }
            _writable.writeShort(maxVersionLevel);
            _writable.writeShort(minVersionLevel);
            RawTaggedFieldWriter _rawWriter = RawTaggedFieldWriter.forFields(_unknownTaggedFields);
            _numTaggedFields += _rawWriter.numFields();
            _writable.writeUnsignedVarint(_numTaggedFields);
            _rawWriter.writeRawTags(_writable, Integer.MAX_VALUE);
        }
        
        @SuppressWarnings("unchecked")
        @Override
        public void fromStruct(Struct struct, short _version) {
            if (_version > 3) {
                throw new UnsupportedVersionException("Can't read version " + _version + " of FinalizedFeatureKey");
            }
            NavigableMap<Integer, Object> _taggedFields = null;
            this._unknownTaggedFields = null;
            _taggedFields = (NavigableMap<Integer, Object>) struct.get("_tagged_fields");
            this.name = struct.getString("name");
            this.maxVersionLevel = struct.getShort("max_version_level");
            this.minVersionLevel = struct.getShort("min_version_level");
            if (!_taggedFields.isEmpty()) {
                this._unknownTaggedFields = new ArrayList<>(_taggedFields.size());
                for (Entry<Integer, Object> entry : _taggedFields.entrySet()) {
                    this._unknownTaggedFields.add((RawTaggedField) entry.getValue());
                }
            }
        }
        
        @Override
        public Struct toStruct(short _version) {
            if (_version > 3) {
                throw new UnsupportedVersionException("Can't write version " + _version + " of FinalizedFeatureKey");
            }
            TreeMap<Integer, Object> _taggedFields = null;
            _taggedFields = new TreeMap<>();
            Struct struct = new Struct(SCHEMAS[_version]);
            struct.set("name", this.name);
            struct.set("max_version_level", this.maxVersionLevel);
            struct.set("min_version_level", this.minVersionLevel);
            struct.set("_tagged_fields", _taggedFields);
            return struct;
        }
        
        @Override
        public int size(ObjectSerializationCache _cache, short _version) {
            int _size = 0, _numTaggedFields = 0;
            if (_version > 3) {
                throw new UnsupportedVersionException("Can't size version " + _version + " of FinalizedFeatureKey");
            }
            {
                byte[] _stringBytes = name.getBytes(StandardCharsets.UTF_8);
                if (_stringBytes.length > 0x7fff) {
                    throw new RuntimeException("'name' field is too long to be serialized");
                }
                _cache.cacheSerializedValue(name, _stringBytes);
                _size += _stringBytes.length + ByteUtils.sizeOfUnsignedVarint(_stringBytes.length + 1);
            }
            _size += 2;
            _size += 2;
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
        public boolean elementKeysAreEqual(Object obj) {
            if (!(obj instanceof FinalizedFeatureKey)) return false;
            FinalizedFeatureKey other = (FinalizedFeatureKey) obj;
            if (this.name == null) {
                if (other.name != null) return false;
            } else {
                if (!this.name.equals(other.name)) return false;
            }
            return true;
        }
        
        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof FinalizedFeatureKey)) return false;
            FinalizedFeatureKey other = (FinalizedFeatureKey) obj;
            if (this.name == null) {
                if (other.name != null) return false;
            } else {
                if (!this.name.equals(other.name)) return false;
            }
            if (maxVersionLevel != other.maxVersionLevel) return false;
            if (minVersionLevel != other.minVersionLevel) return false;
            return MessageUtil.compareRawTaggedFields(_unknownTaggedFields, other._unknownTaggedFields);
        }
        
        @Override
        public int hashCode() {
            int hashCode = 0;
            hashCode = 31 * hashCode + (name == null ? 0 : name.hashCode());
            return hashCode;
        }
        
        @Override
        public FinalizedFeatureKey duplicate() {
            FinalizedFeatureKey _duplicate = new FinalizedFeatureKey();
            _duplicate.name = name;
            _duplicate.maxVersionLevel = maxVersionLevel;
            _duplicate.minVersionLevel = minVersionLevel;
            return _duplicate;
        }
        
        @Override
        public String toString() {
            return "FinalizedFeatureKey("
                + "name=" + ((name == null) ? "null" : "'" + name.toString() + "'")
                + ", maxVersionLevel=" + maxVersionLevel
                + ", minVersionLevel=" + minVersionLevel
                + ")";
        }
        
        public String name() {
            return this.name;
        }
        
        public short maxVersionLevel() {
            return this.maxVersionLevel;
        }
        
        public short minVersionLevel() {
            return this.minVersionLevel;
        }
        
        @Override
        public int next() {
            return this.next;
        }
        
        @Override
        public int prev() {
            return this.prev;
        }
        
        @Override
        public List<RawTaggedField> unknownTaggedFields() {
            if (_unknownTaggedFields == null) {
                _unknownTaggedFields = new ArrayList<>(0);
            }
            return _unknownTaggedFields;
        }
        
        public FinalizedFeatureKey setName(String v) {
            this.name = v;
            return this;
        }
        
        public FinalizedFeatureKey setMaxVersionLevel(short v) {
            this.maxVersionLevel = v;
            return this;
        }
        
        public FinalizedFeatureKey setMinVersionLevel(short v) {
            this.minVersionLevel = v;
            return this;
        }
        
        @Override
        public void setNext(int v) {
            this.next = v;
        }
        
        @Override
        public void setPrev(int v) {
            this.prev = v;
        }
    }
    
    public static class FinalizedFeatureKeyCollection extends ImplicitLinkedHashMultiCollection<FinalizedFeatureKey> {
        public FinalizedFeatureKeyCollection() {
            super();
        }
        
        public FinalizedFeatureKeyCollection(int expectedNumElements) {
            super(expectedNumElements);
        }
        
        public FinalizedFeatureKeyCollection(Iterator<FinalizedFeatureKey> iterator) {
            super(iterator);
        }
        
        public FinalizedFeatureKey find(String name) {
            FinalizedFeatureKey _key = new FinalizedFeatureKey();
            _key.setName(name);
            return find(_key);
        }
        
        public List<FinalizedFeatureKey> findAll(String name) {
            FinalizedFeatureKey _key = new FinalizedFeatureKey();
            _key.setName(name);
            return findAll(_key);
        }
        
        public FinalizedFeatureKeyCollection duplicate() {
            FinalizedFeatureKeyCollection _duplicate = new FinalizedFeatureKeyCollection(size());
            for (FinalizedFeatureKey _element : this) {
                _duplicate.add(_element.duplicate());
            }
            return _duplicate;
        }
    }
}
