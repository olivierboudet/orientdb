package com.orientechnologies.orient.core.serialization.serializer.record.binary;

import com.google.protobuf.*;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.types.OModifiableInteger;
import com.orientechnologies.common.util.OPair;
import com.orientechnologies.common.util.OTriple;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentEntry;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.*;

public class OProtobufDocumentBinarySerializerV0 implements ODocumentSerializer {
  private static final Charset UTF8 = Charset.forName("UTF-8");

  /**
   * <code>
   *  message Record {
   *    oneof data {
   *      bytes raw = 1;
   *      Document document = 2;
   *    }
   *
   *    RID rid = 3;
   *    int64 version = 4;
   *  }
   * </code>
   */
  @Override
  public void serialize(ODocument document, BytesContainer bytes, boolean iClassOnly) {
    try {
      final OClass clazz = ODocumentInternal.getImmutableSchemaClass(document);

      final Map<String, OProperty> props = clazz != null ? clazz.propertiesMap() : null;
      final List<Map.Entry<String, ODocumentEntry>> fields = new ArrayList<Map.Entry<String, ODocumentEntry>>(
          ODocumentInternal.rawEntries(document));

      int containerSize = 0;
      final ArrayList<Integer> messagesSize = new ArrayList<Integer>(64);
      final List<OPair<Integer, Integer>> hashCodePositionMap = new ArrayList<OPair<Integer, Integer>>();

      // Document document = 4;
      final int docSize[] = computeDocumentSize(clazz != null ? clazz.getName() : null, document, fields, props,
          hashCodePositionMap);
      for (int ds : docSize) {
        messagesSize.add(ds);
      }

      containerSize += docSize[0];
      containerSize += computeEmbeddedMessageHeaderSize(2, docSize[0]);

      final int offset = bytes.alloc(containerSize);

      final CodedOutputStream codedOutputStream = CodedOutputStream.newInstance(bytes.bytes, offset, containerSize);

      final Iterator<Integer> sizesIterator = messagesSize.iterator();

      writeEmbeddedMessageHeader(sizesIterator.next(), 2, codedOutputStream);
      writeDocument(clazz != null ? clazz.getName() : null, document, fields, hashCodePositionMap, codedOutputStream,
          sizesIterator);

      assert containerSize == codedOutputStream.getTotalBytesWritten();
    } catch (IOException ioe) {
      throw OException.wrapException(new OSerializationException("Error during document serialization"), ioe);
    }
  }

  /**
   * <code>
   *  message Record {
   *    oneof data {
   *      bytes raw = 1;
   *      Document document = 2;
   *    }
   *
   *    RID rid = 3;
   *    int64 version = 4;
   *  }
   * </code>
   */
  @Override
  public void deserialize(ODocument document, BytesContainer bytes) {
    while (bytes.offset < bytes.bytes.length) {
      final int tag = readRawVarint32(bytes);
      if (tag == 18) {
        readDocument(document, bytes, null, null);
        break;
      } else {
        skipField(tag, bytes);
      }
    }

    ORecordInternal.clearSource(document);
  }

  /**
   * <code>
   *  message Document {
   *    reserved 1;
   *    string class = 2;
   *    map<string, Item> fields = 3;
   *  }
   *  </code>
   */
  private static int[] computeDocumentSize(String className, ODocument document, List<Map.Entry<String, ODocumentEntry>> fields,
      final Map<String, OProperty> props, final List<OPair<Integer, Integer>> hashCodePositionMap) {

    int size = 0;

    if (className != null) {
      size += CodedOutputStream.computeStringSize(2, className);
    }

    int[] sizes = new int[fields.size() * 2 + 1];
    int sizesIndex = 2;

    final Map<String, Integer> fieldPositionMap = new HashMap<String, Integer>();
    for (Map.Entry<String, ODocumentEntry> entry : fields) {
      ODocumentEntry docEntry = entry.getValue();

      if (!docEntry.exist())
        continue;

      if (docEntry.property == null && props != null)
        docEntry.property = props.get(entry.getKey());

      final Object value = docEntry.value;

      if (value != null) {
        final OType type;

        if (docEntry.type != null) {
          type = docEntry.type;
        } else {
          type = getFieldType(docEntry);
          docEntry.type = type;
        }

        if (type == null) {
          throw new OSerializationException(
              "Impossible serialize value of type " + value.getClass() + " with the ODocument binary serializer");
        }

        final int entryPosition = size;
        final int[] entrySize = computeFieldMapEntrySize(entry.getKey(), value, type);

        size += entrySize[0];

        final int fieldMapEntryTagSize = CodedOutputStream.computeTagSize(3);
        final int fieldMapEntrySizeSize = CodedOutputStream.computeRawVarint32Size(entrySize[0]);
        size += fieldMapEntrySizeSize + fieldMapEntryTagSize;

        fieldPositionMap.put(entry.getKey(), entryPosition + fieldMapEntryTagSize);

        if (sizesIndex + entrySize.length > sizes.length) {
          int[] newSizes = new int[sizesIndex + entrySize.length];
          System.arraycopy(sizes, 1, newSizes, 1, sizes.length - 1);
          System.arraycopy(entrySize, 0, newSizes, sizesIndex, entrySize.length);
          sizesIndex += entrySize.length;
          sizes = newSizes;
        } else {
          System.arraycopy(entrySize, 0, sizes, sizesIndex, entrySize.length);
          sizesIndex += entrySize.length;
        }
      }
    }

    assert sizesIndex == sizes.length;

    int indexSize = 0;
    for (Map.Entry<String, Integer> entry : fieldPositionMap.entrySet()) {
      final int hashCode = entry.getKey().hashCode();
      hashCodePositionMap.add(new OPair<Integer, Integer>(hashCode, entry.getValue()));

      indexSize += OIntegerSerializer.INT_SIZE;
      indexSize += CodedOutputStream.computeRawVarint32Size(entry.getValue());
    }

    sizes[1] = indexSize;
    indexSize += computeEmbeddedMessageHeaderSize(1, indexSize);

    size += indexSize;

    sizes[0] = size;

    return sizes;
  }

  private static void writeDocument(String className, ODocument document, List<Map.Entry<String, ODocumentEntry>> fields,
      final List<OPair<Integer, Integer>> hashCodePositionMap, CodedOutputStream codedOutputStream, Iterator<Integer> sizesIterator)
          throws IOException {

    writeEmbeddedMessageHeader(sizesIterator.next(), 1, codedOutputStream);
    for (Map.Entry<Integer, Integer> indexEntry : hashCodePositionMap) {
      byte[] bhs = new byte[OIntegerSerializer.INT_SIZE];
      OIntegerSerializer.INSTANCE.serializeNative(indexEntry.getKey(), bhs, 0);
      codedOutputStream.writeRawBytes(bhs);
      codedOutputStream.writeRawVarint32(indexEntry.getValue());
    }

    if (className != null) {
      codedOutputStream.writeString(2, className);
    }

    for (Map.Entry<String, ODocumentEntry> entry : fields) {
      ODocumentEntry docEntry = entry.getValue();

      if (!docEntry.exist())
        continue;

      final Object value = docEntry.value;

      if (value != null) {
        final int messageSize = sizesIterator.next();
        writeEmbeddedMessageHeader(messageSize, 3, codedOutputStream);
        writeFieldMapEntry(entry.getKey(), value, docEntry.type, codedOutputStream, sizesIterator);
      }
    }
  }

  /**
   * The map syntax is equivalent to the following on the wire, so protocol buffers implementations that do not support maps can
   * still handle your data:<br/>
   * 
   * message MapFieldEntry {<br/>
   * key_type key = 1;<br/>
   * value_type value = 2;<br/>
   * }<br/>
   * repeated MapFieldEntry map_field = N;<br/>
   */
  private static int[] computeFieldMapEntrySize(String key, Object value, OType type) {
    int size = 0;

    final int stringKeySize = CodedOutputStream.computeStringSize(1, key);
    size += stringKeySize;

    final int[] itemSize = computeItemSize(value, type);
    size += itemSize[0];

    final int itemHeaderSize = computeEmbeddedMessageHeaderSize(2, itemSize[0]);
    size += itemHeaderSize;

    final int[] sizes = new int[itemSize.length + 1];
    sizes[0] = size;

    System.arraycopy(itemSize, 0, sizes, 1, itemSize.length);
    return sizes;
  }

  private static void writeFieldMapEntry(String key, Object value, OType type, CodedOutputStream codedOutputStream,
      Iterator<Integer> sizesIterator) throws IOException {
    codedOutputStream.writeString(1, key);

    final int messageSize = sizesIterator.next();

    writeEmbeddedMessageHeader(messageSize, 2, codedOutputStream);
    writeItem(value, type, codedOutputStream, sizesIterator);
  }

  /**
   * <code>
   * message Item {
   *  oneof value {
   *    sint32 val_int = 1; //com.orientechnologies.orient.core.metadata.schema.OType.INTEGER
   *    double val_double = 2; //com.orientechnologies.orient.core.metadata.schema.OType.DOUBLE
   *    string val_string = 3; //com.orientechnologies.orient.core.metadata.schema.OType.STRING
   *    sint64 val_long = 4; //com.orientechnologies.orient.core.metadata.schema.OType.LONG
   *    LinkSet val_link_set = 5; //com.orientechnologies.orient.core.metadata.schema.OType.LINKSET
   *    RID val_link = 6; //com.orientechnologies.orient.core.metadata.schema.OType.LINK
   *    Date val_date = 7; //com.orientechnologies.orient.core.metadata.schema.OType.DATE
   *    DateTime val_date_time = 8; //com.orientechnologies.orient.core.metadata.schema.OType.DATETIME
   *    reserved 9; // LinkBag
   *    Record val_record = 10; //com.orientechnologies.orient.core.metadata.schema.OType.EMBEDDED
   *    List val_embded_list = 11; //com.orientechnologies.orient.core.metadata.schema.OType.EMBEDDEDLIST
   *    LinkList val_link_list = 12; //com.orientechnologies.orient.core.metadata.schema.OType.LINKLIST
   *    Set val_embedded_set = 13; //com.orientechnologies.orient.core.metadata.schema.OType.EMBEDDEDSET
   *    Map val_embedded_map = 14; //com.orientechnologies.orient.core.metadata.schema.OType.EMBEDDEDMAP
   *    LinkMap val_link_map = 15; //com.orientechnologies.orient.core.metadata.schema.OType.LINKMAP
   *    bytes val_byte = 16; //com.orientechnologies.orient.core.metadata.schema.OType.BYTE
   *    bytes val_bytes = 17; //com.orientechnologies.orient.core.metadata.schema.OType.BINARY
   *    bytes val_custom = 18; //com.orientechnologies.orient.core.metadata.schema.OType.CUSTOM
   *    Decimal val_decimal = 19; //com.orientechnologies.orient.core.metadata.schema.OType.DECIMAL
   *    sint32 val_short = 20; //com.orientechnologies.orient.core.metadata.schema.OType.SHORT
   *    float val_float = 21; //com.orientechnologies.orient.core.metadata.schema.OType.FLOAT
   *    bool val_bool = 22; //com.orientechnologies.orient.core.metadata.schema.OType.BOOLEAN
   *  }  
   * reserved 9;
   * }
   * </code>
   */
  private static int[] computeItemSize(Object value, OType type) {
    int size = 0;
    switch (type) {
    case STRING:
      size += computeStringSize(value);
      break;
    case INTEGER:
      size += computeIntSize(value);
      break;
    default:
      throw new OSerializationException("Type " + type + " can not be serialized");
    }

    return new int[] { size };
  }

  private static void writeItem(Object value, OType type, CodedOutputStream codedOutputStream, Iterator<Integer> sizesIterator)
      throws IOException {
    switch (type) {
    case STRING:
      writeString(value, codedOutputStream);
      break;
    case INTEGER:
      writeInt(value, codedOutputStream);
      break;
    default:
      throw new OSerializationException("Type " + type + " can not be serialized");
    }
  }

  /**
   * string val_string = 3; //com.orientechnologies.orient.core.metadata.schema.OType.STRING
   */
  private static int computeStringSize(Object value) {
    return CodedOutputStream.computeStringSize(3, value.toString());
  }

  private static void writeString(Object value, CodedOutputStream codedOutputStream) throws IOException {
    codedOutputStream.writeString(3, value.toString());
  }

  /**
   * sint32 val_int = 1; //com.orientechnologies.orient.core.metadata.schema.OType.INTEGER
   */
  private static int computeIntSize(Object value) {
    return CodedOutputStream.computeSInt32Size(1, ((Number) value).intValue());
  }

  private static void writeInt(Object value, CodedOutputStream codedOutputStream) throws IOException {
    codedOutputStream.writeSInt32(1, ((Number) value).intValue());
  }

  private static int computeEmbeddedMessageHeaderSize(int fieldNumber, int messageSize) {
    int size = 0;
    size += CodedOutputStream.computeTagSize(fieldNumber);
    size += CodedOutputStream.computeRawVarint32Size(messageSize);

    return size;
  }

  private static void writeEmbeddedMessageHeader(int messageSize, int fieldNumber, CodedOutputStream codedOutputStream)
      throws IOException {
    codedOutputStream.writeTag(fieldNumber, 2);
    codedOutputStream.writeRawVarint32(messageSize);
  }

  private static OType getFieldType(final ODocumentEntry entry) {
    OType type = entry.type;
    if (type == null) {
      final OProperty prop = entry.property;
      if (prop != null)
        type = prop.getType();

    }
    if (type == null || OType.ANY == type)
      type = OType.getTypeByValue(entry.value);
    return type;
  }

  private static OType getLinkedType(ODocument document, OType type, String key) {
    if (type != OType.EMBEDDEDLIST && type != OType.EMBEDDEDSET && type != OType.EMBEDDEDMAP)
      return null;
    OClass immutableClass = ODocumentInternal.getImmutableSchemaClass(document);
    if (immutableClass != null) {
      OProperty prop = immutableClass.getProperty(key);
      if (prop != null) {
        return prop.getLinkedType();
      }
    }
    return null;
  }

  @Override
  public int serializeValue(BytesContainer bytes, Object value, OType type, OType linkedType) {
    return 0;
  }

  /**
   * message RID { <br/>
   * sint32 cluster_id = 1; <br/>
   * sint64 cluster_pos = 2; <br/>
   * } <br/>
   */
  private static ORecordId readRID(CodedInputStream codedInputStream) throws IOException {
    final int messageLength = codedInputStream.readRawVarint32();
    final int bytesRead = codedInputStream.getTotalBytesRead();

    int clusterId = 0;
    long clusterPosition = 0;

    while (messageLength > codedInputStream.getTotalBytesRead() - bytesRead) {
      final int tag = codedInputStream.readTag();
      switch (WireFormat.getTagFieldNumber(tag)) {
      case 1:
        clusterId = codedInputStream.readSInt32();
        break;
      case 2:
        clusterPosition = codedInputStream.readSInt64();
        break;
      default:
        codedInputStream.skipField(tag);
      }
    }

    return new ORecordId(clusterId, clusterPosition);
  }

  /**
   * <code>
   *  message Document {
   *    reserved 1;
   *    string class = 2;
   *    map<string, Item> fields = 3;
   *  }
   *  </code>
   */
  private void readDocument(ODocument document, BytesContainer bytesContainer, byte[][] fieldsToInclude, String[] fieldNames) {
    final int messageLength = readRawVarint32(bytesContainer);
    final int bytesRead = bytesContainer.offset;

    int fieldsRead = 0;
    while (messageLength > bytesContainer.offset - bytesRead) {
      if (fieldsToInclude != null && fieldsRead >= fieldsToInclude.length) {
        break;
      } else {
        final int tag = readRawVarint32(bytesContainer);
        switch (tag) {
        case 10: {
          if (readHashIndexPositionMap(bytesContainer, document, fieldsToInclude, fieldNames))
            return;
          break;
        }
        case 18: {
          final String className = readString(bytesContainer);
          if (className.length() > 0)
            ODocumentInternal.fillClassNameIfNeeded(document, className);
          break;
        }
        case 26: {
          final OTriple<String, OType, Object> fieldValue = readMapItem(bytesContainer, fieldsToInclude);
          if (fieldValue != null) {
            if (!ODocumentInternal.rawContainsField(document, fieldValue.key)) {
              ODocumentInternal.rawField(document, fieldValue.key, fieldValue.value.value, fieldValue.value.key);
            }
            fieldsRead++;
          }

          break;
        }
        default:
          skipField(tag, bytesContainer);
        }
      }
    }
  }

  private boolean readHashIndexPositionMap(BytesContainer bytesContainer, ODocument document, byte[][] fieldsToInclude,
      String[] fieldNames) {
    final int messageLength = readRawVarint32(bytesContainer);
    final int bytesRead = bytesContainer.offset;

    int fieldRead = 0;
    if (fieldsToInclude != null) {
      final int endOffset = bytesRead + messageLength;

      while (messageLength > bytesContainer.offset - bytesRead) {
        if (fieldRead >= fieldNames.length) {
          break;
        }

        final int hashCode = OIntegerSerializer.INSTANCE.deserializeNative(bytesContainer.bytes, bytesContainer.offset);
        bytesContainer.offset += OIntegerSerializer.INT_SIZE;

        boolean match = false;

        for (int i = 0; i < fieldNames.length; i++) {
          String fieldName = fieldNames[i];
          final int shc = fieldName.hashCode();
          if (shc == hashCode) {
            final int position = readRawVarint32(bytesContainer);
            match = true;
            final int offset = bytesContainer.offset;

            bytesContainer.offset = position + endOffset;
            final OTriple<String, OType, Object> fieldValue = readMapItem(bytesContainer, new byte[][] { fieldsToInclude[i] });
            bytesContainer.offset = offset;

            if (fieldValue != null) {
              ODocumentInternal.rawField(document, fieldValue.key, fieldValue.value.value, fieldValue.value.key);
              fieldRead++;
              break;
            }
          }
        }

        if (!match)
          skipRawVarint(bytesContainer);
      }

      bytesContainer.offset = bytesRead + messageLength;
      return true;
    }

    bytesContainer.offset = bytesRead + messageLength;
    return false;
  }

  private static long readSInt64(BytesContainer bytesContainer) {
    return decodeZigZag64(readRawVarint64(bytesContainer));
  }

  private static long decodeZigZag64(final long n) {
    return (n >>> 1) ^ -(n & 1);
  }

  private static long readRawVarint64(BytesContainer bytesContainer) {
    fastpath: {
      int pos = bytesContainer.offset;

      if (bytesContainer.bytes.length == pos) {
        break fastpath;
      }

      final byte[] buffer = bytesContainer.bytes;
      long x;
      int y;
      if ((y = buffer[pos++]) >= 0) {
        bytesContainer.offset = pos;
        return y;
      } else if (bytesContainer.bytes.length - pos < 9) {
        break fastpath;
      } else if ((y ^= (buffer[pos++] << 7)) < 0) {
        x = y ^ (~0 << 7);
      } else if ((y ^= (buffer[pos++] << 14)) >= 0) {
        x = y ^ ((~0 << 7) ^ (~0 << 14));
      } else if ((y ^= (buffer[pos++] << 21)) < 0) {
        x = y ^ ((~0 << 7) ^ (~0 << 14) ^ (~0 << 21));
      } else if ((x = ((long) y) ^ ((long) buffer[pos++] << 28)) >= 0L) {
        x ^= (~0L << 7) ^ (~0L << 14) ^ (~0L << 21) ^ (~0L << 28);
      } else if ((x ^= ((long) buffer[pos++] << 35)) < 0L) {
        x ^= (~0L << 7) ^ (~0L << 14) ^ (~0L << 21) ^ (~0L << 28) ^ (~0L << 35);
      } else if ((x ^= ((long) buffer[pos++] << 42)) >= 0L) {
        x ^= (~0L << 7) ^ (~0L << 14) ^ (~0L << 21) ^ (~0L << 28) ^ (~0L << 35) ^ (~0L << 42);
      } else if ((x ^= ((long) buffer[pos++] << 49)) < 0L) {
        x ^= (~0L << 7) ^ (~0L << 14) ^ (~0L << 21) ^ (~0L << 28) ^ (~0L << 35) ^ (~0L << 42) ^ (~0L << 49);
      } else {
        x ^= ((long) buffer[pos++] << 56);
        x ^= (~0L << 7) ^ (~0L << 14) ^ (~0L << 21) ^ (~0L << 28) ^ (~0L << 35) ^ (~0L << 42) ^ (~0L << 49) ^ (~0L << 56);
        if (x < 0L) {
          if (buffer[pos++] < 0L) {
            break fastpath;
          }
        }
      }

      bytesContainer.offset = pos;
      return x;
    }

    return readRawVarint64SlowPath(bytesContainer);
  }

  private static long readRawVarint64SlowPath(BytesContainer bytesContainer) {
    long result = 0;
    for (int shift = 0; shift < 64; shift += 7) {
      final byte b = bytesContainer.bytes[bytesContainer.offset++];
      result |= (long) (b & 0x7F) << shift;
      if ((b & 0x80) == 0) {
        return result;
      }
    }
    throw new OSerializationException("Invalid varint64 format");
  }

  /**
   * The map syntax is equivalent to the following on the wire, so protocol buffers implementations that do not support maps can
   * still handle your data:<br/>
   *
   * message MapFieldEntry {<br/>
   * key_type key = 1;<br/>
   * value_type value = 2;<br/>
   * }<br/>
   * repeated MapFieldEntry map_field = N;<br/>
   */
  private static OTriple<String, OType, Object> readMapItem(BytesContainer bytesContainer, byte[][] fieldsToInclude) {
    final int messageLength = readRawVarint32(bytesContainer);
    final int bytesRead = bytesContainer.offset;

    int fieldNameStart = -1;
    int fieldNameLen = -1;
    OPair<OType, Object> itemValue = null;

    itemLoop: while (messageLength > bytesContainer.offset - bytesRead) {
      final int tag = readRawVarint32(bytesContainer);
      switch (tag) {
      case 10: {
        final int len = readRawVarint32(bytesContainer);
        fieldNameStart = bytesContainer.offset;
        fieldNameLen = len;
        bytesContainer.offset += len;
        break;
      }
      case 18: {
        if (fieldsToInclude != null && fieldNameLen >= 0) {
          for (byte[] name : fieldsToInclude) {
            if (fieldNameLen != name.length) {
              skipField(tag, bytesContainer);
              continue itemLoop;
            }

            for (int i = 0; i < fieldNameLen; i++) {
              if (name[i] != bytesContainer.bytes[fieldNameStart + i]) {
                skipField(tag, bytesContainer);
                continue itemLoop;
              }
            }

            itemValue = readItemValue(bytesContainer);
          }
        } else {
          itemValue = readItemValue(bytesContainer);
        }
        break;
      }
      default:
        skipField(tag, bytesContainer);
      }
    }

    if (fieldNameLen >= 0 && itemValue != null) {
      return new OTriple<String, OType, Object>(new String(bytesContainer.bytes, fieldNameStart, fieldNameLen, UTF8).intern(), itemValue.key,
          itemValue.value);
    }

    return null;

  }

  /**
   * <code>
   * message Item {
   *  oneof value {
   *    sint32 val_int = 1; //com.orientechnologies.orient.core.metadata.schema.OType.INTEGER
   *    double val_double = 2; //com.orientechnologies.orient.core.metadata.schema.OType.DOUBLE
   *    string val_string = 3; //com.orientechnologies.orient.core.metadata.schema.OType.STRING
   *    sint64 val_long = 4; //com.orientechnologies.orient.core.metadata.schema.OType.LONG
   *    LinkSet val_link_set = 5; //com.orientechnologies.orient.core.metadata.schema.OType.LINKSET
   *    RID val_link = 6; //com.orientechnologies.orient.core.metadata.schema.OType.LINK
   *    Date val_date = 7; //com.orientechnologies.orient.core.metadata.schema.OType.DATE
   *    DateTime val_date_time = 8; //com.orientechnologies.orient.core.metadata.schema.OType.DATETIME
   *    reserved 9; // LinkBag
   *    Record val_record = 10; //com.orientechnologies.orient.core.metadata.schema.OType.EMBEDDED
   *    List val_embded_list = 11; //com.orientechnologies.orient.core.metadata.schema.OType.EMBEDDEDLIST
   *    LinkList val_link_list = 12; //com.orientechnologies.orient.core.metadata.schema.OType.LINKLIST
   *    Set val_embedded_set = 13; //com.orientechnologies.orient.core.metadata.schema.OType.EMBEDDEDSET
   *    Map val_embedded_map = 14; //com.orientechnologies.orient.core.metadata.schema.OType.EMBEDDEDMAP
   *    LinkMap val_link_map = 15; //com.orientechnologies.orient.core.metadata.schema.OType.LINKMAP
   *    bytes val_byte = 16; //com.orientechnologies.orient.core.metadata.schema.OType.BYTE
   *    bytes val_bytes = 17; //com.orientechnologies.orient.core.metadata.schema.OType.BINARY
   *    bytes val_custom = 18; //com.orientechnologies.orient.core.metadata.schema.OType.CUSTOM
   *    Decimal val_decimal = 19; //com.orientechnologies.orient.core.metadata.schema.OType.DECIMAL
   *    sint32 val_short = 20; //com.orientechnologies.orient.core.metadata.schema.OType.SHORT
   *    float val_float = 21; //com.orientechnologies.orient.core.metadata.schema.OType.FLOAT
   *    bool val_bool = 22; //com.orientechnologies.orient.core.metadata.schema.OType.BOOLEAN
   *  }
   * reserved 9;
   * }
   * </code>
   */
  private static OPair<OType, Object> readItemValue(BytesContainer bytesContainer) {
    final int messageLength = readRawVarint32(bytesContainer);
    final int bytesRead = bytesContainer.offset;

    OPair<OType, Object> result = null;

    while (messageLength > bytesContainer.offset - bytesRead) {
      final int tag = readRawVarint32(bytesContainer);
      switch (tag) {
      case 8: {
        result = new OPair<OType, Object>(OType.INTEGER, readSInt32(bytesContainer));
        break;
      }
      case 26: {
        result = new OPair<OType, Object>(OType.STRING, readString(bytesContainer));
        break;
      }
      default:
        skipField(tag, bytesContainer);
      }
    }

    return result;
  }

  private static int readSInt32(BytesContainer bytesContainer) {
    return decodeZigZag32(readRawVarint32(bytesContainer));
  }

  private static int decodeZigZag32(int n) {
    return n >>> 1 ^ -(n & 1);
  }

  /**
   * <code>
   *  message Record {
   *    oneof data {
   *      bytes raw = 1;
   *      Document document = 2;
   *    }
   *
   *    RID rid = 3;
   *    int64 version = 4;
   *  }
   * </code>
   */
  @Override
  public void deserializePartial(ODocument document, BytesContainer bytes, String[] iFields) {
    byte[][] fieldNames = new byte[iFields.length][];
    for (int i = 0; i < iFields.length; i++) {
      fieldNames[i] = iFields[i].getBytes(UTF8);
    }

    while (bytes.offset <= bytes.bytes.length) {
      int tag = readRawVarint32(bytes);
      if (tag == 18) {
        readDocument(document, bytes, fieldNames, iFields);
        break;
      } else {
        skipField(tag, bytes);
      }
    }
  }

  private static int readRawVarint32(BytesContainer bytesContainer) {
    fastpath: {
      int pos = bytesContainer.offset;

      if (bytesContainer.bytes.length == pos) {
        break fastpath;
      }

      final byte[] buffer = bytesContainer.bytes;
      int x;
      if ((x = buffer[pos++]) >= 0) {
        bytesContainer.offset = pos;
        return x;
      } else if (bytesContainer.bytes.length - pos < 9) {
        break fastpath;
      } else if ((x ^= (buffer[pos++] << 7)) < 0) {
        x ^= (~0 << 7);
      } else if ((x ^= (buffer[pos++] << 14)) >= 0) {
        x ^= (~0 << 7) ^ (~0 << 14);
      } else if ((x ^= (buffer[pos++] << 21)) < 0) {
        x ^= (~0 << 7) ^ (~0 << 14) ^ (~0 << 21);
      } else {
        int y = buffer[pos++];
        x ^= y << 28;
        x ^= (~0 << 7) ^ (~0 << 14) ^ (~0 << 21) ^ (~0 << 28);
        if (y < 0 && buffer[pos++] < 0 && buffer[pos++] < 0 && buffer[pos++] < 0 && buffer[pos++] < 0 && buffer[pos++] < 0) {
          break fastpath;
        }
      }
      bytesContainer.offset = pos;
      return x;
    }

    return (int) readRawVarint64SlowPath(bytesContainer);
  }

  private static boolean skipField(int tag, BytesContainer bytesContainer) {
    switch (getTagWireType(tag)) {
    case 0:
      skipRawVarint(bytesContainer);
      return true;
    case 1:
      bytesContainer.offset += 8;
      return true;
    case 2:
      final int size = readRawVarint32(bytesContainer);
      bytesContainer.offset += size;
      return true;
    case 3:
      // we do not use it
      return true;
    case 4:
      return false;
    case 5:
      bytesContainer.offset += 4;
      return true;
    default:
      throw new OSerializationException("Invalid format");
    }
  }

  private static int getTagWireType(int tag) {
    return tag & 7;
  }

  private static String readString(BytesContainer bytesContainer) {
    int size = readRawVarint32(bytesContainer);
    String result = new String(bytesContainer.bytes, bytesContainer.offset, size, UTF8);
    bytesContainer.offset += size;
    return result;
  }

  private static void skipRawVarint(BytesContainer bytesContainer) {
    if (bytesContainer.bytes.length - bytesContainer.offset >= 10) {
      final byte[] buffer = bytesContainer.bytes;
      int pos = bytesContainer.offset;
      for (int i = 0; i < 10; i++) {
        if (buffer[pos++] >= 0) {
          bytesContainer.offset = pos;
          return;
        }
      }
    }
    skipRawVarintSlowPath(bytesContainer);
  }

  private static void skipRawVarintSlowPath(BytesContainer bytesContainer) {
    for (int i = 0; i < 10; i++) {
      if (bytesContainer.bytes[bytesContainer.offset++] >= 0) {
        return;
      }
    }
    throw new OSerializationException("Malformed varint");
  }

  @Override
  public Object deserializeValue(BytesContainer bytes, OType type, ODocument ownerDocument) {
    return null;
  }

  @Override
  public OBinaryField deserializeField(BytesContainer bytes, OClass iClass, String iFieldName) {

    return null;
  }

  @Override
  public OBinaryComparator getComparator() {
    return null;
  }

  @Override
  public String[] getFieldNames(BytesContainer iBytes) {
    return new String[0];
  }
}
