/*
  *
  *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
  *  *
  *  *  Licensed under the Apache License, Version 2.0 (the "License");
  *  *  you may not use this file except in compliance with the License.
  *  *  You may obtain a copy of the License at
  *  *
  *  *       http://www.apache.org/licenses/LICENSE-2.0
  *  *
  *  *  Unless required by applicable law or agreed to in writing, software
  *  *  distributed under the License is distributed on an "AS IS" BASIS,
  *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  *  *  See the License for the specific language governing permissions and
  *  *  limitations under the License.
  *  *
  *  * For more information: http://www.orientechnologies.com
  *
  */

package com.orientechnologies.orient.core.serialization.serializer.record.binary;

import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.BytesContainer;

import java.io.IOException;

public class OVarIntSerializer {

  public static int write(BytesContainer bytes, long value) {
    value = signedToUnsigned(value);
    int pos = bytes.offset;
    writeUnsignedVarLong(value, bytes);
    return pos;

  }

  public static short readAsShort(final BytesContainer bytes) {
    return (short) readSignedVarLong(bytes);
  }

  public static long readAsLong(final BytesContainer bytes) {
    return readSignedVarLong(bytes);
  }

  public static int readAsInteger(final BytesContainer bytes) {
    return (int) readSignedVarLong(bytes);
  }

  public static byte readAsByte(final BytesContainer bytes) {
    return (byte) readSignedVarLong(bytes);
  }

  /**
   * Encodes a value using the variable-length encoding from
   * <a href="http://code.google.com/apis/protocolbuffers/docs/encoding.html"> Google Protocol Buffers</a>. It uses zig-zag encoding
   * to efficiently encode signed values. If values are known to be nonnegative, {@link #writeUnsignedVarLong(long, DataOutput)}
   * should be used.
   * 
   * @param value
   *          value to encode
   * @param out
   *          to write bytes to
   * @throws IOException
   *           if {@link DataOutput} throws {@link IOException}
   */
  private static long signedToUnsigned(long value) {
    return (value << 1) ^ (value >> 63);
  }

  /**
   * Encodes a value using the variable-length encoding from
   * <a href="http://code.google.com/apis/protocolbuffers/docs/encoding.html"> Google Protocol Buffers</a>. Zig-zag is not used, so
   * input must not be negative. If values can be negative, use {@link #writeSignedVarLong(long, DataOutput)} instead. This method
   * treats negative input as like a large unsigned value.
   * 
   * @param value
   *          value to encode
   * @param out
   *          to write bytes to
   * @return the number of bytes written
   * @throws IOException
   *           if {@link DataOutput} throws {@link IOException}
   */
  public static void writeUnsignedVarLong(long value, final BytesContainer bos) {
    int pos;
    while ((value & 0xFFFFFFFFFFFFFF80L) != 0L) {
      // out.writeByte(((int) value & 0x7F) | 0x80);
      pos = bos.alloc((short) 1);
      bos.bytes[pos] = (byte) (value & 0x7F | 0x80);
      value >>>= 7;
    }
    // out.writeByte((int) value & 0x7F);
    pos = bos.alloc((short) 1);
    bos.bytes[pos] = (byte) (value & 0x7F);
  }

  /**
   * @param in
   *          to read bytes from
   * @return decode value
   * @throws IOException
   *           if {@link DataInput} throws {@link IOException}
   * @throws IllegalArgumentException
   *           if variable-length value does not terminate after 9 bytes have been read
   * @see #writeSignedVarLong(long, DataOutput)
   */
  public static long readSignedVarLong(final BytesContainer bytes) {
    final long raw = readUnsignedVarLong(bytes);
    // This undoes the trick in writeSignedVarLong()
    final long temp = (((raw << 63) >> 63) ^ raw) >> 1;
    // This extra step lets us deal with the largest signed values by
    // treating
    // negative results from read unsigned methods as like unsigned values
    // Must re-flip the top bit if the original read value had it set.
    return temp ^ (raw & (1L << 63));
  }

  /**
   * @param in
   *          to read bytes from
   * @return decode value
   * @throws IOException
   *           if {@link DataInput} throws {@link IOException}
   * @throws IllegalArgumentException
   *           if variable-length value does not terminate after 9 bytes have been read
   * @see #writeUnsignedVarLong(long, DataOutput)
   */
  public static long readUnsignedVarLong(final BytesContainer bytes) {
    long value = 0L;
    int i = 0;
    long b;
    while (((b = bytes.bytes[bytes.offset++]) & 0x80L) != 0) {
      value |= (b & 0x7F) << i;
      i += 7;
      if (i > 63)
        throw new IllegalArgumentException("Variable length quantity is too long (must be <= 63)");
    }
    return value | (b << i);
  }

  public static int computeVarInt64Size(final long value) {
    if ((value & (0xffffffffffffffffL << 7)) == 0)
      return 1;
    if ((value & (0xffffffffffffffffL << 14)) == 0)
      return 2;
    if ((value & (0xffffffffffffffffL << 21)) == 0)
      return 3;
    if ((value & (0xffffffffffffffffL << 28)) == 0)
      return 4;
    if ((value & (0xffffffffffffffffL << 35)) == 0)
      return 5;
    if ((value & (0xffffffffffffffffL << 42)) == 0)
      return 6;
    if ((value & (0xffffffffffffffffL << 49)) == 0)
      return 7;
    if ((value & (0xffffffffffffffffL << 56)) == 0)
      return 8;
    if ((value & (0xffffffffffffffffL << 63)) == 0)
      return 9;
    return 10;
  }

  public static int computeVarInt32Size(int value) {
    if ((value & (0xffffffff << 7)) == 0)
      return 1;
    if ((value & (0xffffffff << 14)) == 0)
      return 2;
    if ((value & (0xffffffff << 21)) == 0)
      return 3;
    if ((value & (0xffffffff << 28)) == 0)
      return 4;

    return 5;
  }

  public static void writeVarInt32(int value, BytesContainer bytesContainer) {
    while (true) {
      if ((value & ~0x7F) == 0) {
        bytesContainer.bytes[bytesContainer.offset++] = (byte) value;
        return;
      } else {
        bytesContainer.bytes[bytesContainer.offset++] = (byte) ((value & 0x7F) | 0x80);
        value >>>= 7;
      }
    }
  }

  public static int readVarInt32(BytesContainer bytesContainer) {
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

    return (int) readVarInt64SlowPath(bytesContainer);
  }

  private static long readVarInt64SlowPath(BytesContainer bytesContainer) {
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

  public static void writeVarInt64(long value, BytesContainer bytesContainer) {
    while (true) {
      if ((value & ~0x7FL) == 0) {
        bytesContainer.bytes[bytesContainer.offset++] = (byte) value;
        return;
      } else {
        bytesContainer.bytes[bytesContainer.offset++] = (byte) ((value & 0x7F) | 0x80);
        value >>>= 7;
      }
    }
  }

  public static long readVarInt64(BytesContainer bytesContainer) {
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

    return readVarInt64SlowPath(bytesContainer);
  }

  public static void skipRawVarInt(BytesContainer bytesContainer) {
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

}
