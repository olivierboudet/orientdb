/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.orientechnologies.common.serialization.types;

import com.orientechnologies.common.directmemory.ODirectMemoryPointer;
import com.orientechnologies.common.directmemory.ODirectMemoryPointerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * @author Ilya Bershadskiy (ibersh20-at-gmail.com)
 * @since 20.01.12
 */
@Test
public class BinarySerializerTest {
  private int                   FIELD_SIZE;
  private byte[]                OBJECT;
  private OBinaryTypeSerializer binarySerializer;
  byte[] stream;

  @BeforeClass
  public void beforeClass() {
    binarySerializer = new OBinaryTypeSerializer();
    OBJECT = new byte[] { 1, 2, 3, 4, 5, 6 };
    FIELD_SIZE = OBJECT.length + OIntegerSerializer.INT_SIZE;
    stream = new byte[FIELD_SIZE];
  }

  public void testFieldSize() {
    Assert.assertEquals(binarySerializer.getObjectSize(OBJECT), FIELD_SIZE);
  }

  public void testSerialize() {
    binarySerializer.serialize(OBJECT, stream, 0);
    Assert.assertEquals(binarySerializer.deserialize(stream, 0), OBJECT);
  }

  public void testSerializeNative() {
    binarySerializer.serializeNativeObject(OBJECT, stream, 0);
    Assert.assertEquals(binarySerializer.deserializeNativeObject(stream, 0), OBJECT);

  }

  public void testNativeDirectMemoryCompatibility() {
    binarySerializer.serializeNativeObject(OBJECT, stream, 0);

    ByteBuffer byteBuffer = ByteBuffer.allocateDirect(stream.length).order(ByteOrder.nativeOrder());
    byteBuffer.put(stream);

    Assert.assertEquals(binarySerializer.deserializeFromByteBufferObject(byteBuffer, 0), OBJECT);
  }
}
