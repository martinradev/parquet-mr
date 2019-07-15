/* 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.parquet.column.values.bytestreamsplit;

import org.apache.parquet.io.ParquetDecodingException;

import java.io.IOException;

public class ByteStreamSplitValuesReaderForFloat extends ByteStreamSplitValuesReader {

  private static final int FLOAT_SIZE_IN_BYTES = 4;

  public ByteStreamSplitValuesReaderForFloat() {
    super(ByteStreamSplitValuesReaderForFloat.FLOAT_SIZE_IN_BYTES);
  }

  @Override
  public float readFloat() {
    byte[] gatheredBytes = new byte[ByteStreamSplitValuesReaderForFloat.FLOAT_SIZE_IN_BYTES];
    try {
      gatherElementDataFromStreams(gatheredBytes);
      int b1 = gatheredBytes[0] & 0xFF;
      int b2 = gatheredBytes[1] & 0xFF;
      int b3 = gatheredBytes[2] & 0xFF;
      int b4 = gatheredBytes[3] & 0xFF;
      int value = b1 | (b2 << 8) | (b3 << 16) | (b4 << 24);
      return Float.intBitsToFloat(value);
    } catch (IOException | ArrayIndexOutOfBoundsException ex) {
      throw new ParquetDecodingException("Could not read float.");
    }
  }
}
