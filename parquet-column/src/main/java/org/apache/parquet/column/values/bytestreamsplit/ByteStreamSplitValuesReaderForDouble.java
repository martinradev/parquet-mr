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

public class ByteStreamSplitValuesReaderForDouble extends ByteStreamSplitValuesReader {

  private static final int DOUBLE_SIZE_IN_BYTES = 8;

  public ByteStreamSplitValuesReaderForDouble() {
    super(ByteStreamSplitValuesReaderForDouble.DOUBLE_SIZE_IN_BYTES);
  }

  @Override
  public double readDouble() {
    byte[] gatheredBytes = new byte[ByteStreamSplitValuesReaderForDouble.DOUBLE_SIZE_IN_BYTES];
    try {
      gatherElementDataFromStreams(gatheredBytes);
      long value = 0;
      for (int i = 0; i < ByteStreamSplitValuesReaderForDouble.DOUBLE_SIZE_IN_BYTES; ++i) {
        long b = gatheredBytes[i] & 0xFF;
        value |= (b << (i * 8));
      }
      return Double.longBitsToDouble(value);
    } catch (IOException | ArrayIndexOutOfBoundsException ex) {
      throw new ParquetDecodingException("Could not read double.");
    }
  }
}
