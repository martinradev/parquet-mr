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

import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.io.ParquetDecodingException;

import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract public class ByteStreamSplitValuesReader extends ValuesReader {

  private static final Logger LOG = LoggerFactory.getLogger(ByteStreamSplitValuesReader.class);
  private final int numStreams;
  private final int elementSizeInBytes;
  private final byte[][] byteStreams;
  private int indexInStream;

  protected ByteStreamSplitValuesReader(int elementSizeInBytes) {
    this.numStreams = elementSizeInBytes;
    this.elementSizeInBytes = elementSizeInBytes;
    this.byteStreams = new byte[numStreams][];
    this.indexInStream = 0;
  }

  protected void gatherElementDataFromStreams(byte[] gatheredData)
          throws IOException, ArrayIndexOutOfBoundsException {
    if (gatheredData.length != numStreams) {
      throw new IOException("gatherData buffer is not of the expected size.");
    }
    if (this.indexInStream >= byteStreams[0].length) {
      throw new ArrayIndexOutOfBoundsException("Byte-stream data was already exhausted.");
    }
    for (int i = 0; i < numStreams; ++i) {
      gatheredData[i] = this.byteStreams[i][this.indexInStream];
    }
    ++this.indexInStream;
  }

  @Override
  public void initFromPage(int valueCount, ByteBufferInputStream stream) throws IOException {
    LOG.debug("init from page at offset {} for length {}", stream.position(), stream.available());

    if (valueCount * this.elementSizeInBytes > stream.available()) {
      throw new IOException("Stream does not contain enough data for the given number of values.");
    }

    /* Allocate buffer for each byte stream */
    for (int i = 0; i < this.numStreams; ++i) {
      this.byteStreams[i] = new byte[valueCount];
    }

    /* Eagerly read the data for each stream */
    for (int i = 0; i < this.numStreams; ++i) {
      final int numRead = stream.read(this.byteStreams[i], 0, valueCount);
      if (numRead != valueCount) {
        throw new IOException("Failed to read requested number of bytes into one of the byte streams.");
      }
    }

    this.indexInStream = 0;
  }

  @Override
  public void skip() {
    skip(1);
  }

  @Override
  public void skip(int n) {
    if (this.indexInStream + n > this.byteStreams[0].length || n < 0) {
      throw new ParquetDecodingException("Cannot skip this many elements.");
    }
    this.indexInStream += n;
  }

}
