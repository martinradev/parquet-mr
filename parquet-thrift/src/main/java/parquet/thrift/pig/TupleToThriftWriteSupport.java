/**
 * Copyright 2012 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package parquet.thrift.pig;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.pig.data.Tuple;
import org.apache.thrift.TBase;

import parquet.hadoop.BadConfigurationException;
import parquet.hadoop.api.WriteSupport;
import parquet.hadoop.thrift.ThriftWriteSupport;
import parquet.io.api.RecordConsumer;

import com.twitter.elephantbird.pig.util.PigToThrift;
import com.twitter.elephantbird.thrift.TStructDescriptor;
import com.twitter.elephantbird.thrift.TStructDescriptor.Field;

public class TupleToThriftWriteSupport extends WriteSupport<Tuple> {

  private final String className;
  private ThriftWriteSupport<TBase<?,?>> thriftWriteSupport;
  private PigToThrift<TBase<?,?>> pigToThrift;

  /**
   * @param className the thrift class name
   */
  public TupleToThriftWriteSupport(String className) {
    super();
    this.className = className;
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  @Override
  public WriteContext init(Configuration configuration) {
    try {
      Class<?> clazz = configuration.getClassByName(className).asSubclass(TBase.class);
      thriftWriteSupport = new ThriftWriteSupport(clazz);
      pigToThrift = new PigToThrift(clazz);
      List<Field> fields = TStructDescriptor.getInstance((Class<TBase<?,?>>)clazz).getFields();
      for (Field field : fields) {
        System.out.println(field.getName());
      }
      return thriftWriteSupport.init(configuration);
    } catch (ClassNotFoundException e) {
      throw new BadConfigurationException("The thrift class name was not found: " + className, e);
    } catch (ClassCastException e) {
      throw new BadConfigurationException("The thrift class name should extend TBase: " + className, e);
    }
  }

  @Override
  public void prepareForWrite(RecordConsumer recordConsumer) {
    thriftWriteSupport.prepareForWrite(recordConsumer);
  }

  public void write(Tuple t) {
    thriftWriteSupport.write(pigToThrift.getThriftObject(t));
  }

}
