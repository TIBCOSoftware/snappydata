/*
 * Copyright (c) 2017-2019 TIBCO Software Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */
package org.apache.spark.sql.execution

import scala.reflect.ClassTag

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, KryoSerializable}

import org.apache.spark.rdd.{RDD, RDDCheckpointData}
import org.apache.spark.sql.types.TypeUtilities
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{Dependency, SparkContext}

/** base RDD KryoSerializable class that will serialize minimal RDD fields */
abstract class RDDKryo[T: ClassTag](_sc: SparkContext, _deps: Seq[Dependency[_]])
    extends RDD[T](_sc, _deps) with KryoSerializable {

  override def write(kryo: Kryo, output: Output): Unit = {
    output.writeInt(id)
    val storageLevel = getStorageLevel
    if (storageLevel eq StorageLevel.NONE) {
      output.writeByte(0)
      output.writeByte(0)
    } else {
      output.writeByte(storageLevel.toInt)
      output.writeByte(storageLevel.replication)
    }
    checkpointData match {
      case None => output.writeBoolean(false)
      case Some(data) => output.writeBoolean(true)
        kryo.writeClassAndObject(output, data)
    }
  }

  override def read(kryo: Kryo, input: Input): Unit = {
    TypeUtilities.rddIdField.set(this, input.readInt())
    val flags = input.readByte()
    val replication = input.readByte()
    if (flags == 0 && replication == 0) {
      TypeUtilities.rddStorageLevelField.set(this, StorageLevel.NONE)
    } else {
      TypeUtilities.rddStorageLevelField.set(this, StorageLevel(flags, replication))
    }
    if (input.readBoolean()) {
      checkpointData = Some(kryo.readClassAndObject(input)
          .asInstanceOf[RDDCheckpointData[T]])
    } else {
      checkpointData = None
    }
  }
}
