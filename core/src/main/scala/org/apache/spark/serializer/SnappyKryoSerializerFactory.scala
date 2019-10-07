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
package org.apache.spark.serializer

import java.io.{ObjectInputStream, ObjectOutputStream, Serializable => JavaSerializable}

import com.esotericsoftware.kryo.factories.SerializerFactory
import com.esotericsoftware.kryo.serializers.{FieldSerializer => KryoFieldSerializer, JavaSerializer => KryoJavaSerializer}
import com.esotericsoftware.kryo.{Kryo, Serializer => KryoClassSerializer}
import com.gemstone.gemfire.internal.shared.ClientSharedUtils

/**
 * This serializer factory will instantiate new serializers of a given class via reflection. If
 * the class implements Serializable and has either of
 * writeReplace/readResolve/readObject/writeObject then it will set JavaSerializer for that class
 * else FieldSerailizer will be used
 */
class SnappyKryoSerializerFactory extends SerializerFactory {

  override def makeSerializer(kryo: Kryo, clazz: Class[_]): KryoClassSerializer[_] = {
    if (isJavaSerializerRequired(clazz)) return new KryoJavaSerializer()
    val fieldSerializer: KryoFieldSerializer[Nothing] = new KryoFieldSerializer(kryo, clazz)
    fieldSerializer
  }

  private def isJavaSerializerRequired(clazz: Class[_]): Boolean = {

    if (classOf[JavaSerializable].isAssignableFrom(clazz)) {
      hasInheritableReadWriteMethods(clazz, "writeReplace", classOf[AnyRef]) ||
          hasInheritableReadWriteMethods(clazz, "readResolve", classOf[AnyRef]) ||
          hasInheritableReadWriteMethods(clazz, "readObject",
            Void.TYPE, classOf[ObjectInputStream]) ||
          hasInheritableReadWriteMethods(clazz, "writeObject",
            Void.TYPE, classOf[ObjectOutputStream])
    } else {
      false
    }
  }

  private def hasInheritableReadWriteMethods(clazz: Class[_], methodName: String,
      returnType: Class[_], parameterTypes: Class[_]*): Boolean = {
    try {
      ClientSharedUtils.getAnyMethod(clazz, methodName, returnType,
        parameterTypes: _*) != null
    } catch {
      case _: NoSuchMethodException => false;
    }
  }
}
