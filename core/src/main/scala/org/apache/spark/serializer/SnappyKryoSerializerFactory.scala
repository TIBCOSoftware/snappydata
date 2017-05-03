/*
 * Copyright (c) 2017 SnappyData, Inc. All rights reserved.
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

import java.io.{Serializable => JavaSerializable}
import java.lang.reflect.Method

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.factories.SerializerFactory
import com.esotericsoftware.kryo.serializers.{JavaSerializer => KryoJavaSerializer, FieldSerializer =>
KryoFieldSerializer}
import com.esotericsoftware.kryo.{Serializer => KryoClassSerializer}
import org.apache.spark.Logging

/**
 * This serializer factory will instantiates new serializers of a given class via reflection. If the class implements
 * Serializable and has either of writeReplace/readResolve/readObject/writeObject then it will set JavaSerializer for
 * that class else FieldSerailizer will be used
 *
 */
class SnappyKryoSerializerFactory extends SerializerFactory {

  override def makeSerializer(kryo: Kryo, `type`: Class[_]): KryoClassSerializer[_] = {
    if(isJavaSerializerRequired(`type`)) return new KryoJavaSerializer()
    val fieldSerializer: KryoFieldSerializer[Nothing] = new KryoFieldSerializer(kryo, `type`)
    // TODO: Set serialize transient to true when new version of kryo will be used
    return fieldSerializer;
  }

  private def isJavaSerializerRequired(clazz: Class[_]): Boolean = {
    //if (`type`.isAssignableFrom(JavaSerializable)) {

    if (classOf[JavaSerializable].isAssignableFrom(clazz)) {
      return (hasInheritableReadWriteMethods(clazz, "writeReplace") || hasInheritableReadWriteMethods(clazz, "readResolve")
        || hasInheritableReadWriteMethods(clazz, "readObject") || hasInheritableReadWriteMethods(clazz, "writeObject"))
    } else {
      return false
    }

  }

  private def hasInheritableReadWriteMethods(`type`: Class[_], methodName: String): Boolean = {
    var method: Method = null
    var current: Class[_] = `type`

      while (current != null && method == null) {
        try {
          method = current.getDeclaredMethod(methodName)
        }
        catch {
          case ex: NoSuchMethodException => {
            current = current.getSuperclass
          }
        }
      }
    return ((method != null) && (method.getReturnType eq classOf[AnyRef]))
  }
}
