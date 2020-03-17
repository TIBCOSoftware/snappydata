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
package io.snappydata.impl;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectStreamClass;
import java.util.HashMap;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import com.esotericsoftware.kryo.util.ObjectMap;
import org.apache.spark.util.Utils;

/**
 * Fixes ObjectInputStreamWithKryoClassLoader.resolveClass to handle primitive classes.
 */
public class KryoJavaSerializer extends JavaSerializer {

  public Object read(Kryo kryo, Input input, Class type) {
    try {
      @SuppressWarnings("unchecked")
      ObjectMap<Object, Object> graphContext = kryo.getGraphContext();
      ObjectInputStream objectStream = (ObjectInputStream)graphContext.get(this);
      if (objectStream == null) {
        objectStream = new ObjectInputStreamWithKryoClassLoader(input, kryo);
        graphContext.put(this, objectStream);
      }
      return objectStream.readObject();
    } catch (Exception ex) {
      throw new KryoException("Error during Java deserialization.", ex);
    }
  }

  /**
   * Taken from Kryo's JavaSerializer.ObjectInputStreamWithKryoClassLoader.
   * This falls back to super.resolveClass in case of error so as to load
   * primitive classes among others.
   */
  private static class ObjectInputStreamWithKryoClassLoader extends ObjectInputStream {

    private static final HashMap<String, Class<?>> primClasses;

    static {
      primClasses = new HashMap<>(8, 1.0F);
      primClasses.put("boolean", boolean.class);
      primClasses.put("byte", byte.class);
      primClasses.put("char", char.class);
      primClasses.put("short", short.class);
      primClasses.put("int", int.class);
      primClasses.put("long", long.class);
      primClasses.put("float", float.class);
      primClasses.put("double", double.class);
      primClasses.put("void", void.class);
    }

    private final ClassLoader loader;

    ObjectInputStreamWithKryoClassLoader(InputStream in, Kryo kryo) throws IOException {
      super(in);
      this.loader = kryo.getClassLoader();
    }

    @Override
    protected Class<?> resolveClass(ObjectStreamClass desc) {
      String name = desc.getName();
      try {
        return Class.forName(name, false, loader);
      } catch (ClassNotFoundException e) {
        Class<?> cl = primClasses.get(name);
        if (cl != null) {
          return cl;
        } else {
          try {
            // try Spark default way of loading classes
            return Class.forName(name, false, Utils.getContextOrSparkClassLoader());
          } catch (ClassNotFoundException cnfe) {
            throw new RuntimeException("Class not found: " + name, cnfe);
          }
        }
      }
    }
  }
}
