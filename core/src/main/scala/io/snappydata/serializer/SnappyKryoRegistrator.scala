package io.snappydata.serializer

import com.esotericsoftware.kryo.Kryo
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember

import org.apache.spark.serializer.KryoRegistrator

/**
 * This class is used as a kryo registrator for registering snappy related classes.
 * All the classes that needs to be registered along with their serializer should be added in this class
 */
class SnappyKryoRegistrator extends KryoRegistrator{

  override def registerClasses(kryo: Kryo): Unit = {
    kryo.register(classOf[InternalDistributedMember], new com.esotericsoftware.kryo.serializers.JavaSerializer())
  }

}
