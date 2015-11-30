package org.apache.spark.sql.streaming

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, Serializer}

/**
 * Created by ymahajan on 5/11/15.
 */
class TweetKryoSerializer extends Serializer[Tweet] {

  override def write(kryo: Kryo, output: Output, obj: Tweet) {
    Tweet.write(kryo, output, obj)
  }

  override def read(kryo: Kryo, input: Input, typee: Class[Tweet]): Tweet = {
    Tweet.read(kryo, input)
  }
}