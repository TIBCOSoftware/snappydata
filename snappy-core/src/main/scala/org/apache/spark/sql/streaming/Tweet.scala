package org.apache.spark.sql.streaming

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{Input, Output}

/**
 * Created by ymahajan on 5/11/15.
 */
case class Tweet(id: Long,
            text: String)

object Tweet {

  def write(kryo: Kryo, output: Output, obj: Tweet) {
    output.writeLong(obj.id)
    output.writeString(obj.text)
  }

  def read(kryo: Kryo, input: Input): Tweet = {
    //val classTag = kryo.readObject[ClassTag[Any]](input, classOf[ClassTag[Any]])
    val id = input.readLong
    val text = input.readString
    new Tweet(id, text) //(classTag)
  }
}
