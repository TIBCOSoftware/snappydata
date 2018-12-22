/*
 * Copyright (c) 2018 SnappyData, Inc. All rights reserved.
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
package org.apache.spark.sql.streaming

import scala.reflect._
import scala.util.{Failure, Success, Try}

import com.rabbitmq.client.QueueingConsumer.Delivery
import com.rabbitmq.client.{Address, Channel, Connection, ConnectionFactory, QueueingConsumer}

import org.apache.spark.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.SnappyStreamingContext
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.receiver.Receiver

object RabbitMQUtils {
  def createStream[T: ClassTag, D: ClassTag](snsc: SnappyStreamingContext,
      options: Map[String, String]): ReceiverInputDStream[T] = {
    new RabbitMQInputDStream[T, D](snsc, options)
  }
}

trait RabbitMQDecoder[T] extends scala.AnyRef {
  def fromBytes(bytes: scala.Array[scala.Byte]): T
}

final class RabbitMQStringDecoder extends RabbitMQDecoder[String] {
  def fromBytes(bytes: scala.Array[scala.Byte]): String = {
    new Predef.String(bytes)
  }
}

final class RabbitMQInputDStream[T: ClassTag, D: ClassTag](
    _snsc: SnappyStreamingContext,
    options: Map[String, String])
    extends ReceiverInputDStream[T](_snsc) {

  override def getReceiver(): Receiver[T] = {
    new RabbitMQReceiver[T, D](options)
  }
}

final class RabbitMQReceiver[T: ClassTag, D: ClassTag](options: Map[String, String])
    extends Receiver[T](StorageLevel.MEMORY_AND_DISK_SER_2) with Logging {

  override def onStart() {
    // implicit val akkaSystem = akka.actor.ActorSystem()
    getConnectionAndChannel match {
      case Success((connection: Connection, channel: Channel)) =>
        new Thread() {
          override def run() {
            receive(connection, channel)
          }
        }.start()
      case Failure(f) =>
        restart("Failed to connect", f)
    }
  }

  private def getConnectionAndChannel: Try[(Connection, Channel)] = {
    for {
      connection: Connection <- Try(
        new ConnectionFactory()
            .newConnection(Address.parseAddresses("localhost")))
      channel: Channel <- Try(connection.createChannel)
    } yield {
      (connection, channel)
    }
  }

  private def receive(connection: Connection,
      channel: Channel): Unit = {
    val queueName: String = options("queuename")
    val consumer: QueueingConsumer = new QueueingConsumer(channel)
    channel.basicConsume(queueName, false, consumer)
    while (!isStopped()) {
      val delivery: Delivery = consumer.nextDelivery()
      val decoder: RabbitMQDecoder[T] = classTag[D].runtimeClass.getConstructor()
          .newInstance().asInstanceOf[RabbitMQDecoder[T]]
      store(decoder.fromBytes(delivery.getBody))
      channel.basicAck(delivery.getEnvelope.getDeliveryTag, false)
    }
  }

  override def onStop() {
  }
}
