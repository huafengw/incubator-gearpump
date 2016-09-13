/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.gearpump.streaming.examples.hdfs

import akka.actor.ActorSystem
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.cluster.client.ClientContext
import org.apache.gearpump.cluster.main.{ParseResult, CLIOption, ArgumentsParser}
import org.apache.gearpump.partitioner.HashPartitioner
import org.apache.gearpump.streaming.hadoop.SequenceFileSink
import org.apache.gearpump.streaming.sink.DataSinkProcessor
import org.apache.gearpump.streaming.{Processor, StreamApplication}
import org.apache.gearpump.util.{Graph, AkkaApp}
import org.apache.gearpump.util.Graph._

object HDFSExample extends AkkaApp with ArgumentsParser {

  override val options: Array[(String, CLIOption[Any])] = Array(
    "split" -> CLIOption[Int]("<how many split tasks>", required = false, defaultValue = Some(1)),
    "sink" -> CLIOption[Int]("<how many sum tasks>", required = false, defaultValue = Some(1)),
    "destPath" -> CLIOption[String]("<the base path write file to>", required = false,
      defaultValue = Some("/tmp")),
    "debug" -> CLIOption[Boolean]("<true|false>", required = false, defaultValue = Some(false))
  )

  def application(config: ParseResult, system: ActorSystem): StreamApplication = {
    implicit val actorSystem = system
    val splitNum = config.getInt("split")
    val sinkNum = config.getInt("sink")
    val base = config.getString("destPath")
    val split = Processor[Split](splitNum)
    val sequenceFileSink = new SequenceFileSink(UserConfig.empty, base)
    val sum = DataSinkProcessor(sequenceFileSink, sinkNum)
    val partitioner = new HashPartitioner

    val app = StreamApplication("hdfs_sink", Graph(split ~ partitioner ~> sum), UserConfig.empty)
    app
  }

  override def main(akkaConf: Config, args: Array[String]): Unit = {
    val config = parse(args)
    val context = ClientContext(akkaConf)
    val appId = context.submit(application(config, context.system))
    context.close()
  }
}


