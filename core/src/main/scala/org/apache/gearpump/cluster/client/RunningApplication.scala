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
package org.apache.gearpump.cluster.client

import akka.pattern.ask
import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.util.Timeout
import org.apache.gearpump.cluster.ClientToMaster.{RegisterAppResultListener, ResolveAppId, ShutdownApplication}
import org.apache.gearpump.cluster.MasterToClient._
import org.apache.gearpump.cluster.client.RunningApplication.{AppResultListener, WaitUntilFinish}
import org.apache.gearpump.util.ActorUtil
import org.apache.gearpump.cluster.client.RunningApplication._

import scala.concurrent.duration._
import scala.concurrent.Future
import scala.util.{Failure, Success}
import scala.concurrent.ExecutionContext.Implicits.global

class RunningApplication(val appId: Int, master: ActorRef, timeout: Timeout,
    system: ActorSystem) {
  lazy val appMaster: Future[ActorRef] = resolveAppMaster(appId)

  def shutDown(): Unit = {
    val result = ActorUtil.askActor[ShutdownApplicationResult](master,
      ShutdownApplication(appId), timeout)
    result.appId match {
      case Success(_) =>
      case Failure(ex) => throw ex
    }
  }

  def waitUnilFinish(): Unit = {
    val delegator = system.actorOf(Props(new AppResultListener(appId, master)))
    val result = ActorUtil.askActor[ApplicationResult](delegator, WaitUntilFinish, INF_TIMEOUT)
    result match {
      case failed: ApplicationFailed =>
        throw failed.error
      case _ =>
    }
  }

  def askAppMaster[T](msg: Any): Future[T] = {
    appMaster.flatMap(_.ask(msg)(timeout).asInstanceOf[Future[T]])
  }

  private def resolveAppMaster(appId: Int): Future[ActorRef] = {
    master.ask(ResolveAppId(appId))(timeout).
      asInstanceOf[Future[ResolveAppIdResult]].map(_.appMaster.get)
  }
}

object RunningApplication {
  // This magic number is derived from Akka's configuration, which is the maximum delay
  private val INF_TIMEOUT = new Timeout(2147482 seconds)

  private case object WaitUntilFinish

  private class AppResultListener(appId: Int, master: ActorRef) extends Actor {
    private var client: ActorRef = _
    master ! RegisterAppResultListener(appId, self)

    override def receive: Receive = {
      case WaitUntilFinish =>
        this.client = sender()
      case result: ApplicationResult =>
        client forward result
        context.stop(self)
    }
  }
}

