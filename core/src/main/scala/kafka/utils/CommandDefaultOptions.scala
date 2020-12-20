/**
  * Licensed to the Apache Software Foundation (ASF) under one or more
  * contributor license agreements.  See the NOTICE file distributed with
  * this work for additional information regarding copyright ownership.
  * The ASF licenses this file to You under the Apache License, Version 2.0
  * (the "License"); you may not use this file except in compliance with
  * the License.  You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */

package kafka.utils

import joptsimple.{OptionParser, OptionSet}

/**
 * args: 要解析的参数
 * allowCommandOptionAbbreviation: 是否允许缩写，默认是不允许
 * */
abstract class CommandDefaultOptions(val args: Array[String], allowCommandOptionAbbreviation: Boolean = false) {
  /**
   * OptionParser是jopt-simple库的类。
   * */
  val parser = new OptionParser(allowCommandOptionAbbreviation)
  /**
   * helpOpt和versionOpt是统一的，所有解析命令都有。所以先加入到parser中
   * */
  val helpOpt = parser.accepts("help", "Print usage information.").forHelp()
  val versionOpt = parser.accepts("version", "Display Kafka version.").forHelp()

  /**
   * 后面会通过解析parser对options赋值。
   * parser在此之前会接收所有的传入参数，当然最先接收的就是helpOpt和versionOpt。
   *
   * options表示当前命令可以接收的所有参数
  * */
  var options: OptionSet = _
}
