/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * 
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka

import java.util.Properties

import joptsimple.OptionParser
import kafka.server.KafkaServerStartable
import kafka.utils.Implicits._
import kafka.utils.{CommandLineUtils, Exit, Logging}
import org.apache.kafka.common.utils.{Java, LoggingSignalHandler, OperatingSystem, Utils}

import scala.jdk.CollectionConverters._

/**
 * Kafka也就两个方法：①getPropsFromArgs；②main
 * */
object Kafka extends Logging {


  /**
   * 这个方法主要是从配置文件server.properties加载配置，另外对于启动时，通过--override传入的，用来覆盖server.properties文件里的配置的额外配置项，也会在这里加载并覆盖文件里的配置项。
   * */
  def getPropsFromArgs(args: Array[String]): Properties = {
    /**
     * OptionParser 是jopt-simple命令行解析框架里面的类，和JCommander框架类似，
     * pulsar的命令行参数解析就是用的JCommander框架，而kafka用的jopt-simple框架,flink使用的是Apache Commons CLI
     * */
    val optionParser = new OptionParser(false)
    /**
     * 可选属性，应该覆盖在服务器属性文件中设置的值。可以在启动的时候这样覆盖：
     * sh kafka-server-start.sh  ../config/server.properties  --override zookeeper.connection.timeout.ms=111 --override zookeeper.connect=127.0.0.1:2181
     * */
    val overrideOpt = optionParser.accepts("override", "Optional property that should override values set in server.properties file")
      .withRequiredArg()
      .ofType(classOf[String])

    /**
     * 这里是打印版本信息，并退出
     * */
    // This is just to make the parameter show up in the help output, we are not actually using this due the
    // fact that this class ignores the first parameter which is interpreted as positional and mandatory
    // but would not be mandatory if --version is specified
    // This is a bit of an ugly crutch till we get a chance to rework the entire command line parsing
    optionParser.accepts("version", "Print version information and exit.")

    /**
     * 如果参数个数为0，或者参数传入了--help就打印出参数用法
     * */
    if (args.length == 0 || args.contains("--help")) {
      CommandLineUtils.printUsageAndDie(optionParser,
        "USAGE: java [options] %s server.properties [--override property=value]*".format(this.getClass.getCanonicalName.split('$').head))
    }

    /**
     * 如果传入了版本信息，就打印版本信息。
     * */
    if (args.contains("--version")) {
      CommandLineUtils.printVersionAndDie()
    }

    /**
     * 这里是读取server.properties配置文件，然后构建出props。
     * */
    val props = Utils.loadProps(args(0))

    /**
     * 如果参数个数大于1，那么就说明传入了额外的key和value配置吧 那么就会从overrideOpt中读取额外的覆盖配置。
     * */
    if (args.length > 1) {
      val options = optionParser.parse(args.slice(1, args.length): _*)

      /**
       * 这里好像是检查不符合要求的参数个数，如果大于0  就打印用法
       * */
      if (options.nonOptionArguments().size() > 0) {
        CommandLineUtils.printUsageAndDie(optionParser, "Found non argument parameters: " + options.nonOptionArguments().toArray.mkString(","))
      }

      /**
       * 这里似乎是可以在启动的时候，传入一些配置key=value 来修改配置文件里的配置项？ 是的，修改方式是在启动的时候，比如：
       * sh kafka-server-start.sh  ../config/server.properties  --override zookeeper.connection.timeout.ms=111 --override zookeeper.connect=127.0.0.1:2181
       *
       * 传入的--override项，会存到overrideOpt中，然后在这里利用命令行解析工具进行解析.
       * */
      props ++= CommandLineUtils.parseKeyValueArgs(options.valuesOf(overrideOpt).asScala)
    }
    props
  }

  def main(args: Array[String]): Unit = {
    try {
      /**
       * 根据参数加载Prop配置,
       * 主要就是根据传入的server.properties路径 然后加载到serverProps中
       * */
      val serverProps = getPropsFromArgs(args)
      /**
       * 根据Prop配置来构造一个kafkaServerStartable类对象实例.
       * KafkaServerStartable 这个是主要的逻辑。
       * */
      val kafkaServerStartable = KafkaServerStartable.fromProps(serverProps)

      try {
        /**
         * 如果不是windows操作系统，看起来这里会做一些日志相关的操作：
         * 那么这个到底是在干啥呢：
         * new LoggingSignalHandler().register()
         *
         * 以及Java.isIbmJdk又是啥?
         * */
        if (!OperatingSystem.IS_WINDOWS && !Java.isIbmJdk)
          new LoggingSignalHandler().register()
      } catch {
        case e: ReflectiveOperationException =>
          warn("Failed to register optional signal handler that logs a message when the process is terminated " +
            s"by a signal. Reason for registration failure is: $e", e)
      }

      /**
       * 这里是添加一个shutdown hook
       * */
      // attach shutdown handler to catch terminating signals as well as normal termination
      Exit.addShutdownHook("kafka-shutdown-hook", kafkaServerStartable.shutdown())

      /**
       * 这里开始启动服务
       * */
      kafkaServerStartable.startup()
      /**
       * 这里wait 服务shutdown
       * */
      kafkaServerStartable.awaitShutdown()
    }
    catch {
      case e: Throwable =>
        fatal("Exiting Kafka due to fatal exception", e)
        Exit.exit(1)
    }
    Exit.exit(0)
  }
}
