package kafka.tools

import javax.management.remote.{JMXConnector, JMXConnectorFactory, JMXServiceURL}
import javax.management.{Attribute, MBeanServerConnection, ObjectName}
import joptsimple.OptionParser
import kafka.utils.{CommandLineUtils, Exit}

import scala.collection.JavaConverters.asScalaBufferConverter

object DynamicResetLogLevel {

  def main(args: Array[String]): Unit = {
    // Parse command line
    val parser = new OptionParser(false)
    val jmxServiceUrlOpt =
      parser.accepts("jmx-url", "The url to connect to poll JMX data. See Oracle javadoc for JMXServiceURL for details.")
        .withRequiredArg
        .describedAs("service-url")
        .ofType(classOf[String])
        .defaultsTo("service:jmx:rmi:///jndi/rmi://:9999/jmxrmi")
    val objectNameOpt =
      parser.accepts("object-name", "A JMX object name to use as a query. This can contain wild cards, and this option " +
        "can be given multiple times to specify more than one query. If no objects are specified " +
        "all objects will be queried.")
        .withRequiredArg
        .describedAs("name")
        .ofType(classOf[String])

    val logNameOpt =
      parser.accepts("logName", "To set log name.")
        .withRequiredArg
        .describedAs("logName")
        .ofType(classOf[String])

    val logLevelOpt =
      parser.accepts("logLevel", "To set log level.")
        .withRequiredArg
        .describedAs("logLevel")
        .ofType(classOf[String])

    val helpOpt = parser.accepts("help", "Print usage information.")


    if (args.length == 0)
      CommandLineUtils.printUsageAndDie(parser, "Dump JMX values to standard output.")

    val options = parser.parse(args: _*)

    if (options.has(helpOpt)) {
      parser.printHelpOn(System.out)
      Exit.exit(0)
    }

    val url = new JMXServiceURL(options.valueOf(jmxServiceUrlOpt))
    //    val url: JMXServiceURL = new JMXServiceURL("service:jmx:rmi:///jndi/rmi://localhost:8011/jmxrmi")
    val jmxc: JMXConnector = JMXConnectorFactory.connect(url, null)
    val mbsc: MBeanServerConnection = jmxc.getMBeanServerConnection
    val mbeanName: ObjectName = new ObjectName(options.valueOf(objectNameOpt))
    //    val mbeanName: ObjectName = new ObjectName("kafka:type=kafka.Log4jController")
    val ret1 = mbsc.invoke(mbeanName, "getLogLevel", Array[AnyRef](options.valueOf(logNameOpt)), Array[String]("java.lang.String"))
    println(ret1)
    val ret2 = mbsc.invoke(mbeanName, "setLogLevel", Array[AnyRef](options.valueOf(logNameOpt), options.valueOf(logLevelOpt)), Array[String]("java.lang.String", "java.lang.String"))
    val ret3 = mbsc.invoke(mbeanName, "getLogLevel", Array[AnyRef](options.valueOf(logNameOpt)), Array[String]("java.lang.String"))
    println(ret2)
    println(ret3)
  }

}
