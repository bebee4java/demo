package org.scala.flink.stream

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time

object StreamingWindowWC {

  def main(args: Array[String]): Unit = {

    val port : Int = try {
      ParameterTool.fromArgs(args).getInt("port")
    }catch {
      case e : Exception =>{
        System.err.println("your don't input port")
      }
        9000
    }

    val env : StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val sourceStream = env.socketTextStream("localhost", port, '\n')

    import org.apache.flink.api.scala._
//    implicit val typeInfo = TypeInformation.of(classOf[Tuple2[String, Long]])
//    implicit val typeInfo1 = TypeInformation.of(classOf[String])
    val wc = sourceStream.flatMap(line => line.split("\\s"))
      .map(word => Tuple2(word, 1L))
      .keyBy(0)
      .timeWindow(Time.seconds(2), Time.seconds(1))
      .sum(1)

    wc.print().setParallelism(1)
    env.execute("flink streaming word count")

  }

}
