package com.lightbend.kafka.scala.queryablestate

case class HostStoreInfo (host: String, port: Int, storeNames: Seq[String]){
  override def toString: String = s"HostStoreInfo{host= + $host + port=$port storeNames= ${storeNames.mkString(",")}}"
}