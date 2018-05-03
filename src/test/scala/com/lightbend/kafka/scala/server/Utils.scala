/**
  * Copyright (C) 2018 Lightbend Inc. <https://www.lightbend.com>
  */
package com.lightbend.kafka.scala.server

import java.io.File
import java.nio.file.{FileVisitOption, Files, Paths}
import java.util.Comparator

import scala.util.Try
import scala.collection.JavaConverters._

object Utils {
  def deleteDirectory(directory: File): Try[Unit] = Try {
    if (directory.exists()) {
      val rootPath = Paths.get(directory.getAbsolutePath)

      val files =
        Files.walk(rootPath, FileVisitOption.FOLLOW_LINKS).sorted(Comparator.reverseOrder()).iterator().asScala
      files.foreach(Files.delete)
    }
  }

  def dataDirectory(baseDir: String, directoryName: String): Try[File] = Try {

    val dataDirectory = new File(baseDir + directoryName)

    if (dataDirectory.exists() && !dataDirectory.isDirectory())
      throw new IllegalArgumentException(
        s"Cannot use $directoryName as a directory name because a file with that name already exists in $dataDirectory."
      )
    dataDirectory
  }
}
