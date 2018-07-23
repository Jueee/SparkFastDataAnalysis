package com.jueee.learnspark.dataanalysis.util

object StringsUtilByScala {

  def printFinish()={
    println("[finish]" + new Exception().getStackTrace()(1).getMethodName)
  }
}

object FilesUtilByScalaTest{
  def main(args: Array[String]): Unit = {
    StringsUtilByScala.printFinish()
  }
}