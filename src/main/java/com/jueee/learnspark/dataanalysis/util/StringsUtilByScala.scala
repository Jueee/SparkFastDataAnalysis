package com.jueee.learnspark.dataanalysis.util

object StringsUtilByScala {

  def printFinish()={
    println("----------  " + new Exception().getStackTrace()(1).getMethodName + "  ----------")
  }
}

object FilesUtilByScalaTest{
  def main(args: Array[String]): Unit = {
    StringsUtilByScala.printFinish()
  }
}