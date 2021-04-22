package com.atguigu.utils

import java.io.InputStreamReader
import java.util.Properties

/**
 * 加载配置配文件
 */
object PropertiesUtil {

  def load(propertieName: String): Properties = {

    val prop = new Properties()
    prop.load(
      new InputStreamReader(Thread.currentThread().getContextClassLoader.getResourceAsStream(propertieName), "UTF-8")
    )


    prop
  }
}
