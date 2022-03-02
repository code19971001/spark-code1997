package com.it.framework.application

import com.it.framework.common.TApplication
import com.it.framework.controller.WordCountController

/**
 * @author : code1997
 * @date : 2022/3/2 22:09
 */
object WordCountApplication extends App with TApplication {

  start() {
    val wordCountController = new WordCountController()
    wordCountController.dispatch()
  }

}
