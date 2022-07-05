/*
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

package com.zto.fire.shell.spark

import com.zto.fire.common.util.FireUtils

import java.io.BufferedReader

// scalastyle:off println
import scala.Predef.{println => _, _}
// scalastyle:on println
import scala.concurrent.Future
import scala.reflect.classTag
import scala.reflect.io.File
import scala.tools.nsc.{GenericRunnerSettings, Properties}
import scala.tools.nsc.Settings
import scala.tools.nsc.interpreter.{isReplDebug, isReplPower, replProps}
import scala.tools.nsc.interpreter.{AbstractOrMissingHandler, ILoop, IMain, JPrintWriter}
import scala.tools.nsc.interpreter.{NamedParam, SimpleReader, SplashLoop, SplashReader}
import scala.tools.nsc.interpreter.StdReplTags.tagOfIMain
import scala.tools.nsc.util.stringFromStream
import scala.util.Properties.{javaVersion, javaVmName, versionString}

/**
 *  A Spark-specific interactive shell.
 *  intp.interpret("println(\"hello world\")")
 */
class FireILoop(in0: Option[BufferedReader], out: JPrintWriter)
    extends ILoop(in0, out) {
  def this(in0: BufferedReader, out: JPrintWriter) = this(Some(in0), out)
  def this() = this(None, new JPrintWriter(Console.out, true))

  val initializationCommands: Seq[String] = Seq(
    "import org.apache.spark.SparkContext._",
    "import org.apache.spark.sql.functions._",
    "import com.zto.fire.spark.BaseSparkCore",
    "import com.zto.fire._",
    "import com.zto.fire.spark.util.SparkSingletonFactory",
    "import com.zto.fire.shell.spark.Test",
    """
    Test.main(null)
    @transient val spark = Test.getSparkSession
    @transient val fire = Test.getFire
    @transient val sc = Test.getSc
    """
  )

  def initializeSpark(): Unit = {
    if (!intp.reporter.hasErrors) {
      // `savingReplayStack` removes the commands from session history.
      savingReplayStack {
        initializationCommands.foreach(intp quietRun _)
      }
    } else {
      throw new RuntimeException(s"Scala $versionString interpreter encountered " +
        "errors during initialization")
    }
  }

  /** Print a welcome message */
  override def printWelcome(): Unit = {
    FireUtils.isSplash = false
    FireUtils.splash
    echo("Type in expressions to have them evaluated.")
    echo("Type :help for more information.")
  }

  /** Available commands */
  override def commands: List[LoopCommand] = standardCommands

  override def resetCommand(line: String): Unit = {
    super.resetCommand(line)
    initializeSpark()
    echo("Note that after :reset, state of SparkSession and SparkContext is unchanged.")
  }

  override def replay(): Unit = {
    initializeSpark()
    super.replay()
  }

  /**
   * The following code is mostly a copy of `process` implementation in `ILoop.scala` in Scala
   *
   * In newer version of Scala, `printWelcome` is the first thing to be called. As a result,
   * SparkUI URL information would be always shown after the welcome message.
   *
   * However, this is inconsistent compared with the existing version of Spark which will always
   * show SparkUI URL first.
   *
   * The only way we can make it consistent will be duplicating the Scala code.
   *
   * We should remove this duplication once Scala provides a way to load our custom initialization
   * code, and also customize the ordering of printing welcome message.
   */
  override def process(settings: Settings): Boolean = {

    def newReader = in0.fold(chooseReader(settings))(r => SimpleReader(r, out, interactive = true))

    /** Reader to use before interpreter is online. */
    def preLoop = {
      val sr = SplashReader(newReader) { r =>
        in = r
        in.postInit()
      }
      in = sr
      SplashLoop(sr, prompt)
    }

    /* Actions to cram in parallel while collecting first user input at prompt.
     * Run with output muted both from ILoop and from the intp reporter.
     */
    def loopPostInit(): Unit = mumly {
      // Bind intp somewhere out of the regular namespace where
      // we can get at it in generated code.
      intp.quietBind(NamedParam[IMain]("$intp", intp)(tagOfIMain, classTag[IMain]))

      // Auto-run code via some setting.
      ( replProps.replAutorunCode.option
        flatMap (f => File(f).safeSlurp())
        foreach (intp quietRun _)
        )
      // power mode setup
      if (isReplPower) enablePowerMode(true)
      initializeSpark()
      loadInitFiles()
      // SI-7418 Now, and only now, can we enable TAB completion.
      in.postInit()
    }
    def loadInitFiles(): Unit = settings match {
      case settings: GenericRunnerSettings =>
        for (f <- settings.loadfiles.value) {
          loadCommand(f)
          addReplay(s":load $f")
        }
        for (f <- settings.pastefiles.value) {
          pasteCommand(f)
          addReplay(s":paste $f")
        }
      case _ =>
    }
    // wait until after startup to enable noisy settings
    def withSuppressedSettings[A](body: => A): A = {
      val ss = this.settings
      import ss._
      val noisy = List(Xprint, Ytyperdebug)
      val noisesome = noisy.exists(!_.isDefault)
      val current = (Xprint.value, Ytyperdebug.value)
      if (isReplDebug || !noisesome) body
      else {
        this.settings.Xprint.value = List.empty
        this.settings.Ytyperdebug.value = false
        try body
        finally {
          Xprint.value = current._1
          Ytyperdebug.value = current._2
          intp.global.printTypings = current._2
        }
      }
    }
    def startup(): String = withSuppressedSettings {
      // let them start typing
      val splash = preLoop

      // while we go fire up the REPL
      try {
        // don't allow ancient sbt to hijack the reader
        savingReader {
          createInterpreter()
        }
        intp.initializeSynchronous()

        val field = classOf[scala.tools.nsc.interpreter.ILoop].getDeclaredFields.filter(_.getName.contains("globalFuture")).head
        field.setAccessible(true)
        field.set(this, Future successful true)

        if (intp.reporter.hasErrors) {
          echo("Interpreter encountered errors during initialization!")
          null
        } else {
          loopPostInit()
          printWelcome()
          splash.start()

          val line = splash.line           // what they typed in while they were waiting
          if (line == null) {              // they ^D
            try out print Properties.shellInterruptedString
            finally closeInterpreter()
          }
          line
        }
      } finally splash.stop()
    }

    this.settings = settings
    startup() match {
      case null => false
      case line =>
        try loop(line) match {
          case LineResults.EOF => out print Properties.shellInterruptedString
          case _ =>
        }
        catch AbstractOrMissingHandler()
        finally closeInterpreter()
        true
    }
  }
}


