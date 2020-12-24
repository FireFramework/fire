package com.zto.fire.metrics

import java.util.Random
import java.util.concurrent.TimeUnit

import com.codahale.metrics.jvm.{FileDescriptorRatioGauge, GarbageCollectorMetricSet, MemoryUsageGaugeSet, ThreadStatesGaugeSet}
import com.codahale.metrics.{ConsoleReporter, MetricRegistry, Slf4jReporter}

/**
 * Metrics模块测试
 *
 * @author ChengLong
 * @since 2.0.0
 * @create 2020-12-17 10:11
 */
class MetricsTest {
  val metrics = new MetricRegistry()

  // @Test
  def testMeter: Unit = {
    val reporter = ConsoleReporter.forRegistry(metrics).convertRatesTo(TimeUnit.SECONDS).convertDurationsTo(TimeUnit.MILLISECONDS).build
    reporter.start(1, TimeUnit.SECONDS)

    val requests = metrics.meter("requests")
    (1 to 100).foreach(i => {
      requests.mark()
      Thread.sleep(10)
    })
    Thread.sleep(1000)
  }

  // @Test
  def testHistogram: Unit = {
    val reporter = ConsoleReporter.forRegistry(metrics).convertRatesTo(TimeUnit.SECONDS).convertDurationsTo(TimeUnit.MILLISECONDS).build
    reporter.start(1, TimeUnit.SECONDS)

    val reporter2 = Slf4jReporter.forRegistry(metrics).convertDurationsTo(TimeUnit.SECONDS).convertDurationsTo(TimeUnit.MILLISECONDS).withLoggingLevel(Slf4jReporter.LoggingLevel.ERROR).build
    reporter2.start(1, TimeUnit.SECONDS)

    val resultCounts = metrics.histogram(MetricRegistry.name(classOf[MetricsTest], "result-counts"))
    val random = new Random()
    (1 to 1000).foreach(i => {
      resultCounts.update(random.nextInt(100))
      Thread.sleep(10)
    })
    Thread.sleep(1000)
  }

  // @Test
  def testJvm: Unit = {
    val reporter2 = ConsoleReporter.forRegistry(metrics)
      .convertRatesTo(TimeUnit.SECONDS)
      .convertDurationsTo(TimeUnit.MILLISECONDS)
      .build
    reporter2.start(3, TimeUnit.SECONDS)
    val reporter = Slf4jReporter.forRegistry(metrics).convertDurationsTo(TimeUnit.SECONDS).convertDurationsTo(TimeUnit.MILLISECONDS).withLoggingLevel(Slf4jReporter.LoggingLevel.ERROR).build
    reporter.start(5, TimeUnit.SECONDS)

    metrics.register("jvm.gc", new GarbageCollectorMetricSet())
    metrics.register("jvm.memroy", new MemoryUsageGaugeSet())
    metrics.register("jvm.thread-states", new ThreadStatesGaugeSet())
    metrics.register("jvm.fd.usage", new FileDescriptorRatioGauge())

    Thread.sleep(100000)
  }
}
