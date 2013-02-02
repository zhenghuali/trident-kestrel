package trident.kestrel;

import java.io.Serializable;
import java.lang.String;
import java.lang.SuppressWarnings;
import java.lang.System;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import com.twitter.finagle.builder.ClientBuilder;
import com.twitter.finagle.kestrel.java.Client;
import com.twitter.finagle.kestrel.protocol.Kestrel;
import com.twitter.finagle.kestrel.protocol.Response;
import com.twitter.finagle.stats.CommonsStatsReceiver;
import com.twitter.finagle.stats.StatsReceiver;
import com.twitter.util.Duration;
import com.twitter.util.Future;

import backtype.storm.metric.api.CombinedMetric;
import backtype.storm.metric.api.CountMetric;
import backtype.storm.metric.api.ICombiner;
import backtype.storm.task.IMetricsContext;
import backtype.storm.task.IMetricsContext;
import backtype.storm.topology.ReportedFailedException;
import org.apache.log4j.Logger;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

import storm.trident.state.JSONNonTransactionalSerializer;
import storm.trident.state.Serializer;
import storm.trident.state.State;
import storm.trident.state.StateFactory;

/**
 * Trident State implementation handles enqueue to kestrel.
 *
 */
public class KestrelState<T> implements State {
  private static final Logger LOG = Logger.getLogger(KestrelState.class);

  private static final String FINAGLE_NAME = "trident_kestrel";
  private static final StatsReceiver STATS_RECEIVER = new CommonsStatsReceiver();

  public static class Options<T> implements Serializable {
    public Serializer serializer = new JSONNonTransactionalSerializer();
    public final String hostString;
    public final int connectionLimit;
    public final int connectionTimeout;
    public final int maxWaiters;
    public final int retries;
    public final int requestTimeout;
    public final String queueName;
    public final String statsName;

    /**
     * Constructor of options for kestrel state. Note that statsName must be unique across exported
     * stats of toppologies.
     */
    public Options(String hostString, String queueName, int connectionLimit, int connectionTimeout,
                   int maxWaiters, int retries, int requestTimeout, String statsName) {
      this.hostString = hostString;
      this.queueName = queueName;
      this.connectionLimit = connectionLimit;
      this.connectionTimeout = connectionTimeout;
      this.maxWaiters = maxWaiters;
      this.retries = retries;
      this.requestTimeout = requestTimeout;
      this.statsName = statsName;
    }

    public Options(String hostString, String queueName, int connectionLimit, int connectionTimeout,
                   int maxWaiters, int retries, int requestTimeout) {
      this(hostString, queueName, connectionLimit, connectionTimeout, maxWaiters, retries,
          requestTimeout, null);
    }
  }

  public static class Factory implements StateFactory {
    Options options;

    public Factory(Options options) {
      this.options = options;
    }

    public State makeState(Map conf, IMetricsContext metricsContext, int partionIndex, int numPartitions) {
      return new KestrelState(options, metricsContext);
    }
  }

  private final Client kestrelClient;
  private final Options options;
  private final IMetricsContext metricsContext;
  private final String prefixName;
  private CountMetric errorCount;
  private CountMetric successCount;
  private CountMetric totalItemsEnqueued;
  private CombinedMetric sumLatencies;
  private CombinedMetric maxLatency;

  public KestrelState(Options options, IMetricsContext metricsContext) {
    this.options = Preconditions.checkNotNull(options);
    this.metricsContext = metricsContext;
    this.kestrelClient = Client.newInstance(
        ClientBuilder.safeBuildFactory(ClientBuilder.get()
            .name(FINAGLE_NAME).reportTo(STATS_RECEIVER)
            .hosts(options.hostString).hostConnectionLimit(options.connectionLimit)
            .hostConnectionMaxWaiters(options.maxWaiters).codec(Kestrel.get())
            .connectTimeout(Duration.fromTimeUnit(options.connectionTimeout, TimeUnit.MILLISECONDS))
            .retries(options.retries)
            .requestTimeout(Duration.fromTimeUnit(options.requestTimeout, TimeUnit.MILLISECONDS))
        ));
    this.prefixName = options.statsName;
    LOG.info("Created kestrel client to " + options.hostString);

    if (metricsContext != null && prefixName != null) {
      errorCount = new CountMetric();
      successCount = new CountMetric();
      totalItemsEnqueued = new CountMetric();
      sumLatencies = new CombinedMetric(new ICombiner<Long>() {
        @Override
        public Long identity() {
          return 0L;
        }
        @Override
        public Long combine(Long a, Long b) {
          return a + b;
        }
      });
      maxLatency = new CombinedMetric(new ICombiner<Long>() {
        @Override
        public Long identity() {
          return 0L;
        }
        @Override
        public Long combine(Long a, Long b) {
          return Math.max(a, b);
        }
      });
      errorCount = (CountMetric) metricsContext.registerMetric(prefixName + "-error-count",
          errorCount, 60);
      successCount = (CountMetric) metricsContext.registerMetric(prefixName + "-success-count",
          successCount, 60);
      totalItemsEnqueued = (CountMetric) metricsContext.registerMetric(prefixName + "-batch-size",
          totalItemsEnqueued, 60);
      sumLatencies = (CombinedMetric) metricsContext.registerMetric(prefixName + "-sum-latencies",
          sumLatencies, 60);
      maxLatency = (CombinedMetric) metricsContext.registerMetric(prefixName + "-max-latency",
          maxLatency, 60);
    }
  }

  public void beginCommit(Long txid) {
    // no op
  }

  public void commit(Long txid) {
    // no op
  }

  /**
   * Blocking call to enqueue to kestrel.
   * @param objectsToEnqueue list of objects to enqueue.
   */
  public void enqueue(List<T> objectsToEnqueue) {
    List<Future<Response>> futures = Lists.newArrayList();
    long timeStart = System.nanoTime();
    try {
      for (T oneItem:objectsToEnqueue) {
        ChannelBuffer buffer = ChannelBuffers.wrappedBuffer(options.serializer.serialize(oneItem));
        futures.add(kestrelClient.set(options.queueName, buffer));
      }
      // now harvest the futures
      for (Future<Response> future:futures) {
        future.get();
      }
      long latency = (System.nanoTime() - timeStart) / 1000;
      updateMetrics(true, latency, objectsToEnqueue.size());
    } catch (Exception e) {
      long latency = (System.nanoTime() - timeStart) / 1000;
      updateMetrics(false, latency, objectsToEnqueue.size());
      LOG.warn("Received exception during enqueue " + e);
    }
  }

  /**
   * Updates metrics after a service request including its success and latency.
   */
  private void updateMetrics(boolean success, long latencyMS, long totalRequests) {
    if (prefixName != null) {
      if (success) {
        successCount.incr();
      } else {
        errorCount.incr();
      }
      totalItemsEnqueued.incrBy(totalRequests);
      sumLatencies.update(latencyMS);
      maxLatency.update(latencyMS);
    }
  }
}

