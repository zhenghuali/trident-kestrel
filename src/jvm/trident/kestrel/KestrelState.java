package trident.kestrel;

import java.io.Serializable;
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

    public Options(String hostString, String queueName,
        int connectionLimit, int connectionTimeout, int maxWaiters,
        int retries, int requestTimeout) {
      this.hostString = hostString;
      this.queueName = queueName;
      this.connectionLimit = connectionLimit;
      this.connectionTimeout = connectionTimeout;
      this.maxWaiters = maxWaiters;
      this.retries = retries;
      this.requestTimeout = requestTimeout;
    }
  }

  public static class Factory implements StateFactory {
    Options options;

    public Factory(Options options) {
      this.options = options;
    }

    public State makeState(Map conf, IMetricsContext metricsContext, int partionIndex, int numPartitions) {
      return new KestrelState(options);
    }
  }

  private final Client kestrelClient;
  private final Options options;

  public KestrelState(Options options) {
    this.options = Preconditions.checkNotNull(options);
    this.kestrelClient = Client.newInstance(
        ClientBuilder.safeBuildFactory(ClientBuilder.get()
            .name(FINAGLE_NAME).reportTo(STATS_RECEIVER)
            .hosts(options.hostString).hostConnectionLimit(options.connectionLimit)
            .hostConnectionMaxWaiters(options.maxWaiters).codec(Kestrel.get())
            .connectTimeout(Duration.fromTimeUnit(options.connectionTimeout, TimeUnit.MILLISECONDS))
            .retries(options.retries)
            .requestTimeout(Duration.fromTimeUnit(options.requestTimeout, TimeUnit.MILLISECONDS))
        ));
    LOG.info("Created kestrel client to " + options.hostString);
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
    LOG.debug("Items to enqueue " + objectsToEnqueue.size());
    try {
      for (T oneItem:objectsToEnqueue) {
        ChannelBuffer buffer = ChannelBuffers.wrappedBuffer(options.serializer.serialize(oneItem));
        futures.add(kestrelClient.set(options.queueName, buffer));
      }
      // now harvest the futures
      for (Future<Response> future:futures) {
        LOG.debug("processed response " + future.get());
      }
    } catch (Exception e) {
      LOG.warn("Received exception during enqueue " + e);
      throw new ReportedFailedException("Failed kestrel enqueue:", e);
    }
  }
}

