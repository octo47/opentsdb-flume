package ru.yandex.opentsdb.flume;

import com.google.common.base.Throwables;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import net.opentsdb.core.TSDB;
import net.opentsdb.core.Tags;
import net.opentsdb.core.WritableDataPoints;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.lifecycle.LifecycleState;
import org.apache.flume.sink.AbstractSink;
import org.hbase.async.HBaseClient;
import org.hbase.async.HBaseRpc;
import org.hbase.async.PleaseThrottleException;
import org.hbase.async.PutRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.HashMap;

/**
 * Sink, capable to do writes into opentsdb.
 * This version expects full command in event body.
 *
 * @author Andrey Stepachev
 */
public class OpenTSDBSink extends AbstractSink implements Configurable {

  private final Logger logger = LoggerFactory.getLogger(OpenTSDBSink.class);
  public static final Charset UTF8 = Charset.forName("UTF-8");


  private SinkCounter sinkCounter;
  private int batchSize;

  private TSDB tsdb;
  private HBaseClient hbaseClient;
  private String zkquorum;
  private String zkpath;
  private String seriesTable;
  private String uidsTable;
  private int cacheSize = 2048;

  private class State {
    boolean throttle = false;
    Exception failure;
    Cache<String, WritableDataPoints> datapoints =
            CacheBuilder.newBuilder().maximumSize(cacheSize).build();


    private WritableDataPoints getDataPoints(final String metric,
                                             final HashMap<String, String> tags) {
      final String key = metric + tags;
      WritableDataPoints dp = datapoints.getIfPresent(key);
      if (dp != null) {
        return dp;
      }
      datapoints.put(key, dp);
      return dp;
    }

  }

  @Override
  public Status process() throws EventDeliveryException {
    Status result = Status.READY;
    final Channel channel = getChannel();
    final State state = new State();
    final class Errback implements Callback<Object, Exception> {
      public Object call(final Exception arg) {
        if (arg instanceof PleaseThrottleException) {
          final PleaseThrottleException e = (PleaseThrottleException) arg;
          logger.warn("Need to throttle, HBase isn't keeping up.", e);
          state.throttle = true;
          final HBaseRpc rpc = e.getFailedRpc();
          if (rpc instanceof PutRequest) {
            hbaseClient.put((PutRequest) rpc);  // Don't lose edits.
          }
          return null;
        }
        state.failure = arg;
        return arg;
      }

      public String toString() {
        return "importFile errback";
      }
    }
    final Errback errback = new Errback();
    int batch = 0;
    do {
      Transaction transaction = channel.getTransaction();
      Event event;
      try {
        while ((event = channel.take()) != null && batch < batchSize) {
          final int idx = eventBodyStart(event);
          if (idx == -1) {
            logger.error("empty event");
            continue;
          }
          final byte[] body = event.getBody();
          final String[] words = Tags.splitString(
                  new String(body, idx, body.length - idx, UTF8), ' ');
          final String metric = words[0];
          if (metric.length() <= 0) {
            logger.error("invalid metric: " + metric);
            continue;
          }
          final long timestamp = Tags.parseLong(words[1]);
          if (timestamp <= 0) {
            logger.error("invalid metric: " + metric);
            continue;
          }
          final String value = words[2];
          if (value.length() <= 0) {
            throw new RuntimeException("invalid value: " + value);
          }
          final HashMap<String, String> tags = new HashMap<String, String>();
          for (int i = 3; i < words.length; i++) {
            if (!words[i].isEmpty()) {
              Tags.parse(tags, words[i]);
            }
          }
          final WritableDataPoints dp = state.getDataPoints(metric, tags);
          Deferred<Object> d;
          if (Tags.looksLikeInteger(value)) {
            d = dp.addPoint(timestamp, Tags.parseLong(value));
          } else {  // floating point value
            d = dp.addPoint(timestamp, Float.parseFloat(value));
          }
          d.addErrback(errback);
          if (state.throttle) {
            logger.info("Throttling...");
            long throttle_time = System.nanoTime();
            try {
              d.joinUninterruptibly();
            } catch (Exception e) {
              throw new RuntimeException("Should never happen", e);
            }
            throttle_time = System.nanoTime() - throttle_time;
            if (throttle_time < 1000000000L) {
              logger.info("Got throttled for only " + throttle_time + "ns, sleeping a bit now");
              try {
                Thread.sleep(1000);
              } catch (InterruptedException e) {
                throw new RuntimeException("interrupted", e);
              }
            }
            logger.info("Done throttling...");
            state.throttle = false;
          }
          batch++;
        }
        try {
          tsdb.flush().join();
        } catch (Exception e) {
          throw Throwables.propagate(e);
        }
        sinkCounter.incrementBatchCompleteCount();
        if (batch < batchSize)
          sinkCounter.incrementBatchUnderflowCount();
      } finally {
        transaction.close();
      }
    } while (batch > 0 && getLifecycleState().equals(LifecycleState.IDLE));

    return result;
  }

  private byte[] PUT = {'p', 'u', 't'};

  public int eventBodyStart(Event event) {
    int idx = 0;
    final byte[] body = event.getBody();
    for (byte b : PUT) {
      if (body[idx++] != b)
        return -1;
    }
    while (idx < body.length) {
      if (body[idx] != ' ' && body[idx] != '\t')
        return idx;
    }
    return -1;
  }


  @Override
  public void configure(Context context) {
    sinkCounter = new SinkCounter(getName());
    batchSize = context.getInteger("batchSize", 100);
    zkquorum = context.getString("zkquorum");
    zkpath = context.getString("zkpath", "/hbase");
    seriesTable = context.getString("table.main", "tsdb");
    uidsTable = context.getString("table.main", "tsdb");
  }

  @Override
  public synchronized void start() {
    super.start();
    hbaseClient = new HBaseClient(zkquorum, zkpath);
    tsdb = new TSDB(hbaseClient, seriesTable, uidsTable);
  }

  @Override
  public synchronized void stop() {
    if (tsdb != null)
      try {
        tsdb.shutdown().joinUninterruptibly();
        tsdb = null;
        hbaseClient = null;
      } catch (Exception e) {
        logger.error("Can't shutdown tsdb", e);
        throw Throwables.propagate(e);
      }
    super.stop();
  }
}
