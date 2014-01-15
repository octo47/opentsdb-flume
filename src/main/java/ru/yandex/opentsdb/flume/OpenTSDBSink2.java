package ru.yandex.opentsdb.flume;

import com.google.common.base.Throwables;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import com.yammer.metrics.core.Meter;
import net.opentsdb.core.TSDB;
import net.opentsdb.core.Tags;
import net.opentsdb.core.WritableDataPoints;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.ChannelCounter;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.hbase.async.HBaseClient;
import org.hbase.async.HBaseRpc;
import org.hbase.async.PleaseThrottleException;
import org.hbase.async.PutRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Sink, capable to do writes into opentsdb.
 * This version expects full command in event body.
 *
 * @author Andrey Stepachev
 */
public class OpenTSDBSink2 extends AbstractSink implements Configurable {

  private final Logger logger = LoggerFactory.getLogger(OpenTSDBSink2.class);
  public static final Charset UTF8 = Charset.forName("UTF-8");

  private SinkCounter sinkCounter;
  private ChannelCounter channelCounter;
  private int batchSize;
  private Semaphore statesPermitted;

  private TSDB tsdb;
  private HBaseClient hbaseClient;
  private String zkquorum;
  private String zkpath;
  private String seriesTable;
  private String uidsTable;
  private Meter pointsCounter;

  /**
   * State object holds result of asynchronous operations.
   * Implements throttle, when hbase can't handle incoming
   * rate of data points.
   * This object should be added as callback to handle throttles.
   */
  private class State implements Callback<Object, Object> {
    volatile boolean throttle = false;
    volatile Exception failure;
    private long stime = System.currentTimeMillis();
    private AtomicInteger inFlight = new AtomicInteger(0);
    private Semaphore signal = new Semaphore(0);
    private final Callback<Void, State> callback;
    private final int count;

    private State(List<EventData> data) throws Exception {
      this(data, null);
    }

    private State(List<EventData> data, Callback<Void, State> callback) throws Exception {
      this.callback = callback;
      this.count = data.size();
      writeDataPoints(data);
    }

    public Object call(final Object arg) throws Exception {
      if (arg instanceof PleaseThrottleException) {
        final PleaseThrottleException e = (PleaseThrottleException) arg;
        logger.warn("Need to throttle, HBase isn't keeping up.", e);
        throttle = true;
        final HBaseRpc rpc = e.getFailedRpc();
        if (rpc instanceof PutRequest) {
          hbaseClient.put((PutRequest) rpc);  // Don't lose edits.
        }
        return null;
      } else if (arg instanceof Exception) {
        int now = inFlight.decrementAndGet();
        if (callback != null && now == 0)
          callback.call(this);
        failure = (Exception) arg;
      } else {
        pointsCounter.mark();
        int now = inFlight.decrementAndGet();
        if (callback != null && now == 0)
          callback.call(this);
      }
      return arg;
    }

    /**
     * Main entry method for adding points
     *
     * @param data points list
     */
    private void writeDataPoints(List<EventData> data) throws Exception {
      if (data.size() < 1)
        return;
      inFlight.incrementAndGet(); // hack for not complete before we push all points
      try {
        Set<String> failures = new HashSet<String>();
        long prevTs = data.get(0).timestamp;
        for (EventData eventData : data) {
          try {
            if (eventData.timestamp == prevTs)
              continue;
            sinkCounter.incrementEventDrainAttemptCount();
            prevTs = eventData.timestamp;
            final Deferred<Object> d =
                    eventData.writePoint(tsdb);
            d.addBoth(this);
            if (throttle)
              throttle(d);
            inFlight.incrementAndGet();
            sinkCounter.incrementEventDrainSuccessCount();
          } catch (IllegalArgumentException ie) {
            failures.add(ie.getMessage());
          }
        }
        if (failures.size() > 0) {
          logger.error("Points imported with " + failures.toString() + " IllegalArgumentExceptions");
        }
      } finally {
        int total = inFlight.decrementAndGet();
        if (total == 0 && callback != null)
          callback.call(this);
      }
    }

    /**
     * Helper method, implements throttle.
     * Sleeps, until throttle will be switch off
     * by successful operation.
     *
     * @param deferred
     */
    private void throttle(Deferred deferred) {
      logger.info("Throttling...");
      long throttle_time = System.nanoTime();
      try {
        deferred.join();
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
      throttle = false;
    }

    public int count() {
      return count;
    }
  }

  /**
   * Parsed event.
   */
  private static class EventData {

    final String seriesKey;
    final String metric;
    final long timestamp;
    final String value;
    final HashMap<String, String> tags;

    public EventData(String seriesKey, String metric, long timestamp, String value, HashMap<String, String> tags) {
      this.seriesKey = seriesKey;
      this.metric = metric.replace('@', '_'); // FIXME: Workaround for opentsdb regarding the '@' symbol as invalid
      this.timestamp = timestamp;
      this.value = value;
      this.tags = tags;
    }

    public Deferred<Object> writePoint(TSDB tsdb) {
      Deferred<Object> d;
      if (Tags.looksLikeInteger(value)) {
        d = tsdb.addPoint(metric, timestamp, Tags.parseLong(value), tags);
      } else {  // floating point value
        d = tsdb.addPoint(metric, timestamp, Float.parseFloat(value), tags);
      }
      return d;
    }

    public Deferred<Object> makeDeferred(WritableDataPoints dp) {
      Deferred<Object> d;
      if (Tags.looksLikeInteger(value)) {
        d = dp.addPoint(timestamp, Tags.parseLong(value));
      } else {  // floating point value
        d = dp.addPoint(timestamp, Float.parseFloat(value));
      }
      return d;
    }

    static Comparator<EventData> orderBySeriesAndTimestamp() {
      return new Comparator<EventData>() {
        public int compare(EventData o1, EventData o2) {
          int c = o1.seriesKey.compareTo(o2.seriesKey);
          if (c == 0)
            return (o1.timestamp < o2.timestamp) ? -1 : (o1.timestamp == o2.timestamp ? 0 : 1);
          else
            return c;
        }
      };
    }

    static Comparator<EventData> orderByTimestamp() {
      return new Comparator<EventData>() {
        public int compare(EventData o1, EventData o2) {
          return (o1.timestamp < o2.timestamp) ? -1 : (o1.timestamp == o2.timestamp ? 0 : 1);
        }
      };
    }

    @Override
    public String toString() {
      return "EventData{" +
              "seriesKey='" + seriesKey + '\'' +
              ", metric='" + metric + '\'' +
              ", timestamp=" + timestamp +
              ", value='" + value + '\'' +
              ", tags=" + tags +
              '}';
    }
  }


  /**
   * EventData 'constructor'
   *
   * @param event what to parse
   * @return constructed EventData
   */
  private EventData parseEvent(final byte[] body) {
    final int idx = eventBodyStart(body);
    if (idx == -1) {
      logger.error("empty event");
      return null;
    }
    final String[] words = Tags.splitString(
            new String(body, idx, body.length - idx, UTF8), ' ');
    final String metric = words[0];
    if (metric.length() <= 0) {
      logger.error("invalid metric: " + metric);
      return null;
    }
    final long timestamp = Tags.parseLong(words[1]);
    if (timestamp <= 0) {
      logger.error("invalid metric: " + metric);
      return null;
    }
    final String value = words[2];
    if (value.length() <= 0) {
      logger.error("invalid value: " + value);
      return null;
    }
    final String[] tagWords = Arrays.copyOfRange(words, 3, words.length);
    // keep them sorted, helps to identify tags in different order, but
    // same set of them
    Arrays.sort(tagWords);
    final HashMap<String, String> tags = new HashMap<String, String>();
    StringBuilder seriesKey = new StringBuilder(metric);
    for (String tagWord : tagWords) {
      if (!tagWord.isEmpty()) {
        Tags.parse(tags, tagWord);
        seriesKey.append(' ').append(tagWord);
      }
    }
    return new EventData(seriesKey.toString(), metric, timestamp, value, tags);
  }

  /**
   * Process event, spawns up to 'parallel' number of executors,
   * and concurrently take transactions and send
   * to hbase.
   *
   * @return Status
   * @throws org.apache.flume.EventDeliveryException
   */
  @Override
  public Status process() throws EventDeliveryException {
    final Channel channel = getChannel();
    ArrayList<EventData> datas = new ArrayList<EventData>(batchSize);
    Transaction transaction = channel.getTransaction();
    Event event;
    try {
      transaction.begin();
      long stime = System.currentTimeMillis();
      while ((event = channel.take()) != null && datas.size() < batchSize) {
        try {
          BatchEvent be = new BatchEvent(event.getBody());
          for (byte[] body : be) {
            final EventData eventData = parseEvent(body);
            if (eventData == null)
              continue;

            datas.add(eventData);
          }
        } catch (Exception e) {
          continue;
        }
      }
      final int size = datas.size();
      channelCounter.addToEventTakeSuccessCount(size);
      if (size == 1) {
        statesPermitted.acquire();
        new State(datas, returnPermitCb());
      } else if (size > 0) {
        stime = System.currentTimeMillis();
        // sort incoming datapoints, tsdb doesn't like unordered
        Collections.sort(datas, EventData.orderBySeriesAndTimestamp());
//        int start = 0;
//        String seriesKey = datas.get(0).seriesKey;
//        for (int end = 1; end < size; end++) {
//          if (!seriesKey.equals(datas.get(end).seriesKey)) {
//            statesPermitted.acquire();
//            new State(datas.subList(start, end), returnPermitCb());
//            start = end;
//          }
//        }
        statesPermitted.acquire();
        new State(datas, returnPermitCb());
      }
      if (size > 0) {
        sinkCounter.incrementBatchCompleteCount();
        channelCounter.addToEventPutSuccessCount(size);
      }
      transaction.commit();
    } catch (Throwable t) {
      logger.error("Batch failed: number of events " + datas.size(), t);
      transaction.rollback();
      throw Throwables.propagate(t);
    } finally {
      transaction.close();
    }
    return Status.READY;
  }

  private Callback<Void, State> returnPermitCb() {
    return new Callback<Void, State>() {
      @Override
      public Void call(State state) throws Exception {
        statesPermitted.release();
        logger.info("Batch finished with " + state.count() + " events in " + (System.currentTimeMillis() -
                state.stime) + "ms " + statesPermitted.availablePermits() + " permits now");
        if (state.failure != null)
          logger.error("Batch finished with most recent exception", state.failure);
        return null;
      }
    };
  }

  private byte[] PUT = {'p', 'u', 't'};

  public int eventBodyStart(final byte[] body) {
    int idx = 0;
    for (byte b : PUT) {
      if (body[idx++] != b)
        return -1;
    }
    while (idx < body.length) {
      if (body[idx] != ' ') {
        return idx;
      }
      idx++;
    }
    return -1;
  }


  @Override
  public void configure(Context context) {
    sinkCounter = new SinkCounter(getName());
    channelCounter = new ChannelCounter(getName());

    batchSize = context.getInteger("batchSize", 100);
    zkquorum = context.getString("zkquorum");
    zkpath = context.getString("zkpath", "/hbase");
    seriesTable = context.getString("table.main", "tsdb");
    uidsTable = context.getString("table.uids", "tsdb-uid");
    statesPermitted = new Semaphore(context.getInteger("states", 50));
    pointsCounter = Metrics.registry.newMeter(this.getClass(), "write", "points", TimeUnit.SECONDS);
  }

  @Override
  public synchronized void start() {
    logger.info(String.format("Starting: %s:%s series:%s uids:%s batchSize:%d",
            zkquorum, zkpath, seriesTable, uidsTable, batchSize));
    hbaseClient = new HBaseClient(zkquorum, zkpath);
    tsdb = new TSDB(hbaseClient, seriesTable, uidsTable);
    channelCounter.start();
    sinkCounter.start();
    super.start();
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
    channelCounter.stop();
    sinkCounter.stop();
    super.stop();
  }
}
