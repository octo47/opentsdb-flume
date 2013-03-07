package ru.yandex.opentsdb.flume;

import com.google.common.base.Throwables;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import net.opentsdb.core.TSDB;
import net.opentsdb.core.Tags;
import net.opentsdb.core.WritableDataPoints;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

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
  private int parallel;

  /**
   * State object holds result of asynchronous operations.
   * Implements throttle, when hbase can't handle incoming
   * rate of data points.
   * This object should be added as callback to handle throttles.
   */
  private class State implements Callback<Object, Exception> {
    volatile boolean throttle = false;
    volatile Exception failure;
    private List<Deferred> inFlight = new ArrayList<Deferred>(batchSize);

    public Object call(final Exception arg) {
      if (arg instanceof PleaseThrottleException) {
        final PleaseThrottleException e = (PleaseThrottleException) arg;
        logger.warn("Need to throttle, HBase isn't keeping up.", e);
        throttle = true;
        final HBaseRpc rpc = e.getFailedRpc();
        if (rpc instanceof PutRequest) {
          hbaseClient.put((PutRequest) rpc);  // Don't lose edits.
        }
        return null;
      }
      failure = arg;
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
      final WritableDataPoints dataPoints = tsdb.newDataPoints();
      final EventData first = data.get(0);
      dataPoints.setSeries(first.metric, first.tags);
      dataPoints.setBatchImport(false);
      long prevTs = 0;
      for (EventData eventData : data) {
        if (eventData.timestamp == prevTs)
          continue;
        prevTs = eventData.timestamp;
        final Deferred<Object> d = eventData.makeDeferred(dataPoints, this);
        d.addErrback(this);
        if (throttle)
          throttle(d);
        inFlight.add(d);
      }
    }

    /**
     * Wait for all deferries are complete
     *
     * @throws Exception
     */
    private void join() throws Exception {
      for (Deferred deferred : inFlight) {
        if (throttle) {
          throttle(deferred);
        } else {
          deferred.join();
        }
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
      this.metric = metric;
      this.timestamp = timestamp;
      this.value = value;
      this.tags = tags;
    }

    public Deferred<Object> makeDeferred(WritableDataPoints dp, State state) {
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
  private EventData parseEvent(Event event) {
    final int idx = eventBodyStart(event);
    if (idx == -1) {
      logger.error("empty event");
      return null;
    }
    final byte[] body = event.getBody();
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
   * @throws EventDeliveryException
   */
  @Override
  public Status process() throws EventDeliveryException {
    final Channel channel = getChannel();
    final BlockingQueue<Integer> next = new ArrayBlockingQueue<Integer>(parallel + 1);
    final List<Future<Long>> futures = new ArrayList<Future<Long>>();
    final Callable<Long> worker = worker(channel, next);
    final ExecutorService service = Executors.newFixedThreadPool(parallel);

    try {
      futures.add(service.submit(worker));
      while (futures.size() > 0) {
        try {
          Integer handled;
          while ((handled = next.poll(100, TimeUnit.MILLISECONDS)) != null) {
            if (handled > batchSize / 2
                    && futures.size() < parallel
                    && !getLifecycleState().equals(LifecycleState.STOP)) {
              futures.add(service.submit(worker));
              futures.add(service.submit(worker));
              logger.info("Additional worker spawned: threads=" + futures.size() + ", processed=" + handled);
            }
          }
          final Iterator<Future<Long>> it = futures.iterator();
          while (it.hasNext()) {
            Future<Long> future = it.next();
            if (future.isDone()) {
              try {
                future.get();
              } catch (ExecutionException e) {
                logger.error("Worker failed: ", e.getCause());
              }
              it.remove();
            }
          }
        } catch (InterruptedException e) {
          service.shutdown();
        }
      }
      return Status.READY;
    } finally {
      service.shutdownNow();
    }
  }

  /**
   * Workhorse. Gets transaction, parses, sends to hbase storage.
   *
   * @param channel from where transactions are get
   * @param next
   * @return callable
   */
  private Callable<Long> worker(final Channel channel, final BlockingQueue<Integer> next) {
    return new Callable<Long>() {
      @Override
      public Long call() throws Exception {
        long total = 0;
        ArrayList<EventData> datas = new ArrayList<EventData>(batchSize);
        final State state = new State();
        Transaction transaction = channel.getTransaction();
        Event event;
        try {
          transaction.begin();
          while ((event = channel.take()) != null && datas.size() < batchSize) {
            if (state.failure != null)
              throw Throwables.propagate(state.failure);

            try {
              final EventData eventData = parseEvent(event);
              if (eventData == null)
                continue;

              datas.add(eventData);
            } catch (Exception e) {
              continue;
            }
          }
          final int size = datas.size();
          next.put(size);
          total += size;
          if (size == 1) {
            state.writeDataPoints(datas);
          } else if (size > 0) {
            // sort incoming datapoints, tsdb doesn't like unordered
            Collections.sort(datas, EventData.orderBySeriesAndTimestamp());
            int start = 0;
            String seriesKey = datas.get(0).seriesKey;
            for (int end = 1; end < size; end++) {
              if (!seriesKey.equals(datas.get(end).seriesKey)) {
                state.writeDataPoints(datas.subList(start, end));
                start = end;
              }
            }
            state.writeDataPoints(datas.subList(start, size));
            // trigger flush
            tsdb.flush();
            // and wait, until all our inflight deferred will complete
            state.join();
            sinkCounter.incrementBatchCompleteCount();
            if (size < batchSize)
              sinkCounter.incrementBatchUnderflowCount();
          }
          transaction.commit();
        } catch (Throwable t) {
          logger.error("Batch failed: number of events " + datas.size(), t);
          transaction.rollback();
          throw Throwables.propagate(t);
        } finally {
          transaction.close();
        }
        return total;
      }
    };
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
    batchSize = context.getInteger("batchSize", 100);
    zkquorum = context.getString("zkquorum");
    zkpath = context.getString("zkpath", "/hbase");
    seriesTable = context.getString("table.main", "tsdb");
    uidsTable = context.getString("table.uids", "tsdb-uid");
    parallel = context.getInteger("parallel", Runtime.getRuntime().availableProcessors() / 2 + 1);
  }

  @Override
  public synchronized void start() {
    logger.info(String.format("Starting: %s:%s series:%s uids:%s batchSize:%d",
            zkquorum, zkpath, seriesTable, uidsTable, batchSize));
    hbaseClient = new HBaseClient(zkquorum, zkpath);
    tsdb = new TSDB(hbaseClient, seriesTable, uidsTable);
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
    super.stop();
  }
}
