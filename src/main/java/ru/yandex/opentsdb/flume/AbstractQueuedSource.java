package ru.yandex.opentsdb.flume;

import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.CounterGroup;
import org.apache.flume.Event;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author Andrey Stepachev
 */
public class AbstractQueuedSource
        extends AbstractSource
        implements EventDrivenSource, Configurable {

  private static final Logger logger = LoggerFactory
          .getLogger(AbstractQueuedSource.class);

  protected BlockingQueue<Event> queue;
  protected CounterGroup counterGroup = new CounterGroup();
  protected Lock lock = new ReentrantLock();
  protected Condition cond = lock.newCondition();
  protected Thread flushThread;
  private int batchSize;

  private int flush() {
    return flush(false);
  }

  protected int flush(boolean force) {
    try {
      final List<Event> list = new ArrayList<Event>();
      final int drained = queue.drainTo(list, batchSize);
      getChannelProcessor().processEventBatch(list);
      list.clear();
      return drained;
    } catch (ChannelException fce) {
      if (force) {
        logger.error("Forced to flush, but we've lost " + queue.size() +
                " events, channel don't accepts data", fce);
        return 0;
      } else {
        logger.error("Can't flush, channel don't accept our events", fce);
        throw fce;
      }
    }
  }

  @Override
  public void configure(Context context) {
    batchSize = context.getInteger("batchSize", 100);
    queue = new ArrayBlockingQueue<Event>(batchSize * 100);
  }

  class MyFlusher implements Runnable {

    @Override
    public void run() {
      while (!Thread.interrupted()) {
        try {
          lock.lock();
          int flushed = flush();
          if (flushed == 0) {
            try {
              cond.await(100, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
              break;
            }
          }
          if (flushed > 0 && logger.isDebugEnabled())
            logger.debug("Flushed {}", flushed);
        } finally {
          lock.unlock();
        }
      }
    }
  }
}
