package ru.yandex.opentsdb.flume;

import org.apache.flume.Channel;
import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.CounterGroup;
import org.apache.flume.Event;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
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
public class AbstractLineEventSource
        extends AbstractSource
        implements EventDrivenSource, Configurable {

  private static final Logger logger = LoggerFactory
          .getLogger(AbstractLineEventSource.class);

  protected BlockingQueue<LineBasedFrameDecoder.LineEvent> queue;
  protected CounterGroup counterGroup = new CounterGroup();
  private Lock lock = new ReentrantLock();
  private Condition cond = lock.newCondition();
  private Thread flushThread;
  private int batchSize;
  private volatile boolean closed = false;

  private synchronized int flush() {
    return flush(false);
  }

  protected synchronized int flush(boolean force) {
    boolean slow = false;
    final List<Event> list = new ArrayList<Event>();
    final int drained = queue.drainTo(list, batchSize);
    if (drained == 0)
      return drained;
    logger.debug("Events taken from queue " + drained);

    while (!closed && list.size() > 0) {
      try {
        if (slow) {
          logger.debug("Slow insert of " + list.size() + " events");
          final Iterator<Event> it = list.iterator();
          while (it.hasNext()) {
            getChannelProcessor().processEvent(it.next());
            it.remove();
          }
        } else {
          logger.debug("Bulk insert " + list.size() + " events");
          getChannelProcessor().processEventBatch(list);
        }
        list.clear();
      } catch (ChannelException fce) {
        if (force) {
          logger.error("Forced to flush, but we've lost " + list.size() +
                  " events, channel don't accepts data:" + fce.getMessage());
          list.clear();
        } else {
          dropChannelsHead(list.size());
          if (!slow)
            slow = true;
        }
      }
    }
    return drained;
  }

  private void dropChannelsHead(int toDrop) {
    final List<Channel> allChannels = getChannelProcessor().getSelector().getAllChannels();
    logger.info("Draining channels:" + allChannels.toString());
    for (Channel channel : allChannels) {
      final Transaction tx = channel.getTransaction();
      tx.begin();
      try {
        int dropped = 0;
        while (toDrop-- > 0) {
          final Event take = channel.take();
          if (take == null)
            break;
          dropped++;
        }
        logger.info(channel.toString() + " dropped " + dropped + " events");
        tx.commit();
      } catch (Exception e) {
        tx.rollback();
        logger.error("Drop channel head failed", e);
      } finally {
        tx.close();
      }
    }
  }

  void offer(LineBasedFrameDecoder.LineEvent e) {
    queue.offer(e);
  }

  @Override
  public synchronized void start() {
    flushThread = new Thread(new MyFlusher());
    flushThread.setDaemon(false);
    flushThread.start();
    super.start();
  }

  @Override
  public synchronized void stop() {
    super.stop();
    if (!closed) {
      closed = true;
      flushThread.interrupt();
      try {
        flushThread.join();
      } catch (InterruptedException e) {
      }
      while (true) {
        if ((flush(true) == 0)) break;
      }
    }
  }

  @Override
  public void configure(Context context) {
    batchSize = context.getInteger("batchSize", 100);
    queue = new ArrayBlockingQueue<LineBasedFrameDecoder.LineEvent>(batchSize * 100);
  }

  protected void signalWaiters() throws InterruptedException {
    if (!lock.tryLock(1, TimeUnit.MILLISECONDS))
      return;
    try {
      cond.signal();
    } finally {
      lock.unlock();
    }
  }

  class MyFlusher implements Runnable {

    @Override
    public void run() {
      while (!closed) {
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
      logger.info("Flusher stopped");
    }
  }
}
