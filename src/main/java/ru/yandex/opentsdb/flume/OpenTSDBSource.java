package ru.yandex.opentsdb.flume;

import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.CounterGroup;
import org.apache.flume.Event;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.conf.Configurables;
import org.apache.flume.source.AbstractSource;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.string.StringEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author Andrey Stepachev
 */
public class OpenTSDBSource extends AbstractSource
        implements EventDrivenSource, Configurable {

  private static final Logger logger = LoggerFactory
          .getLogger(OpenTSDBSource.class);
  public static final Charset UTF8 = Charset.forName("UTF-8");

  private int batchSize;
  private BlockingQueue<Event> queue;
  private CounterGroup counterGroup = new CounterGroup();
  private String host;
  private int port;
  private Channel nettyChannel;
  private Lock lock = new ReentrantLock();
  private Condition cond = lock.newCondition();

  private byte[] PUT = {'p', 'u', 't'};
  private Thread flushThread;

  public boolean isEvent(Event event) {
    int idx = 0;
    final byte[] body = event.getBody();
    for (byte b : PUT) {
      if (body[idx++] != b)
        return false;
    }
    return true;
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

  class EventHandler extends SimpleChannelHandler {
    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
      Event line = (Event) e.getMessage();
      if (line == null)
        return;
      if (isEvent(line)) {
        try {
          queue.offer(line);
        } catch (ChannelException ex) {
          logger.error("Error putting event to queue, event dropped", ex);
        }
      } else {
        lock.lock();
        try {
          cond.signal();
          e.getChannel().write("ok\n");
          if (logger.isDebugEnabled())
            logger.debug("Waking up flusher");
        } finally {
          lock.unlock();
        }
      }
    }
  }

  @Override
  public void start() {
    org.jboss.netty.channel.ChannelFactory factory = new NioServerSocketChannelFactory(
            Executors.newCachedThreadPool(), Executors.newCachedThreadPool());

    ServerBootstrap serverBootstrap = new ServerBootstrap(factory);
    serverBootstrap.setPipelineFactory(new ChannelPipelineFactory() {
      @Override
      public ChannelPipeline getPipeline() {
        EventHandler handler = new EventHandler();
        final ChannelPipeline pipeline = Channels.pipeline(handler);
        pipeline.addFirst("decoder", new LineBasedFrameDecoder(1024));
        pipeline.addLast("encoder", new StringEncoder(UTF8));
        return pipeline;
      }
    });

    logger.info("OpenTSDB Source starting...");

    if (host == null) {
      nettyChannel = serverBootstrap.bind(new InetSocketAddress(port));
    } else {
      nettyChannel = serverBootstrap.bind(new InetSocketAddress(host, port));
    }
    flushThread = new Thread(new MyFlusher());
    flushThread.setDaemon(false);
    flushThread.start();
    super.start();
  }

  @Override
  public void stop() {
    logger.info("OpenTSDB Source stopping...");
    logger.info("Metrics:{}", counterGroup);

    if (nettyChannel != null) {
      nettyChannel.close();
      try {
        nettyChannel.getCloseFuture().await(60, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        logger.warn("netty server stop interrupted", e);
      } finally {
        nettyChannel = null;
      }
    }

    flushThread.interrupt();
    try {
      flushThread.join();
    } catch (InterruptedException e) {
    }
    while (true) {
      if ((flush(true) == 0)) break;
    }

    super.stop();
  }

  private int flush() {
    return flush(false);
  }

  private int flush(boolean force) {
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
    Configurables.ensureRequiredNonNull(context, "port");
    port = context.getInteger("port");
    host = context.getString("bind");
    batchSize = context.getInteger("batchSize", 100);
    queue = new ArrayBlockingQueue<Event>(batchSize * 100);
  }
}
