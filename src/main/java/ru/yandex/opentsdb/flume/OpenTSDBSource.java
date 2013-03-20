package ru.yandex.opentsdb.flume;

import com.google.common.base.Charsets;
import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.conf.Configurables;
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
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * @author Andrey Stepachev
 */
public class OpenTSDBSource extends AbstractLineEventSource {

  private static final Logger logger = LoggerFactory
          .getLogger(OpenTSDBSource.class);

  private String host;
  private int port;
  private Channel nettyChannel;

  private byte[] PUT = {'p', 'u', 't'};

  public boolean isEvent(Event event) {
    int idx = 0;
    final byte[] body = event.getBody();
    for (byte b : PUT) {
      if (body[idx++] != b) {
        return false;
      }
    }
    return true;
  }

  class EventHandler extends SimpleChannelHandler {
    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
      LineBasedFrameDecoder.LineEvent line = (LineBasedFrameDecoder.LineEvent) e.getMessage();
      if (line == null) {
        return;
      }
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
          if (logger.isDebugEnabled()) {
            logger.debug("Waking up flusher");
          }
        } finally {
          lock.unlock();
        }
      }
    }
  }

  @Override
  public void configure(Context context) {
    super.configure(context);
    Configurables.ensureRequiredNonNull(context, "port");
    port = context.getInteger("port");
    host = context.getString("bind");
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
        pipeline.addLast("encoder", new StringEncoder(Charsets.UTF_8));
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

}
