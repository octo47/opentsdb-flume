package ru.yandex.opentsdb.flume;

import org.apache.flume.ChannelException;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.conf.Configurables;
import org.apache.flume.source.AbstractSource;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.*;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.string.StringEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * @author Andrey Stepachev
 */
public class OpenTSDBSource extends AbstractSource
        implements EventDrivenSource, Configurable {

  private static final Logger logger = LoggerFactory
          .getLogger(OpenTSDBSource.class);
  public static final Charset UTF8 = Charset.forName("UTF-8");

  private CounterGroup counterGroup = new CounterGroup();
  private String host;
  private int port;
  private Channel nettyChannel;

  private byte[] PUT = {'p', 'u', 't'};

  public boolean isEvent(Event event) {
    int idx = 0;
    final byte[] body = event.getBody();
    for (byte b : PUT) {
      if (body[idx++] != b)
        return false;
    }
    return true;
  }


  class EventHandler extends SimpleChannelHandler {
    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
      Event line = (Event) e.getMessage();
      if (line == null)
        return;
      if (isEvent(line)) {
        try {
          getChannelProcessor().processEvent(line);
          counterGroup.incrementAndGet("events.success");
        } catch (ChannelException ex) {
          counterGroup.incrementAndGet("events.dropped");
          logger.error("Error writing to channel, event dropped", ex);
        }
      } else {
        e.getChannel().write("ok\n");
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

    super.stop();
  }

  @Override
  public void configure(Context context) {
    Configurables.ensureRequiredNonNull(context, "port");
    port = context.getInteger("port");
    host = context.getString("bind");
  }
}
