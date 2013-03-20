package ru.yandex.opentsdb.flume;

import org.apache.flume.Context;
import org.apache.flume.conf.Configurables;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.buffer.ChannelBufferInputStream;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpChunkAggregator;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpRequestDecoder;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseEncoder;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.HttpServerCodec;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * This source handles json requests in form of
 * <pre>
 *   {
 *        "hostname/some/check":[
 *          {"type":"numeric","timestamp":1337258239,"value":3.14},
 *            ...
 *        ],
 *        ...
 *   }
 * </pre>
 *
 * @author Andrey Stepachev
 */
public class LegacyHttpSource extends AbstractLineEventSource {

  private static final Logger logger = LoggerFactory
          .getLogger(LegacyHttpSource.class);

  private String host;
  private int port;
  private Channel nettyChannel;
  private JsonFactory jsonFactory = new JsonFactory();

  class EventHandler extends SimpleChannelHandler {

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
      HttpResponse response;
      try {
        final HttpRequest req = (HttpRequest) e.getMessage();
        if (req.getMethod().equals(HttpMethod.POST))
          response = doPost(ctx, e, req);
        else if (req.getMethod().equals(HttpMethod.GET))
          response = doGet(ctx, e, req);
        else
          response = new DefaultHttpResponse(
                  HttpVersion.HTTP_1_1,
                  HttpResponseStatus.BAD_REQUEST);
      } catch (Exception ex) {
        response = new DefaultHttpResponse(
                HttpVersion.HTTP_1_1,
                HttpResponseStatus.INTERNAL_SERVER_ERROR);
        response.setContent(
                ChannelBuffers.copiedBuffer(ex.getMessage().getBytes()));
      }
      e.getChannel().write(response).addListener(new ChannelFutureListener() {
        public void operationComplete(ChannelFuture future) throws Exception {
          future.getChannel().close();
        }
      });
    }

    private HttpResponse doGet(ChannelHandlerContext ctx, MessageEvent e, HttpRequest req)
            throws IOException {
      HttpResponse response = new DefaultHttpResponse(
              HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
      return response;

    }

    /*
     *       "hostname/some/check":[
     *          {"type":"numeric","timestamp":1337258239,"value":3.14},
     *            ...
     *        ],
     */
    private void parseMetric(String metric, JsonParser parser) throws IOException {
      nextObj:
      while (parser.nextToken() != JsonToken.END_ARRAY) {
        assert parser.getCurrentToken().equals(JsonToken.START_OBJECT);
        parser.nextToken();

        String type = null;
        Long timestamp = null;
        Double value = null;
        while (parser.nextToken() != JsonToken.END_OBJECT) {
          final String currentName = parser.getCurrentName();
          if (currentName.equals("type")) {
            type = parser.getText();
          } else if (currentName.equals("timestamp")) {
            parser.nextValue();
            timestamp = parser.getLongValue();
          } else if (currentName.equals("value")) {
            final JsonToken token = parser.nextToken();
            switch (token) {
              case VALUE_NUMBER_FLOAT:
                value = parser.getDoubleValue();
                break;
              case VALUE_NUMBER_INT:
                value = (double) parser.getLongValue();
                break;
              default:
                // unknown point encountered, skip it as a whole
                parser.skipChildren();
                continue nextObj;
            }
          }
        } // end while(parser.nextToken() != JsonToken.END_OBJECT))
        if (type == null || timestamp == null)
          throw new IllegalArgumentException("Metric " + metric + " expected " +
                  "to have 'type','timestamp' and 'value' at line "
                  + parser.getCurrentLocation().getLineNr());

        if (type.equals("numeric")) {
          if (value == null)
            throw new IllegalArgumentException("Metric " + metric + " expected " +
                    "to have 'type','timestamp' and 'value' at line "
                    + parser.getCurrentLocation().getLineNr());
          addNumericMetric(metric, timestamp, value);
        }
        // ignore unknown format
      }

    }

    private void addNumericMetric(String metric, Long timestamp, Double value)
            throws IOException {

      final ByteArrayOutputStream ba = new ByteArrayOutputStream();
      final OutputStreamWriter writer = new OutputStreamWriter(ba);
      writer.write("put");
      writer.write(" ");
      writer.write("legacy.");
      writer.write(metric);
      writer.write(" ");
      writer.write(timestamp.toString());
      writer.write(" ");
      writer.write(value.toString());
      writer.write(" ");
      writer.flush();
      queue.add(new LineBasedFrameDecoder.LineEvent(ba.toByteArray()));
    }

    private HttpResponse doPost(ChannelHandlerContext ctx, MessageEvent e, HttpRequest req)
            throws IOException {
      final JsonParser parser = jsonFactory.createJsonParser(
              new ChannelBufferInputStream(
                      req.getContent()));
      int cnt = 0;
      while (parser.nextToken() != JsonToken.END_OBJECT) {
        parser.nextToken();
        final String metric = parser.getCurrentName();
        if (parser.nextToken() != JsonToken.START_ARRAY)
          throw new IllegalArgumentException(
                  "Metric " + metric + " should be an 'name':[array] at line "
                          + parser.getCurrentLocation().getLineNr());
        parseMetric(metric, parser);
        cnt++;
      }
      HttpResponse response = new DefaultHttpResponse(
              HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
      response.setContent(ChannelBuffers.copiedBuffer(
              ("Seen " + cnt + "events").getBytes()
      ));
      return response;
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

    ServerBootstrap bootstrap = new ServerBootstrap(factory);
    bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
      public ChannelPipeline getPipeline() throws Exception {
        final ChannelPipeline pipeline = Channels.pipeline(new HttpServerCodec());
        pipeline.addLast("decoder", new HttpRequestDecoder());
        pipeline.addLast("aggregator", new HttpChunkAggregator(1048576));
        pipeline.addLast("encoder", new HttpResponseEncoder());
        pipeline.addLast("handler", new EventHandler());
        return pipeline;
      }
    });
    bootstrap.setOption("child.tcpNoDelay", true);
    bootstrap.setOption("child.keepAlive", true);
    logger.info("HTTP Source starting...");

    if (host == null) {
      nettyChannel = bootstrap.bind(new InetSocketAddress(port));
    } else {
      nettyChannel = bootstrap.bind(new InetSocketAddress(host, port));
    }
    flushThread = new Thread(new MyFlusher());
    flushThread.setDaemon(false);
    flushThread.start();
    super.start();
  }

  @Override
  public void stop() {
    logger.info("HTTP Source stopping...");
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
