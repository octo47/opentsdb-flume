package ru.yandex.opentsdb.flume;

import com.google.common.base.Charsets;
import com.google.common.io.LineReader;
import org.apache.flume.Context;
import org.apache.flume.conf.Configurables;
import org.apache.flume.conf.ConfigurationException;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.buffer.ChannelBufferInputStream;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.http.DefaultHttpRequest;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpChunkAggregator;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpRequestDecoder;
import org.jboss.netty.handler.codec.http.HttpRequestEncoder;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseDecoder;
import org.jboss.netty.handler.codec.http.HttpResponseEncoder;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.HttpServerCodec;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.jboss.netty.handler.codec.http.QueryStringDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
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

  private static final Integer DEFAULT_HTTP_CHUNK_SIZE = 10 * 1024 * 1024;
  private static final Integer DEFAULT_CHILD_BUFFER_SIZE = 1024 * 1024;

  private int maxChunkSize;
  private int childSendBufferSize;
  private int childRecieveBufferSize;
  private String host;
  private int port;
  private Channel nettyChannel;
  private JsonFactory jsonFactory = new JsonFactory();
  private URL tsdbUrl;
  private ExecutorService bossExecutor;
  private ExecutorService workerExecutor;
  private NioClientSocketChannelFactory clientSocketChannelFactory;

  class QueryParams {
    long from;
    long to;
    String key;
    String type;

    public QueryParams(String type, String key, long from, long to) {
      this.type = type;
      this.key = key;
      this.from = from;
      this.to = to;
    }

    /**
     * keep in sync with {@link MetricParser#addNumericMetric}
     */
    public String tsdbQueryParams() {
      String metric = "l." + type + "." + key;
      return "m=sum:" + metric + "&start=" + from + "&end=" + to + "&ascii";
    }

    private String urlEncode(String string) {
      try {
        return URLEncoder.encode(string, "UTF-8");
      } catch (UnsupportedEncodingException e) {
        throw new IllegalStateException(e);
      }
    }
  }


  class QueryHandler extends SimpleChannelHandler {

    final Channel clientChannel;
    final QueryParams query;
    private HttpResponse response;

    QueryHandler(Channel clientChannel, QueryParams query) {
      this.clientChannel = clientChannel;
      this.query = query;
    }

    public HttpResponse getResponse() {
      return response;
    }

    @Override
    public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
      final String query = "/q?" + this.query.tsdbQueryParams();
      logger.debug("Sending query " + query);
      final DefaultHttpRequest req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET,
              query);
      e.getChannel().write(req);
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
      HttpResponse resp = (HttpResponse) e.getMessage();
      if (resp.getStatus().equals(HttpResponseStatus.OK)) {
        response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
        final LineReader lineReader = new LineReader(
                new InputStreamReader(
                        new ChannelBufferInputStream(resp.getContent())));
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        final JsonGenerator jsonGenerator = jsonFactory.createJsonGenerator(outputStream);
        jsonGenerator.writeStartArray();
        String line;
        while ((line = lineReader.readLine()) != null) {
          final String[] split = line.split("\\s+", 4);
          jsonGenerator.writeStartObject();
          jsonGenerator.writeNumberField("timestamp", Long.parseLong(split[1]));
          jsonGenerator.writeNumberField("value", Double.parseDouble(split[2]));
          jsonGenerator.writeEndObject();
        }
        jsonGenerator.writeEndArray();
        jsonGenerator.close();
        response.setContent(ChannelBuffers.wrappedBuffer(outputStream.toByteArray()));
      } else {
        response = new DefaultHttpResponse(
                HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
      }
      e.getChannel().close();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
      logger.error("Exception caugth in channel", e.getCause());
      response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.INTERNAL_SERVER_ERROR);
      e.getChannel().close();
    }
  }

  private void writeResponseAndClose(MessageEvent e, HttpResponse response) {
    writeResponseAndClose(e.getChannel(), response);
  }

  private void writeResponseAndClose(Channel ch, HttpResponse response) {
    ch.write(response).addListener(ChannelFutureListener.CLOSE);
  }

  class EventHandler extends SimpleChannelHandler {

    private LegacyHttpSource.MetricParser metricParser = new MetricParser();

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
      try {
        final HttpRequest req = (HttpRequest) e.getMessage();
        if (req.getMethod().equals(HttpMethod.POST)) {
          doPost(ctx, e, req);
        } else if (req.getMethod().equals(HttpMethod.GET)) {
          doGet(ctx, e, req);
        } else {
          writeResponseAndClose(e, new DefaultHttpResponse(
                  HttpVersion.HTTP_1_1,
                  HttpResponseStatus.BAD_REQUEST));
        }
      } catch (Exception ex) {
        if (logger.isDebugEnabled())
          logger.debug("Failed to process message", ex);
        HttpResponse response = new DefaultHttpResponse(
                HttpVersion.HTTP_1_1,
                HttpResponseStatus.INTERNAL_SERVER_ERROR);
        response.setContent(
                ChannelBuffers.copiedBuffer(ex.getMessage().getBytes()));
        writeResponseAndClose(e, response);
      }
    }


    private void doGet(ChannelHandlerContext ctx, final MessageEvent e, HttpRequest req)
            throws IOException {
      final QueryStringDecoder decoded = new QueryStringDecoder(req.getUri());
      if (!decoded.getPath().equalsIgnoreCase("/read")) {
        writeResponseAndClose(e, new DefaultHttpResponse(
                HttpVersion.HTTP_1_1, HttpResponseStatus.NOT_FOUND));
        return;
      }
      try {
        QueryParams params = parseQueryParameters(decoded);
        final Channel clientChannel = e.getChannel();
        // disable client temporary, until we connect
        clientChannel.setReadable(false);

        ClientBootstrap clientBootstrap = new ClientBootstrap(clientSocketChannelFactory);
        final ChannelPipeline pipeline = clientBootstrap.getPipeline();
        pipeline.addLast("decoder", new HttpResponseDecoder());
        pipeline.addLast("aggregator", new HttpChunkAggregator(1048576));
        pipeline.addLast("encoder", new HttpRequestEncoder());
        final QueryHandler queryHandler = new QueryHandler(clientChannel, params);
        pipeline.addLast("query", queryHandler);

        logger.debug("Making connection to: " + tsdbUrl);

        ChannelFuture tsdbChannelFuture = clientBootstrap.connect(new InetSocketAddress(
                tsdbUrl.getHost(),
                tsdbUrl.getPort()));

        tsdbChannelFuture.addListener(new ChannelFutureListener() {
          public void operationComplete(ChannelFuture future)
                  throws Exception {
            if (future.isSuccess()) {
              future.getChannel().getCloseFuture().addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) throws Exception {
                  writeResponseAndClose(clientChannel, queryHandler.getResponse());
                }
              });
            } else {
              logger.error("Connect failed", future.getCause());
              final DefaultHttpResponse response =
                      new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.BAD_GATEWAY);
              clientChannel
                      .write(response)
                      .addListener(ChannelFutureListener.CLOSE);
            }
          }
        });

      } catch (IllegalArgumentException iea) {
        final DefaultHttpResponse response =
                new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.BAD_REQUEST);
        response.setContent(
                ChannelBuffers.copiedBuffer(iea.getMessage().getBytes(Charsets.UTF_8)));
        writeResponseAndClose(e, response);
        return;
      }


    }


    private void doPost(ChannelHandlerContext ctx, MessageEvent e, HttpRequest req)
            throws IOException {

      final QueryStringDecoder decoded = new QueryStringDecoder(req.getUri());
      if (!decoded.getPath().equalsIgnoreCase("/write")) {
        writeResponseAndClose(e,
                new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.NOT_FOUND));
        return;
      }

      try {
        metricParser.parse(req);
      } catch (IllegalArgumentException iae) {
        logger.warn("Metric parser failed: " + iae.getMessage());
      }

      HttpResponse response = new DefaultHttpResponse(
              HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
      response.setContent(ChannelBuffers.copiedBuffer(
              ("Seen events").getBytes()
      ));
      writeResponseAndClose(e, response);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
      e.getChannel().close();
    }

    private QueryParams parseQueryParameters(QueryStringDecoder decoded) {
      final Map<String, List<String>> parameters = decoded.getParameters();
      return new QueryParams(
              safeOneValue(parameters, "type"),
              safeOneValue(parameters, "key"),
              Long.parseLong(safeOneValue(parameters, "from")),
              Long.parseLong(safeOneValue(parameters, "to"))
      );
    }

    private String safeOneValue(Map<String, List<String>> params, String key) {
      return safeOneValue(params, key, null);
    }

    private String safeOneValue(Map<String, List<String>> params, String key, String defaultValue) {
      final List<String> strings = params.get(key);
      if (strings != null)
        for (String string : strings) {
          if (string.trim().length() > 0)
            return string;
        }

      if (defaultValue == null)
        throw new IllegalArgumentException("Parameter " + key + " not found");
      else
        return defaultValue;
    }

  }

  class MetricParser {


    public void parse(HttpRequest req) throws IOException {

      final JsonParser parser = jsonFactory.createJsonParser(
              new ChannelBufferInputStream(req.getContent()));

      parser.nextToken(); // Skip the wrapper

      while (parser.nextToken() != JsonToken.END_OBJECT) {

        final String metric = parser.getCurrentName();

        JsonToken currentToken = parser.nextToken();
        if (currentToken == JsonToken.START_OBJECT) {
          parseMetricObject(metric, parser);
        } else if (currentToken == JsonToken.START_ARRAY) {
          int illegalTokens = parseMetricArray(metric, parser);
          if(illegalTokens > 0) {
              logger.warn("{} illegal tokens encountered", illegalTokens);
          }
        } else {
          logger.warn("Illegal token: expected {} or {}, but was {}: {}",new Object[] {
                  JsonToken.START_OBJECT, JsonToken.START_ARRAY, currentToken, parser.getText()});
        }
      }
    }

    /*
     *       "hostname/some/check":[
     *          {"type":"numeric","timestamp":1337258239,"value":3.14},
     *            ...
     *        ],
     */
    private int parseMetricArray(String metric, JsonParser parser) throws IOException {
      JsonToken currentToken;
      int illegalTokens = 0;

      while ((currentToken = parser.nextToken()) != JsonToken.END_ARRAY) {

        if(!currentToken.equals(JsonToken.START_OBJECT)) {
            logger.warn("Illegal token: expected {}, but was {}: {}",
                    new Object[] {JsonToken.START_OBJECT, currentToken, parser.getText()});
            illegalTokens++;
        } else {
            parseMetricObject(metric, parser);
        }

      }

      return illegalTokens;
    }

    private void parseMetricObject(String metric, JsonParser parser) throws IOException {
      String type = null;
      Long timestamp = null;
      Double value = null;
      while (parser.nextToken() != JsonToken.END_OBJECT) {
        final String currentName = parser.getCurrentName();
        if (currentName.equals("type")) {
          type = parser.getText();
        } else if (currentName.equals("timestamp")) {
          final JsonToken token = parser.nextToken();
          switch (token) {
            case VALUE_NUMBER_INT:
              timestamp = parser.getLongValue();
              break;
            default:
              throw new IllegalArgumentException("timestamp should be numeric"
                      + parser.getCurrentLocation().getLineNr());
          }
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
              return;
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
        addNumericMetric(metric, type, timestamp, value);
      }
    }

    private void addNumericMetric(String metric, String type, Long timestamp, Double value)
            throws IOException {

      final ByteArrayOutputStream ba = new ByteArrayOutputStream();
      final OutputStreamWriter writer = new OutputStreamWriter(ba);
      writer.write("put");
      writer.write(" ");
      writer.write("l.");
      writer.write(type);
      writer.write(".");
      writer.write(metric);
      writer.write(" ");
      writer.write(timestamp.toString());
      writer.write(" ");
      writer.write(value.toString());
      writer.write(" ");
      writer.write("legacy=true");
      writer.flush();
      queue.add(ba.toByteArray());
    }

  }

  @Override
  public void configure(Context context) {
    super.configure(context);
    Configurables.ensureRequiredNonNull(context, "port");
    port = context.getInteger("port");
    host = context.getString("bind");

    maxChunkSize = context.getInteger("netty.max.http.chunk.size", DEFAULT_HTTP_CHUNK_SIZE);

    childSendBufferSize = context.getInteger("netty.child.sendBufferSize",
            DEFAULT_CHILD_BUFFER_SIZE);
    childRecieveBufferSize = context.getInteger("netty.child.recieveBufferSize",
            DEFAULT_CHILD_BUFFER_SIZE);

    try {
      tsdbUrl = new URL(context.getString("tsdb.url"));
    } catch (MalformedURLException e) {
      throw new ConfigurationException("tsdb.url", e);
    }
  }


  @Override
  public void start() {

    // Start the connection attempt.
    bossExecutor = Executors.newCachedThreadPool();
    workerExecutor = Executors.newCachedThreadPool();
    clientSocketChannelFactory = new NioClientSocketChannelFactory(
            bossExecutor, workerExecutor, 2);

    org.jboss.netty.channel.ChannelFactory factory = new NioServerSocketChannelFactory(
            bossExecutor, workerExecutor);

    ServerBootstrap bootstrap = new ServerBootstrap(factory);
    bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
      public ChannelPipeline getPipeline() throws Exception {
        final ChannelPipeline pipeline = Channels.pipeline(new HttpServerCodec());
        pipeline.addLast("decoder", new HttpRequestDecoder());
        pipeline.addLast("aggregator", new HttpChunkAggregator(maxChunkSize));
        pipeline.addLast("encoder", new HttpResponseEncoder());
        pipeline.addLast("handler", new EventHandler());
        return pipeline;
      }
    });
    bootstrap.setOption("child.tcpNoDelay", true);
    bootstrap.setOption("child.keepAlive", true);
    bootstrap.setOption("child.sendBufferSize", childSendBufferSize);
    bootstrap.setOption("child.receiveBufferSize", childRecieveBufferSize);
    logger.info("HTTP Source starting...");

    if (host == null) {
      nettyChannel = bootstrap.bind(new InetSocketAddress(port));
    } else {
      nettyChannel = bootstrap.bind(new InetSocketAddress(host, port));
    }
    super.start();
  }

  @Override
  public void stop() {
    super.stop();
    logger.info("HTTP Source stopping...");

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


    bossExecutor.shutdown();
    workerExecutor.shutdown();
  }

}
