package ru.yandex.opentsdb.flume;

import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.ByteArrayRequestEntity;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.params.HttpClientParams;
import org.apache.flume.Channel;
import org.apache.flume.ChannelSelector;
import org.apache.flume.Context;
import org.apache.flume.Transaction;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.channel.ReplicatingChannelSelector;
import org.apache.flume.conf.Configurables;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExternalResource;

import java.io.File;
import java.io.IOException;
import java.net.HttpURLConnection;

/**
 * @author Andrey Stepachev
 */
public class LegacyHttpSourceTest {

  public static final String URI = "http://localhost";

  @Rule
  public HttpServerInterceptor httpServer = new HttpServerInterceptor(8888);

  @Rule
  public SourceInterceptor source =
          new SourceInterceptor(8887, httpServer.getAddress().getHostName() + ":" + httpServer.getAddress().getPort());

  String[] testRequests = new String[]{
          "{\"TESTHOST/nobus/test\": [{\"type\": \"numeric\", \"timestamp\": 1364451167, " +
                  "\"value\": 3.14}]}",
          "{\"TESTHOST/nobus/test\": [{\"timestamp\": 1364451167, \"type\": \"numeric\", " +
                  "\"value\": 3.14}]}",
          "{\"TESTHOST/nobus/test\": [{" +
                  "\"value\": 3.14, \"timestamp\": 1364451167, \"type\": \"numeric\" }]}",
          "{\"TESTHOST/nobus/test\": {" +
                  "\"value\": 3.14, \"timestamp\": 1364451167, \"type\": \"numeric\" }}"
  };

  @Test
  public void postMethod() throws IOException, InterruptedException {
    Transaction transaction = source.channel.getTransaction();
    transaction.begin();
    for (String testRequest : testRequests) {
      final PostMethod post = executePost("/write", (
              testRequest
      ).getBytes());
      Assert.assertEquals(testRequest, post.getStatusCode(), HttpStatus.SC_OK);
    }
    transaction.commit();
    transaction.close();
    Thread.sleep(500);
    final Transaction tx2 = source.channel.getTransaction();
    tx2.begin();
    final BatchEvent take =
            (BatchEvent) source.channel.take();
    Assert.assertNotNull(take);
    Assert.assertEquals(
            new String(take.iterator().next()),
            "put l.numeric.TESTHOST/nobus/test 1364451167 3.14 legacy=true");
    tx2.commit();
    tx2.close();;
  }

  @Test
  public void getMethod() throws IOException, InterruptedException {
    httpServer.addHandler("/q", new HttpHandler() {
      @Override
      public void handle(HttpExchange exchange) throws IOException {
        final byte[] bytes = readBytes("reply1.txt");
        exchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, bytes.length);
        exchange.getResponseBody().write(bytes);
        exchange.close();
      }

    });
    final GetMethod get = executeGet("/read", "type=numeric&key=/host/load/avg15&from=1334620800&to=1334707200");
    Assert.assertEquals(get.getStatusCode(), HttpStatus.SC_OK);
  }


  private GetMethod executeGet(String uri, String args)
          throws IOException {
    HttpClient cli = new HttpClient();
    final GetMethod get = new GetMethod(URI + ":" + source.port + uri + "?" + args);
    get.setParams(new HttpClientParams());
    cli.executeMethod(get);
    System.out.println(get.getResponseBodyAsString());
    return get;
  }

  private PostMethod executePost(String uri, byte[] body) throws IOException {
    HttpClient cli = new HttpClient();
    final PostMethod post = new PostMethod(URI + ":" + source.port + uri);
    post.setRequestEntity(new ByteArrayRequestEntity(body));
    cli.executeMethod(post);
    return post;
  }

  public static class SourceInterceptor extends ExternalResource {

    final int port;
    final String tsdbHostPort;

    LegacyHttpSource source;
    Channel channel;
    Context context;
    ChannelSelector rcs;

    public SourceInterceptor(final int port, String tsdbHostPort) {
      this.port = port;
      this.tsdbHostPort = tsdbHostPort;
    }

    protected final void before() throws Throwable {
      super.before();
      source = new LegacyHttpSource();
      channel = new MemoryChannel();
      context = new Context();

      context.put("port", Integer.toString(port));
      context.put("tsdb.url", "http://" + tsdbHostPort);
      context.put("keep-alive", "1");
      context.put("capacity", "1000");
      context.put("transactionCapacity", "1000");
      Configurables.configure(source, context);
      Configurables.configure(channel, context);


      rcs = new ReplicatingChannelSelector();
      rcs.setChannels(Lists.newArrayList(channel));

      source.setChannelProcessor(new ChannelProcessor(rcs));

      source.start();
    }

    protected final void after() {
      source.stop();
      channel.stop();
      super.after();
    }
  }

  private byte[] readBytes(String fileName) throws IOException {
    return Files.toByteArray(
            new File(this.getClass().getResource("reply1.txt").getFile()));
  }

}
