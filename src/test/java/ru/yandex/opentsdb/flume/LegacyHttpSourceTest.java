package ru.yandex.opentsdb.flume;

import com.google.common.collect.Lists;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.ByteArrayRequestEntity;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.flume.Channel;
import org.apache.flume.ChannelSelector;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.Transaction;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.channel.ReplicatingChannelSelector;
import org.apache.flume.conf.Configurables;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;

/**
 * @author Andrey Stepachev
 */
public class LegacyHttpSourceTest {

  public static final String URI = "http://localhost:55661";

  @Test
  public void putOp() throws IOException {
    LegacyHttpSource source = new LegacyHttpSource();
    Channel channel = new MemoryChannel();
    Context context = new Context();

    context.put("port", Integer.toString(55661));
    context.put("keep-alive", "1");
    context.put("capacity", "1000");
    context.put("transactionCapacity", "1000");
    Configurables.configure(source, context);
    Configurables.configure(channel, context);


    ChannelSelector rcs = new ReplicatingChannelSelector();
    rcs.setChannels(Lists.newArrayList(channel));

    source.setChannelProcessor(new ChannelProcessor(rcs));

    source.start();
    final PostMethod post = executePost((
            "{\n" +
                    "   \"hostname/some/check\":[\n" +
                    "    {\"type\":\"numeric\",\"timestamp\":1337258239,\"value\":3.14}\n" +
                    "   ]\n" +
                    "}"
    ).getBytes());
    Assert.assertEquals(post.getStatusCode(), HttpStatus.SC_OK);

    Transaction transaction = channel.getTransaction();
    transaction.begin();
    final LineBasedFrameDecoder.LineEvent take = (LineBasedFrameDecoder.LineEvent) channel.take();
    Assert.assertNotNull(take);
    Assert.assertEquals(
            new String(take.getBody()),
            "put legacy.hostname/some/check 1337258239 3.14 ");

    transaction.commit();
    transaction.close();

    source.stop();

  }

  private GetMethod executeGet() throws IOException {
    HttpClient cli = new HttpClient();
    final GetMethod get = new GetMethod(URI);
    cli.executeMethod(get);
    return get;
  }

  private PostMethod executePost(byte[] body) throws IOException {
    HttpClient cli = new HttpClient();
    final PostMethod post = new PostMethod(URI);
    post.setRequestEntity(new ByteArrayRequestEntity(body));
    cli.executeMethod(post);
    return post;
  }
}
