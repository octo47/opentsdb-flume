package ru.yandex.opentsdb.flume;

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
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
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExternalResource;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Andrey Stepachev
 */
public class OpenTSDBSourceTest {

  public static final int CHANNEL_SIZE = 10;

  @Rule
  public SourceInterceptor source =
          new SourceInterceptor(8887);


  @Test
  public void testChannelFail() throws Exception {


    int eventsAdded = doInTx(new Function<Transaction, Integer>() {
      @Override
      public Integer apply(@Nullable Transaction input) {
        // fill up buffer
        for (int i = 0; i < CHANNEL_SIZE; i++) {
          source.channel.put(makeSeqEvent());
        }
        return CHANNEL_SIZE;
      }
    });

    // put additional event, so flushure will overwhelm channel capacity
    source.source.offer(makeSeqEvent());


    // give some time to flusher
    Thread.sleep(1000);
    List<Event> l = doInTx(new Function<Transaction, List<Event>>() {
      @Override
      public List<Event> apply(@Nullable Transaction input) {
        List<Event> l = Lists.newArrayList();
        Event e;
        while (l.size() < CHANNEL_SIZE && (e = source.channel.take()) != null) {
          l.add(e);
        }
        return l;
      }
    });
    Assert.assertEquals(l.size(), CHANNEL_SIZE);

    Event e = doInTx(new Function<Transaction, Event>() {
      @Override
      public Event apply(@Nullable Transaction input) {
        return source.channel.take();
      }
    });
    Assert.assertNull(e);
  }

  private LineBasedFrameDecoder.LineEvent makeSeqEvent() {
    return new LineBasedFrameDecoder.LineEvent(
            ("put metric.me 123412354 host=fff tag=fff " + source.cnt.incrementAndGet()).getBytes()
    );
  }

  private <R> R doInTx(Function<Transaction, R> function) {
    final Transaction tx = source.channel.getTransaction();
    tx.begin();
    try {
      final R r = function.apply(tx);
      tx.commit();
      return r;
    } catch (Exception e) {
      tx.rollback();
      throw Throwables.propagate(e);
    } finally {
      tx.close();
    }
  }

  public static class SourceInterceptor extends ExternalResource {

    final int port;

    AtomicInteger cnt = new AtomicInteger();
    OpenTSDBSource source;
    Channel channel;
    Context context;
    ChannelSelector rcs;

    public SourceInterceptor(final int port) {
      this.port = port;
    }

    protected final void before() throws Throwable {
      super.before();
      source = new OpenTSDBSource();
      channel = new MemoryChannel();
      context = new Context();

      context.put("port", Integer.toString(port));
      context.put("capacity", Integer.toString(CHANNEL_SIZE));
      context.put("transactionCapacity", Integer.toString(CHANNEL_SIZE));
      context.put("keep-alive", "0");
      context.put("batchSize", Integer.toString(CHANNEL_SIZE));
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

}
