package ru.yandex.opentsdb.flume;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.sun.xml.internal.messaging.saaj.packaging.mime.util.LineInputStream;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.List;

/**
 * Sink, capable to do writes into opentsdb.
 * This version expects full command in event body.
 * @author Andrey Stepachev
 */
public class OpenTSDBSink extends AbstractSink implements Configurable {
  public static final byte[] LF = "\n".getBytes();
  public static final byte[] VER_CMD = "ver\n".getBytes();

  private final Logger log = LoggerFactory.getLogger(OpenTSDBSink.class);

  private SinkCounter sinkCounter;
  private InetSocketAddress[] endpoints;
  private int batchSize;
  private int sendBufSize;
  private int timeout;

  @Override
  public Status process() throws EventDeliveryException {
    Status result = Status.READY;
    Channel channel = getChannel();
    Transaction transaction = channel.getTransaction();
    Socket sock = null;
    Event event = null;
    ByteArrayOutputStream buf = new ByteArrayOutputStream(sendBufSize);

    try {
      transaction.begin();
      int batch = 0;
      while (batch < batchSize && (event = channel.take()) != null) {
        buf.write(event.getBody());
        buf.write(LF);
        batch++;
      }
      final byte[] bytes = buf.toByteArray();
      if (batch == 0) {
        result = Status.BACKOFF;
      } else {
        sock = mkConn();
        final OutputStream so = sock.getOutputStream();
        so.write(bytes);
        so.write(VER_CMD);
        // actually, we don't bother with result
        new LineInputStream(sock.getInputStream()).readLine();
      }
      transaction.commit();
    } catch (Exception ex) {
      transaction.rollback();
      throw new EventDeliveryException("Failed to log event: " + event, ex);
    } finally {
      transaction.close();
      if (sock != null && sock.isConnected()) {
        sinkCounter.incrementConnectionClosedCount();
        try {
          sock.close();
        } catch (IOException e) {
          throw new ChannelException(e);
        }
      }
    }

    return result;
  }

  @Override
  public void configure(Context context) {
    sinkCounter = new SinkCounter(getName());
    batchSize = context.getInteger("batchSize", 100);
    timeout = context.getInteger("timeout", 10000);
    sendBufSize = context.getInteger("sendBufSize", 2048 * 1024);
    initEndpoints(context);
  }


  private Socket mkConn() {
    try {
      final InetSocketAddress ep = nextEndpoint();
      log.info("Connecting to " + ep);
      final Socket socket = new Socket();
      socket.setTcpNoDelay(false);
      socket.setSendBufferSize(sendBufSize);
      socket.connect(ep, timeout);
      log.info("Connected to " + ep);
      sinkCounter.incrementConnectionCreatedCount();
      return socket;
    } catch (IOException e) {
      sinkCounter.incrementConnectionFailedCount();
      throw new ChannelException(e);
    }
  }

  private InetSocketAddress nextEndpoint() {

    final InetSocketAddress b = endpoints[0];
    System.arraycopy(endpoints, 1, endpoints, 0, endpoints.length - 1);
    endpoints[endpoints.length - 1] = b;
    return b;
  }

  private void initEndpoints(Context context) {

    String endpointsStr = context.getString("endpoints");

    Preconditions.checkState(endpointsStr != null, "No endpoints specified");
    final String[] splitEndpoints = endpointsStr.split(",");
    final List<InetSocketAddress> list = Lists.newArrayList();
    for (String endpoint : splitEndpoints) {
      final String[] hp = endpoint.split(":");
      if (hp[0].trim().length() == 0)
        continue;
      try {
        list.add(new InetSocketAddress(
                InetAddress.getByName(hp[0]), Short.parseShort(hp[1])));
      } catch (UnknownHostException e) {
        Preconditions.checkState(false, "Wrong host specified:" + endpoint);
      }
    }
    Collections.shuffle(list);
    endpoints = list.toArray(new InetSocketAddress[list.size()]);
  }
}
