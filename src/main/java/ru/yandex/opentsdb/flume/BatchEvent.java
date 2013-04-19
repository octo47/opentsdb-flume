package ru.yandex.opentsdb.flume;

import com.google.common.collect.Maps;
import com.google.common.io.ByteArrayDataInput;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import org.apache.flume.Event;
import org.xerial.snappy.SnappyCodec;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * @author Andrey Stepachev
 */
public class BatchEvent implements Event, Iterable<byte[]> {

  private static final Map<String, String> EMPTY_MAP =
          Collections.unmodifiableMap(Maps.<String, String>newHashMap());
  public static final int MAX_MESSAGE_SIZE = 64 * 1024 * 1024;

  private Map<String, String> headers = EMPTY_MAP;
  private byte[] body;

  public BatchEvent(byte[] body) {
    this.body = body;
  }

  public static BatchEvent encodeBatch(List<byte[]> list) {
    try {
      final ByteArrayOutputStream ba = new ByteArrayOutputStream();
      final DataOutputStream bodyOutput = new DataOutputStream(new GZIPOutputStream(ba));
      bodyOutput.writeInt(list.size());
      for (byte[] event : list) {
        bodyOutput.writeInt(event.length);
        bodyOutput.write(event);
      }
      bodyOutput.flush();
      bodyOutput.close();
      return new BatchEvent(ba.toByteArray());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public Map<String, String> getHeaders() {
    return headers;
  }

  public void setHeaders(Map<String, String> headers) {
    this.headers = headers;
  }

  public byte[] getBody() {
    return body;
  }

  public void setBody(byte[] body) {
    this.body = body;
  }

  @Override
  public Iterator<byte[]> iterator() {
    try {
      return new Iterator<byte[]>() {

        final ByteArrayDataInput stream =
                ByteStreams.newDataInput(
                        ByteStreams.toByteArray(
                                new GZIPInputStream(new ByteArrayInputStream(body))));
        int messages = stream.readInt();

        @Override
        public boolean hasNext() {
          return messages > 0;
        }

        @Override
        public byte[] next() {
          if (messages <= 0)
            throw new IndexOutOfBoundsException();
          messages--;
          final int size = stream.readInt();
          if (size > MAX_MESSAGE_SIZE)
            throw new IllegalStateException("Message too big: " + MAX_MESSAGE_SIZE + " bytes allowed");
          byte[] body = new byte[size];
          stream.readFully(body);
          return body;
        }

        @Override
        public void remove() {
          throw new UnsupportedOperationException();
        }
      };
    } catch (IOException e) {
      throw new IllegalArgumentException(e);
    }
  }
}
