package ru.yandex.opentsdb.flume;

import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.junit.rules.ExternalResource;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

/**
* @author Andrey Stepachev
*/
public class HttpServerInterceptor extends ExternalResource {
  private InetSocketAddress address;

  private HttpServer httpServer;

  public HttpServerInterceptor(final int port) {
    try {
      this.address = new InetSocketAddress(InetAddress.getLocalHost(), port);
    } catch (UnknownHostException e) {
      throw new RuntimeException(e);
    }
  }

  protected final void before() throws Throwable {
    super.before();
    httpServer = HttpServer.create(address, 0);
    httpServer.start();
  }

  protected final void after() {
    httpServer.stop(0);
    super.after();
  }

  public void addHandler(String path, HttpHandler handler) {
    httpServer.createContext(path, handler);
  }

  public InetSocketAddress getAddress() {
    return address;
  }
}
