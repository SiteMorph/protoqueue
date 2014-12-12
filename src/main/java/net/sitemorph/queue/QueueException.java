package net.sitemorph.queue;

/**
 * Exception used by queue to wrap up underlying errors.
 *
 * @author damien@sitemorph.net
 */
public class QueueException extends Exception {

  public QueueException(String message, Throwable throwable) {
    super(message, throwable);
  }

  public QueueException(String message) {
    super(message);
  }
}
