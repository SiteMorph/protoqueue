package net.sitemorph.queue;

/**
 * Exception used to signify to a queue orchestrator that a task that they had
 * claimed has been claimed by another due to a timeout.
 *
 * @author dmaien@sitemorph.net
 */
public class StaleClaimException extends QueueException {
  public StaleClaimException(String message, Throwable throwable) {
    super(message, throwable);
  }

  public StaleClaimException(String message) {
    super(message);
  }
}
