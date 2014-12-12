package net.sitemorph.queue;

/**
 * As the task dispatcher may run for a very long time it must repeatedly
 * connect to the queue on each cycle rather than hold a connection. To allow
 * it to do this, this factory interface allows retreival of a queue as well
 * as a shutdown callback when the dispatcher is done.
 *
 * @author damien@sitemorph.net
 */
public interface TaskQueueFactory {

  public TaskQueue getTaskQueue() throws QueueException;

  /**
   * Attempt to return any queue requested.
   * @param queue if one was received. May be null if called due to an error
   *              caused by this factory's get task queue call.
   */
  public void returnTaskQueue(TaskQueue queue) throws QueueException;
}
