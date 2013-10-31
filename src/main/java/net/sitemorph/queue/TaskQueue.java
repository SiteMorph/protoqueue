package net.sitemorph.queue;

import net.sitemorph.protostore.CrudIterator;
import net.sitemorph.queue.Message.Task;

/**
 * A general purpose queue implementation which always queues tasks and skips
 * reflection etc. Typed queues should be implemented by using contained data
 * from the task queue name plus the data payload. The task queue uses the task
 * timestamp ordering to prioritise the queue.
 *
 * @author damien@sitemorph.net
 */
public interface TaskQueue {

  /**
   * Peek at the current item on the top of the queue. This method is
   * idempotent as it has no side effects.
   *
   * @return the current task. Returns null if nothing present in the queue.
   */
  public Task peek() throws QueueException;

  /**
   * Pop the current item from the queue returning it.
   *
   * @return the old top of the queue. Returns null on queue empty.
   */
  public Task pop() throws QueueException;

  /**
   * Push an item onto the queue. Note that the time will be set if it has not
   * been.
   *
   * @param task to add to the queue.
   * @return the constructed task (with urn)
   */
  public Task push(Task.Builder task) throws QueueException;

  /**
   * Remove a task from the queue.
   *
   * @param task to remove from the queue
   */
  public void remove(Task task) throws QueueException;

  /**
   * Internal method for queue inspection operations returning the entire queue.
   * @return all tasks
   */
  CrudIterator<Task> tasks() throws QueueException;

  public void close() throws QueueException;
}
