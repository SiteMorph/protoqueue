package net.sitemorph.queue;

import net.sitemorph.protostore.CrudIterator;
import net.sitemorph.queue.Message.Task;

import java.util.UUID;

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
   * Claim the current item on the top of the queue. It will read the first
   * non-claimed task, as such this supports multiple consumer dispatchers to
   * claim tasks.
   *
   * The claim will instantiate a claim on the task that it returns.
   *
   * Note that only overdue tasks will be returned.
   *
   * @param identity of the claimer
   * @param now is the current time
   * @param claimTimeout epoc time after which the claim should be considered
   *                     invalid.
   * @return the current task. Returns null if nothing present in the queue.
   */
  public Task claim(UUID identity, long now,
      long claimTimeout) throws QueueException;

  /**
   * Release a claimed task on failure or other scenario where others may then
   * claim the task
   *
   * @param task which has been claimed using the claim method.
   * @throws StaleClaimException when the claim is out of date.
   */
  public void release(Task task) throws QueueException, StaleClaimException;

  /**
   * Push an item onto the queue. Note that the time will be set if it has not
   * been.
   *
   * @param task to add to the queue.
   * @return the constructed task (with urn)
   */
  public Task push(Task.Builder task) throws QueueException;

  /**
   * Remove a task from the queue. This method will fail if the current vector
   * is out of date or
   *
   * @param task to remove from the queue
   */
  public void remove(Task task) throws QueueException;

  /**
   * Internal method for queue inspection operations returning the entire queue.
   * @return all tasks
   */
  CrudIterator<Task> tasks() throws QueueException;

  /**
   * Called to close the task queue and release it's resources.
   * @throws QueueException
   */
  public void close() throws QueueException;
}
