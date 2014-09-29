package net.sitemorph.queue;

import net.sitemorph.protostore.CrudStore;
import net.sitemorph.queue.Message.Task;

/**
 * The task picker looks at the current queue and tries to choose the best thing
 * to work on next given what it knows about the requirements for each task.
 *
 * @author damien@sitemorph.net
 *
 * @todo Review options http://en.wikipedia.org/wiki/Completely_Fair_Scheduler
 */
public interface TaskPicker {

  /**
   * Pick the next task to work on from the queue.
   *
   * @return the task to run or null if none can be picked.
   */
  public Task pick(long now, CrudStore<Task> taskStore) throws QueueException;
}
