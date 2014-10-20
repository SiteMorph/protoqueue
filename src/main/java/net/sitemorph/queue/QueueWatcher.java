package net.sitemorph.queue;

import net.sitemorph.queue.Message.Task;

import java.util.List;

/**
 * The queue watcher interface allows registration of handlers to review queue
 * state on different queue events.
 *
 * @author damien@sitemorph.net
 */
public interface QueueWatcher {

  /**
   * Called to notify the watcher of the task dispatcher sleeping events.
   *
   * @param taskDispatcher context
   */
  public void dispatcherSleeping(TaskDispatcher taskDispatcher)
      throws QueueException;

  /**
   * Called to notify the watcher that a task is being run by a collection of
   * task workers in the context of the provided dispatcher
   * @param task being executed.
   * @param taskSet of selected task workers
   * @param taskDispatcher context
   */
  public void taskScheduled(Task task, List<TaskWorker> taskSet,
      TaskDispatcher taskDispatcher) throws QueueException;

  /**
   * Called for each of the task workers who failed to complete a task.
   *
   * @param worker that failed to complete.
   * @param task context that failed.
   * @param taskDispatcher context.
   */
  public void workerFailed(TaskWorker worker, Task task, TaskDispatcher taskDispatcher);
}
