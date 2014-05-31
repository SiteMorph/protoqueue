package net.sitemorph.queue;

import net.sitemorph.queue.Message.Task;

/**
 * A task worker is a specific unit of work and provides a runnable interface.
 * Note that an implementation class will be called to ask if it relevant to
 * a task. If it is, then it will be included in the run set for a task.
 *
 * The lifecyle of a task is
 *
 * 0) task worker is reset.
 * 1) is Relevant ( task ).
 * 2) task is set for next call of run.
 * 3) run is called.
 * 3.1) The task may be stopped at any time. If stopped goes to stop state.
 *    Tasks that can't be interrupted may be terminated.
 * 4) exit status of all tasks run is inspected and if all succeeded rest. If
 *    any task in the task set fails stop can be called to recover after the
 *    run is complete and the task will be run again.
 *
 * Depending on the state of the queue the task worker may be called twice for
 * the same task. Although this is theoretically possible it is unlikely to
 * happen often. Therefore, tasks should be implemented to be
 * idempotent or use a different mechanism like an external lock.
 *
 * Once all registered task workers have completed their work the resulting
 * task is returned and removed from the queue. This provides at least once
 * semantics for tasks.
 *
 * Note that if the same task worker instance is registered multiple times it
 * could be called concurrently so it is up to the implementer to resolve.
 *
 * In extreme circumstances a task can de-register it's self from the task
 * listeners when it is apparent that a task will not complete due to it's
 * participation in the task.
 */
public interface TaskWorker extends Runnable {

  /**
   * Reset a task before handling a new task job run.
   */
  public void reset();

  /**
   * Return true if the worker wants to be included in the set of workers for
   * the given task.
   *
   * @param task scope
   * @return true if wish to be included.
   */
  public boolean isRelevant(Task task);

  /**
   * If a task is relevant, it will be set, as well as the dispatcher.
   * @param task scope of the next call to run
   * @param dispatcher context.
   */
  public void setTask(Task task, TaskDispatcher dispatcher);

  /**
   * Get the current executing status of a task.
   * @return task status state.
   */
  public TaskStatus getStatus();

  /**
   * Stop a task, called on shutdown and timeout.
   */
  public void stop();
}
