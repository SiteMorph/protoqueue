package net.sitemorph.queue;

/**
 * Task running status.
 */
public enum TaskStatus {

  /**
   * The task has been reset.
   */
  RESET,

  /**
   * The scope of a task has been set but has not yet been run.
   */
  TASK_SET,
  /**
   * The task is currently running.
   */
  RUNNING,
  /**
   * The task is in an error state.
   */
  ERROR,
  /**
   * The task has been stopped
   */
  STOPPED,
  /**
   * The task has completed the last call to run with the previously set scope
   */
  DONE
}
