package net.sitemorph.queue;

import net.sitemorph.queue.Message.Task;

/**
 * A task worker that doesn't do anything
 *
 * @author damien@sitemorph.net
 */
public class NullTaskWorker implements TaskWorker {
  private volatile TaskStatus state;

  @Override
  public void reset() {
    state = TaskStatus.RESET;
  }

  @Override
  public boolean isRelevant(Task task) {
    return true;
  }

  @Override
  public void setTask(Task task, TaskDispatcher dispatcher) {
    state = TaskStatus.TASK_SET;
  }

  @Override
  public TaskStatus getStatus() {
    return state;
  }

  @Override
  public void stop() {
    state = TaskStatus.STOPPED;
  }

  @Override
  public void undo() {
    state = TaskStatus.UNDONE;
  }

  @Override
  public void run() {
    state = TaskStatus.DONE;
  }
}
