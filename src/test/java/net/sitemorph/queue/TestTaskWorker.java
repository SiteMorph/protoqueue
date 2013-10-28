package net.sitemorph.queue;

import net.sitemorph.queue.Message.Task;

/**
 * Test worker which just instruments all sorts of issues and nightmares.
 *
 * @author damien@sitemorph.net
 */
public class TestTaskWorker implements TaskWorker {

  private boolean isReset = false;
  private Task task = null;
  private boolean isStopped = false;
  private boolean hasUndone = false;
  private boolean hasRun = false;
  private volatile TaskDispatcher dispatcher;
  private TaskStatus overrideStatus = null;

  @Override
  public void reset() {
    isReset = true;
  }

  @Override
  public boolean isRelevant(Task task) {
    // we like to work so always hop in!
    return true;
  }

  @Override
  public void setTask(Task task, TaskDispatcher dispatcher) {
    this.task = task;
    this.dispatcher = dispatcher;
  }

  @Override
  public TaskStatus getStatus() {
    if (null == overrideStatus) {
      if (hasUndone) {
        return TaskStatus.UNDONE;
      }
      if (hasRun) {
        return TaskStatus.DONE;
      } else {
        return TaskStatus.RESET;
      }
    } else {
      return overrideStatus;
    }
  }

  public void setOverrideStatus(TaskStatus overrideStatus) {
    this.overrideStatus = overrideStatus;
  }

  @Override
  public void stop() {
    isStopped = true;
  }

  @Override
  public void undo() {
    hasUndone = true;
  }

  @Override
  public void run() {
    hasRun = true;
    if (null != dispatcher) {
      dispatcher.shutdown();
    }
    if (hasUndone) {
      dispatcher.unregister(this);
    }
  }

  public boolean hasRun() {
    return hasRun;
  }

  public boolean isReset() {
    return isReset;
  }

  public Task getTask() {
    return task;
  }

  public boolean isStopped() {
    return isStopped;
  }

  public boolean isHasUndone() {
    return hasUndone;
  }

  public void setShutdownDispatcher(TaskDispatcher dispatcher) {
    this.dispatcher = dispatcher;
  }
}
