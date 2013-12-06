package net.sitemorph.queue;

import net.sitemorph.queue.Message.Task;

import java.util.concurrent.Callable;

/**
 * Callable adapter for task workers to allow them to be called by invoke all.
 *
 * @author damien@sitemorph.net
 */
public class CallableAdapter implements Callable<Task> {

  private volatile Task task;
  private volatile TaskWorker worker;

  public CallableAdapter(TaskWorker worker, Task task) {
    this.worker = worker;
    this.task = task;
  }

  @Override
  public Task call() throws Exception {
    worker.run();
    return task;
  }
}
