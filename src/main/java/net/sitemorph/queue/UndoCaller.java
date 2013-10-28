package net.sitemorph.queue;

/**
 * Simple runner that calls the undo method of a worker.
 *
 * @author damien@sitemorph.net
 */
class UndoCaller implements Runnable {

  private final TaskWorker worker;

  public UndoCaller(TaskWorker worker) {
    this.worker = worker;
  }

  @Override
  public void run() {
    worker.undo();
  }
}
