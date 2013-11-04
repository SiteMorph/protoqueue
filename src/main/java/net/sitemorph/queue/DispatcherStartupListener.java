package net.sitemorph.queue;

/**
 * Annotation interface that indicates that a task wishes to be notified about
 * the dispatcher startup event. An implementing class will be called by the
 * dispatcher before it resumes operation on the task queue.
 *
 * @author damien@sitemorph.net
 */
public interface DispatcherStartupListener {

  public void dispatcherStarted(TaskDispatcher dispatcher) throws QueueException;
}
