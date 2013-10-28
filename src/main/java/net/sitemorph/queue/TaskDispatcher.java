package net.sitemorph.queue;

import net.sitemorph.protostore.CrudException;
import net.sitemorph.protostore.CrudIterator;
import net.sitemorph.queue.Message.Task;

import com.google.common.collect.Lists;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Task dispatcher is used to manage long running tasks. Implementations of a
 * task runner should be implemented such that they can be killed or restarted
 * at will. This ultimately means that they should support 'resuming' based on
 * data passed as state.
 *
 * The semantics of the task dispatcher are that it will try to execute tasks
 * using the registered tasks and will keep retrying until success at which
 * point the task is removed from the queue.
 *
 * If multiple task executors are registered for a path then all must complete.
 * If one of the tasks fails to complete then all tasks will be called to undo
 * their work. In the normal case with only a single task executor it will just
 * run but if another fails undo will be called.
 *
 * The task path is used as a uri to register task dispatchers.
 *
 * Tasks will be executed as soon after their timestamp as possible but future
 * tasks will not be executed until they are overdue.
 *
 * Note: Use at your own risk.
 */

// TODO 20131008 Implement critical section around queue updates
public class TaskDispatcher implements Runnable {

  private static final long TASK_AWAIT_SLEEP = 1000;
  private static final long ONE_DAY = 24 * 60 * 60000;
  private Logger log = LoggerFactory.getLogger(getClass());
  private ExecutorService executorService;
  private volatile boolean run = true;
  private long sleep;
  private TaskQueueFactory taskQueueFactory;
  private final List<TaskWorker> workers;
  private long taskTimeout = ONE_DAY;

  private TaskDispatcher() {
    workers = Lists.newArrayList();
  }

  @Override
  public void run() {
    while (run) {

      log.debug("TaskDispatcher Starting Run");

      TaskQueue queue = null;
      try {
        queue = taskQueueFactory.getTaskQueue();

        Task task = queue.peek();
        // if empty or future then sleep
        if (null == task || isFutureTask(task)) {
          try {
            log.debug("TaskDispatcher out of tasks. Sleeping");
            taskQueueFactory.returnTaskQueue(queue);
            synchronized (this) {
              wait(sleep);
            }
          } catch (InterruptedException e) {
            log.info("TaskDispatcher interrupted while waiting for more tasks");
          }
          continue;
        }

        // build the set of workers up
        List<Future> futures = Lists.newArrayList();
        List<TaskWorker> running = Lists.newArrayList();
        long start = System.currentTimeMillis();
        synchronized (workers) {
          for (TaskWorker worker : workers) {
            if (worker.isRelevant(task)) {
              worker.reset();
              worker.setTask(task, this);
              futures.add(executorService.submit(worker));
              running.add(worker);
            }
          }
        }

        // await futures
        while (notDone(futures)) {
          try {
            log.debug("TaskDispatcher waiting for tasks to complete");
            synchronized (this) {
              wait(TASK_AWAIT_SLEEP);
            }
          } catch(InterruptedException e) {
            log.info("TaskDispatcher interrupted waiting for tasks " +
                "to complete");
          }
          if (timeoutTask(start)) {
            log.debug("TaskDispatcher Task timeout reached. Cancelling.");
            cancelTasks(futures);
          }
        }

        // if all done check status
        if (successful(running)) {
          log.debug("TaskDispatcher Task Set Successful. De-queueing Task {}",
              task.getUrn());
          queue.remove(task);
          taskQueueFactory.returnTaskQueue(queue);
        } else {
          log.debug("TaskDispatcher Task Set Failed. Calling Undo.");
          undo(running);
          taskQueueFactory.returnTaskQueue(queue);
          synchronized (this) {
            log.debug("TaskDispatcher waiting after error to prevent overload");
            wait(TASK_AWAIT_SLEEP);
          }
        }
      } catch (Throwable t) {
        log.error("Task Dispatcher Encountered Unhandled Error", t);
        try {
          taskQueueFactory.returnTaskQueue(queue);
        } catch (QueueException e) {
          log.error("Queue error releasing task que in task dispatcher");
        }
        log.info("Task dispatcher sleeping to await system recovery");
        try {
          synchronized (this) {
            wait(sleep);
          }
        } catch (InterruptedException e) {
          log.info("Task dispatcher interrupted while in error sleep", e);
        }
      }
    }
  }

  public void unregister(TaskWorker worker) {
    synchronized (workers) {
      workers.remove(worker);
    }
  }

  /**
   * The reschedule method is a conveniece method that schedules a task but
   * doesn't take into account if the task is already scheduled twice.
   * The reschedule semantics are fuzzy as the current task is actually delted
   * only when all registered listeners have completed. Hence this feature
   * becomes fuzzy as it requires the user to put themselves into an error
   * state to avoid task deletion.
   *
   * Given that, advise queueing a new task using the old as a prototype.
   *
   * @param update to insert.
   * @return the new task item.
   * @throws QueueException
   */
  @Deprecated
  public Task reschedule(Task.Builder update) throws QueueException {
    TaskQueue queue = taskQueueFactory.getTaskQueue();
    Task result = queue.push(update);
    taskQueueFactory.returnTaskQueue(queue);
    return result;
  }

  public Task schedule(Task.Builder update) throws QueueException {
    TaskQueue queue = taskQueueFactory.getTaskQueue();
    Task result = queue.push(update);
    taskQueueFactory.returnTaskQueue(queue);
    return result;
  }

  public void deschedule(Task task) throws QueueException  {
    TaskQueue queue = taskQueueFactory.getTaskQueue();
    queue.remove(task);
    taskQueueFactory.returnTaskQueue(queue);
  }

  public boolean isTaskScheduled(String path)
      throws QueueException, CrudException {
    // run over the task queue and ensure that there is a task for this event
    // scheduled
    boolean scheduled = false;
    TaskQueue queue = null;
    try {
      queue = taskQueueFactory.getTaskQueue();

      CrudIterator<Task> tasks = queue.tasks();
      while (tasks.hasNext()) {
        Task task = tasks.next();
        if (task.getPath().equals(path)) {
          scheduled = true;
          break;
        }
      }
      tasks.close();
      taskQueueFactory.returnTaskQueue(queue);
    } catch (QueueException e) {
      taskQueueFactory.returnTaskQueue(queue);
      log.error("Error checking for presence of scheduled task.", e);
      throw e;
    } catch (CrudException e) {
      taskQueueFactory.returnTaskQueue(queue);
      log.error("Crud exception trying to look for new tasks", e);
      throw e;
    }
    return scheduled;
  }

  /**
   * Test if a task is scheduled or not.
   *
   * @param path of the task
   * @return true if found for the interval provided
   */
  public boolean isTaskScheduled(String path, Interval interval)
      throws QueueException, CrudException {
    boolean scheduled = false;
    TaskQueue queue = null;
    try {
      queue = taskQueueFactory.getTaskQueue();
      CrudIterator<Task> tasks = queue.tasks();
      while (tasks.hasNext()) {
        Task task = tasks.next();
        if (task.getPath().equals(path) &&
            interval.contains(task.getRunTime())) {
            scheduled = true;
            break;
        }
      }
      tasks.close();
      taskQueueFactory.returnTaskQueue(queue);
    } catch (QueueException e) {
      taskQueueFactory.returnTaskQueue(queue);
      log.error("Error checking for presence of {} with interval {}",
          path, interval, e);
      throw e;
    } catch (CrudException e) {
      taskQueueFactory.returnTaskQueue(queue);
      log.error("Error checking for presence of {} with interval {}",
          path, interval, e);
      throw e;
    }
    return scheduled;
  }

  private void undo(List<TaskWorker> running)
      throws ExecutionException, InterruptedException {
    if (!run) {
      log.error("Undo tasks could not be completed due to scheduler shutdown " +
          "prior to undo invocation.");
      return;
    }
    for (TaskWorker worker : running) {
      log.info("Calling undo for {} in state {}",
          worker.getClass().getCanonicalName(), worker.getStatus());
      Future future = executorService.submit(new UndoCaller(worker));
      future.get();
    }
  }

  private boolean successful(List<TaskWorker> running) {
    for (TaskWorker worker : running) {
      if (!TaskStatus.DONE.equals(worker.getStatus())) {
        return false;
      }
    }
    return true;
  }

  private void cancelTasks(List<Future> futures)
      throws ExecutionException, InterruptedException {
    for(Future future : futures) {
      Object task = future.get();
      if (task instanceof TaskWorker) {
        ((TaskWorker) task).stop();
      }
      future.cancel(true);
    }
  }

  private boolean timeoutTask(long startedTime) {
    return DateTime.now().isAfter(startedTime + taskTimeout);
  }

  private boolean notDone(List<Future> futures) {
    for (Future future: futures) {
      if (!future.isDone()) {
        return true;
      }
    }

    return false;
  }

  private boolean isFutureTask(Task task) {
    return DateTime.now(DateTimeZone.UTC).isBefore(task.getRunTime());
  }

  public void shutdown() {
    log.debug("TaskDispatcher Shutting Down");
    run = false;
    executorService.shutdown();
    synchronized (this) {
      notify();
    }
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {

    private Builder() {
      dispatcher = new TaskDispatcher();
    }

    private TaskDispatcher dispatcher;

    public Builder setWorkerPoolSize(int poolSize) {
      dispatcher.executorService = Executors.newFixedThreadPool(poolSize);
      return this;
    }

    public Builder setSleepInterval(long sleep) {
      dispatcher.sleep = sleep;
      return this;
    }

    public Builder setTaskQueueFactory(TaskQueueFactory factory) {
      dispatcher.taskQueueFactory = factory;
      return this;
    }

    public Builder registerTaskWorker(TaskWorker taskWorker) {
      dispatcher.workers.add(taskWorker);
      return this;
    }

    public Builder setTaskTimeout(long timeout) {
      dispatcher.taskTimeout = timeout;
      return this;
    }

    public TaskDispatcher build() {
      return dispatcher;
    }
  }
}
