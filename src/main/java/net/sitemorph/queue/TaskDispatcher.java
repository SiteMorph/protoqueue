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
 * using the registered tasks and will keep retrying until success, at which
 * point the task is removed from the queue.
 *
 * If multiple task executors are registered for a path then all must complete.
 * If one of the tasks fails to complete then all tasks will be called to undo
 * their work. In the normal case with only a single task executor it will just
 * run but if another fails undo will be called. Undo is intended for situations
 * where it would be good to clean up rather than leave work in an inconsistent
 * state. As such it is a best effort feature.
 *
 * The task path is used as a uri to register task dispatchers.
 *
 * Tasks will be executed as soon after their timestamp as possible but future
 * tasks will not be executed until they are overdue.
 */

// TODO 20131008 Implement critical section around queue updates
public class TaskDispatcher implements Runnable {

  private static final long TASK_TIMEOUT_PERIOD = 1000;
  private static final long ONE_DAY = 24 * 60 * 60000;
  private Logger log = LoggerFactory.getLogger(getClass());
  private ExecutorService executorService;
  private volatile boolean run = true;
  private long sleep = TASK_TIMEOUT_PERIOD;
  private TaskQueueFactory taskQueueFactory;
  private final List<TaskWorker> workers;
  private long taskTimeout = ONE_DAY;

  private TaskDispatcher() {
    workers = Lists.newArrayList();
  }

  @Override
  public void run() {

    // fire startup events
    for (TaskWorker worker : workers) {
      if (worker instanceof DispatcherStartupListener) {
        try {
          ((DispatcherStartupListener) worker).dispatcherStarted(this);
        } catch (QueueException e) {
          log.error("Error calling startup method on task worker {}",
              worker.getClass().getName(), e);
        }
      }
    }

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
        synchronized (this) {
          for (TaskWorker worker : workers) {
            log.debug("Considering worker {} for task {}", worker.getClass(),
                task.getUrn());
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
              wait(sleep);
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
            log.debug("TaskDispatcher waiting after error to prevent " +
                "immediate rerun of failed tasks");
            wait(sleep);
          }
        }
      } catch (Throwable t) {
        log.error("Task dispatcher encountered unhandled error", t);
        try {
          taskQueueFactory.returnTaskQueue(queue);
        } catch (QueueException e) {
          log.error("Queue error releasing task queue in task dispatcher");
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

  /**
   * Remove a task listener from the registered listeners.
   *
   * @param worker listener
   */
  public void deregister(TaskWorker worker) {
    synchronized (workers) {
      workers.remove(worker);
    }
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
      throws QueueException {
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
      throw new QueueException("Storage error checking for task scheduled " +
          "status", e);
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
      throws QueueException {
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
      throw new QueueException("Error accessing queue storage", e);
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

  /**
   * Get a new builder.
   * @return a new builder instance used to construct a task dispatcher.
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {

    private Builder() {
      dispatcher = new TaskDispatcher();
    }

    private TaskDispatcher dispatcher;

    /**
     * Set the size of the fixed worker pool. Recommend at least two.
     *
     * @param poolSize for a fixed worker pool.
     * @return builder
     */
    public Builder setWorkerPoolSize(int poolSize) {
      dispatcher.executorService = Executors.newFixedThreadPool(poolSize);
      return this;
    }

    /**
     * Set how long the dispatcher sleeps while awaiting tasks. Note that this
     * sets an upper bound on task start 'lateness'.
     * @param sleep milliseconds.
     * @return builder
     */
    public Builder setSleepInterval(long sleep) {
      dispatcher.sleep = sleep;
      return this;
    }

    /**
     * Factory that is use dto create queue storage drivers by the task manager
     * and task running instances.
     * @param factory for task queues.
     * @return builder
     */
    public Builder setTaskQueueFactory(TaskQueueFactory factory) {
      dispatcher.taskQueueFactory = factory;
      return this;
    }

    /**
     * Register a task worker as a task event listener. The task can then decide
     * if it needs to perform work for events in the task queue.
     *
     * @param taskWorker which will listen.
     * @return builder
     */
    public Builder registerTaskWorker(TaskWorker taskWorker) {
      dispatcher.workers.add(taskWorker);
      return this;
    }

    /**
     * The dispatcher will time tasks out if they don't complete within this
     * time frame.
     * @param timeout in milliseconds.
     * @return builder
     */
    public Builder setTaskTimeout(long timeout) {
      dispatcher.taskTimeout = timeout;
      return this;
    }

    /**
     * Build the dispatcher.
     * @return dispatcher.
     * @throws IllegalStateException if the worker pool size isn't set or the
     *    task queue factory has not been set
     */
    public TaskDispatcher build() {
      if (null == dispatcher.executorService) {
        throw new IllegalStateException("Could not build task dispatcher " +
            "as the executor pool size was not set");
      }
      if (null == dispatcher.taskQueueFactory) {
        throw new IllegalStateException("Could not build task dispatcher " +
            "as the task queue factory has not been set");
      }
      return dispatcher;
    }
  }
}
