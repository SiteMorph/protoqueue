package net.sitemorph.queue;

import com.google.common.collect.Lists;
import net.sitemorph.protostore.CrudException;
import net.sitemorph.protostore.CrudIterator;
import net.sitemorph.queue.Message.Task;
import org.joda.time.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import static org.joda.time.DateTime.now;

/**
 * Task dispatcher is used to manage long running and future tasks.
 * Implementations of a
 * task runner should be implemented such that they can be killed or restarted
 * at will. This ultimately means that they should support 'resuming' based on
 * data passed as state.
 *
 * The semantics of the task dispatcher are that it will try to execute tasks
 * using the registered tasks and will keep retrying until success, at which
 * point the task is removed from the queue. Tasks are claimed by a dispatcher
 * which releases the task.
 *
 * If multiple task executors are registered for a path then all must complete.
 * If one of the tasks fails to complete then all tasks will be called to undo
 * their work. In the normal case with only a single task executor it will just
 * run but if another fails undo will be called. Undo is intended for situations
 * where it would be good to clean up rather than leave work in an inconsistent
 * state. As such it is a best effort feature.
 *
 * Tasks must call the done method on the dispatcher or the dispatcher will wait
 * until it times out. This change was made rather than sleeping to allow the
 * dispatcher to be more performant by avoiding unnecessary wait periods at the
 * cost of requiring
 *
 * If no task worker is registered or collaborates in the processing of an
 * event it will be considered 'complete' and removed from the queue.
 *
 * The task path is used as a uri to register task dispatchers.
 *
 * Tasks will be executed as soon after their timestamp as possible but future
 * tasks will not be executed until they are overdue.
 */
public class TaskDispatcher implements Runnable {

  private static final long TASK_TIMEOUT_PERIOD = 1000;
  private static final long ONE_DAY = 24 * 60 * 60000;
  private static final long SHUTDOWN_GRACE_PERIOD = 1000;

  private long unhandledErrorSleep = 10000;
  private Logger log = LoggerFactory.getLogger(getClass());
  private ExecutorService executorService;
  private volatile boolean run = true;
  private long sleep = TASK_TIMEOUT_PERIOD;
  private TaskQueueFactory taskQueueFactory;
  private final List<TaskWorker> workers;
  private long taskTimeout = ONE_DAY;
  private volatile UUID identity;
  private volatile long minimumSleep = 100;
  private volatile List<QueueWatcher> watchers;

  private TaskDispatcher() {
    workers = Lists.newArrayList();
    watchers = Lists.newArrayList();
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
      long time = now().getMillis();
      try {
        Task task;
        queue = taskQueueFactory.getTaskQueue();

        task = queue.claim(identity, time, time + taskTimeout + sleep);

        // if empty or future then sleep
        if (null == task) {
          log.debug("No task returned. Sleeping for {}", sleep);
          long alarm = nextAlarmTime(queue, time, sleep);

          long period = alarm - time;
          if (minimumSleep > period) {
            period = minimumSleep;
          }
          taskQueueFactory.returnTaskQueue(queue);
          queue = null;
          try {
            log.debug("TaskDispatcher out of tasks. Sleeping for {}", period);
            for (QueueWatcher watcher : watchers) {
              watcher.dispatcherSleeping(this);
            }
            long beginSleep = System.currentTimeMillis();
            synchronized (this) {
              wait(period);
            }
            long endSleep = System.currentTimeMillis();
            log.debug("TaskDispatcher Slept for {}", (endSleep - beginSleep));
          } catch (InterruptedException e) {
            log.info("TaskDispatcher interrupted while waiting for more tasks");
          }
          // Must restart cycle
          continue;
        } else {
          log.debug("Task returned by claim {}", task.getUrn());
        }
        // return the task queue while the worker is at it...
        taskQueueFactory.returnTaskQueue(queue);

        // build the set of workers up
        List<Callable<Task>> running = Lists.newArrayList();
        List<TaskWorker> taskSet = Lists.newArrayList();
        for (TaskWorker worker : workers) {
          if (worker.isRelevant(task)) {
            worker.reset();
            worker.setTask(task, this);
            running.add(new CallableAdapter(worker, task));
            taskSet.add(worker);
          }
        }

        boolean ok = true;
        try {
          for (QueueWatcher watcher : watchers) {
            watcher.taskScheduled(task, taskSet, this);
          }
          executorService.invokeAll(running, taskTimeout, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
          log.error("TaskDispatcher Task set was interrupted", e);
          ok = false;
        } catch (NullPointerException e) {
          log.error("TaskDispatcher Null element in worker set", e);
          ok = false;
        } catch (RejectedExecutionException e) {
          log.error("TaskDispatcher error scheduling task", e);
          ok = false;
        }

        // restore the task queue
        queue = taskQueueFactory.getTaskQueue();
        if (run && ok && successful(taskSet, task)) {
          log.debug("TaskDispatcher {} Task Set Successful. De-queueing Task {}",
              task.getPath(), task.getUrn());
          try {
            queue.remove(task);
          } catch (StaleClaimException e) {
            ok = false;
            log.info("Successful task but queue reports task as stale so " +
                "cancelling");
          } catch (QueueException e) {
            ok = false;
            log.info("Successful task but queue reports that the task has " +
                "been dequeued by another dispatcher");
          }
        } else {
          log.debug("Task set not successful. Calling stop / rollback.");
          ok = false;
        }
        if (!ok) {
          log.debug("TaskDispatcher Task Set Failed.");
          cancelTasks(taskSet);
          try {
            queue.release(task);
          } catch (StaleClaimException e) {
            log.info("Attempt to release task " + task.getUrn() + " failed");
          }
          taskQueueFactory.returnTaskQueue(queue);
        } else {
          log.debug("TaskDispatcher Task Set Successful.");
          taskQueueFactory.returnTaskQueue(queue);
        }
      } catch (Throwable t) {
        log.error("Task dispatcher encountered unhandled error", t);
        try {
          taskQueueFactory.returnTaskQueue(queue);
        } catch (Throwable e) {
          log.error("Queue error releasing task queue in task dispatcher", e);
        }
        log.info("Task dispatcher sleeping to await system recovery");
        try {
          synchronized (this) {
            wait(unhandledErrorSleep);
          }
        } catch (InterruptedException e) {
          log.info("Task dispatcher interrupted while in error sleep", e);
        }
      }
    }
  }

  private long nextAlarmTime(TaskQueue queue, long currentTime, long sleep)
      throws QueueException, CrudException {
    long alarm =  currentTime + sleep;
    // figure out the next task time
    CrudIterator<Task> tasks = queue.tasks();
    while (tasks.hasNext()) {
      Task candidate = tasks.next();
      if (candidate.getRunTime() > (currentTime + sleep)) {
        // already after alarm
        break;
      }
      if (candidate.hasClaim() && candidate.hasClaimTimeout()) {
        if (candidate.getClaimTimeout() < alarm) {
          log.debug("Found a claim timeout before alarm time");
          alarm = candidate.getClaimTimeout();
        }
      } else if (candidate.getRunTime() < alarm) {
        log.debug("Found task time {} before alarm time {}",
            candidate.getRunTime(), alarm);
        alarm = candidate.getRunTime();
      }
    }
    tasks.close();
    if (alarm < currentTime) {
      alarm = currentTime;
    }
    return alarm;
  }

  /**
   * Remove a task listener from the registered listeners.
   *
   * @param worker listener
   */
  public void deregister(TaskWorker worker) {
    workers.remove(worker);
  }

  public Task schedule(Task.Builder update) throws QueueException {
    TaskQueue queue = taskQueueFactory.getTaskQueue();
    Task result = queue.push(update);
    taskQueueFactory.returnTaskQueue(queue);
    // notify self in case sleeping and task could be started
    synchronized (this) {
      notify();
    }
    return result;
  }

  public void deschedule(Task task) throws QueueException  {
    TaskQueue queue = taskQueueFactory.getTaskQueue();
    queue.remove(task);
    taskQueueFactory.returnTaskQueue(queue);
  }

  /**
   * Deschedule scheduled task from the queue by given task path
   *
   * @param path a path of the task to be de scheduled
   * @throws QueueException
   */
  public void deschedule(String path) throws QueueException {
    log.info("de scheduling task " + path);
    // run over the task queue and ensure that there is a task for this event
    // scheduled
    TaskQueue queue = null;
    Task task = null;
    try {
      queue = taskQueueFactory.getTaskQueue();

      CrudIterator<Task> tasks = queue.tasks();
      while (tasks.hasNext()) {
        Task found = tasks.next();
        if (found.getPath().equals(path)) {
          task = found;
          break;
        }
      }
      tasks.close();

      // scheduled
      if (task != null) {
        queue.remove(task);
      }
      taskQueueFactory.returnTaskQueue(queue);
    } catch (QueueException e) {
      taskQueueFactory.returnTaskQueue(queue);
      log.error("Error to deschedule scheduled task.", e);
      throw e;
    } catch (CrudException e) {
      taskQueueFactory.returnTaskQueue(queue);
      log.error("Crud exception trying to look for schedule tasks", e);
      throw new QueueException("Storage error checking for task scheduled status", e);
    }
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
  public synchronized boolean isTaskScheduled(String path, Interval interval)
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

  private boolean successful(List<TaskWorker> running, Task task) {
    for (TaskWorker worker : running) {
      if (!TaskStatus.DONE.equals(worker.getStatus())) {
        for (QueueWatcher watcher : watchers) {
          watcher.workerFailed(worker, task, this);
        }
        return false;
      }
    }
    return true;
  }

  private void cancelTasks(List<TaskWorker> workers)
      throws ExecutionException, InterruptedException {
    for(TaskWorker task : workers) {
      task.stop();
    }
  }

  public void shutdown() {
    log.debug("TaskDispatcher Shutting Down");
    run = false;
    synchronized (this) {
      // notify in case task dispatcher has 'cleanup' left to do for completed
      notify();
    }
    try {
      Thread.sleep(SHUTDOWN_GRACE_PERIOD);
    } catch (InterruptedException e) {
      log.error("Task dispatcher shutdown grace period interrupted");
    }
    executorService.shutdown();
  }

  /**
   * Get a new builder.
   * @return a new builder instance used to construct a task dispatcher.
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {

    private UncaughtExceptionHandler handler = null;
    private int poolSize = 1;

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
      this.poolSize = poolSize;
      return this;
    }

    /**
     * Set the claim identity of the task dispatcher.
     *
     * @param identity
     */
    public Builder setIdentity(UUID identity) {
      this.dispatcher.identity = identity;
      return this;
    }

    /**
     * Set the uncaught exception handler for a a task dispatcher.
     * @param handler to handle the exception.
     * @return fluent builder
     */

    public Builder setUncaughtExceptionHandler(
        UncaughtExceptionHandler handler) {
      this.handler = handler;
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
     * Set the minimum sleep time to be used when a claim will be overdue but
     * you want to give a minimum time before this thread will restart e.g. 10
     * milliseconds.
     *
     * @param minimumSleep used when the thread is sleeping due to lack of work.
     * Must be positive as 0 implies for ever.
     */
    public Builder setMinimumSleep(long minimumSleep) {
      if (0 >= minimumSleep) {
        throw new IllegalArgumentException("Minimum sleep cycle must be  >= 1");
      }
      dispatcher.minimumSleep = minimumSleep;
      return this;
    }

    /**
     * Set the sleep time to use on an unhandled error which gives the
     * environment time to recover. E.g. 10000 or 60000
     *
     * @param unhandledErrorSleep
     */
    public Builder setUnhandledErrorSleep(long unhandledErrorSleep) {
      dispatcher.unhandledErrorSleep = unhandledErrorSleep;
      return this;
    }

    /**
     * Build the dispatcher.
     * @return dispatcher.
     * @throws IllegalStateException if the worker pool size isn't set or the
     *    task queue factory has not been set
     */
    public TaskDispatcher build() {
      if (0 > poolSize) {
        throw new IllegalStateException("Could not build task dispatcher " +
            "as the executor pool size was not set");
      }
      if (null == dispatcher.taskQueueFactory) {
        throw new IllegalStateException("Could not build task dispatcher " +
            "as the task queue factory has not been set");
      }
      if (null == dispatcher.identity) {
        throw new IllegalStateException("Could not build task dispatcher " +
            "as the identity of the dispatcher was not set");
      }
      // create a factory to set up a handler per thread.
      final UncaughtExceptionHandler exceptionHandler = this.handler;
      ThreadFactory factory = new ThreadFactory() {
        @Override
        public Thread newThread(Runnable runnable) {
          final Thread thread = new Thread(runnable);
          if (null != exceptionHandler) {
            thread.setUncaughtExceptionHandler(exceptionHandler);
          }
          return thread;
        }
      };
      dispatcher.executorService =
          Executors.newFixedThreadPool(poolSize, factory);
      return dispatcher;
    }
  }
}
