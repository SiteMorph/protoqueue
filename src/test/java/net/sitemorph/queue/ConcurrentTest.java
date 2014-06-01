package net.sitemorph.queue;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;

import net.sitemorph.protostore.CrudException;
import net.sitemorph.protostore.CrudIterator;
import net.sitemorph.protostore.CrudStore;
import net.sitemorph.protostore.InMemoryStore.Builder;
import net.sitemorph.protostore.SortOrder;
import net.sitemorph.queue.Message.Task;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Concurrent task handler test which runs multiple schedulers which all claim
 * and work on tasks.
 */
public class ConcurrentTest {

  private static Logger log = LoggerFactory.getLogger("ConcurrentTest");

  private static volatile Map<String, AtomicInteger> counters =
      Maps.newConcurrentMap();
  private static volatile Set<String> error = Sets.newConcurrentHashSet();
  private static volatile String last;
  private static final int TASK_COUNT = 10000;
  private static final int TASK_SLEEP = 10;
  private static final double ERROR_RATE = 0.1;
  final CrudStore<Task> store = new Builder<Task>()
      .setPrototype(Task.newBuilder())
      .setVectorField("vector")
      .setUrnField("urn")
      .setSortOrder("runTime", SortOrder.ASCENDING)
      .addIndexField("path")
      .build();

  @BeforeMethod(alwaysRun = true)
  public void resetTest() throws CrudException {
    log.info("RESETTING TASKS");
    counters.clear();
    for (int i = 0; i < TASK_COUNT; i ++) {
      counters.put(UUID.randomUUID().toString(), new AtomicInteger(0));
    }
    error.clear();
    last = null;
    long now = System.currentTimeMillis();
    for (Map.Entry<String, AtomicInteger> entry : counters.entrySet()) {
      store.create(Task.newBuilder()
          .setPath(entry.getKey())
          .setRunTime(now++));
      last = entry.getKey();
    }
    log.info("Done counters");
  }

  @Test(groups = "longTest")
  public void testSingleWorker() throws InterruptedException, CrudException {

    log.info("SINGLE WORKER START");

    TaskDispatcher dispatcher = TaskDispatcher.newBuilder()
        .setIdentity(UUID.randomUUID())
        .setSleepInterval(10)
        .setTaskTimeout(100)
        .setWorkerPoolSize(1)
        .setUncaughtExceptionHandler(new UncaughtExceptionHandler() {
          @Override
          public void uncaughtException(Thread thread, Throwable throwable) {
            log.error("Unhandled exception", throwable);
            throw new RuntimeException("Uncaught error", throwable);
          }
        })
        .setTaskQueueFactory(new TaskQueueFactory() {
          @Override
          public TaskQueue getTaskQueue() throws QueueException {
            return CrudTaskQueue.fromCrudStore(store);
          }

          @Override
          public void returnTaskQueue(TaskQueue queue) throws QueueException {
            // don't need to release resources.
          }
        })
        .registerTaskWorker(new UnreliableWorker(ERROR_RATE))
        .build();
    log.info("Starting dispatcher");
    Thread thread = new Thread(dispatcher);
    thread.setDaemon(true);
    thread.setName("Dispatcher");
    thread.setUncaughtExceptionHandler(new UncaughtExceptionHandler() {
      @Override
      public void uncaughtException(Thread thread, Throwable throwable) {
        log.error("uncaught error", throwable);
      }
    });
    thread.start();
    long start = System.currentTimeMillis();
    while (0 == counters.get(last).intValue()) {
      int sum = 0;
      for (Map.Entry<String, AtomicInteger> entry : counters.entrySet()) {
        sum += entry.getValue().intValue();
      }
      CrudIterator<Task> tasks = store.read(Task.newBuilder());
      int claims = 0;
      while (tasks.hasNext()) {
        if (tasks.next().hasClaim()) {
          claims++;
        }
      }
      tasks.close();
      log.info("Sleeping while waiting for counter updates. Currently at {} " +
          "with {} claims.",
          sum,
          claims);
      Thread.sleep(1000);
    }
    long end = System.currentTimeMillis();
    log.info("Stopping dispatcher");
    dispatcher.shutdown();
    long time = end - start;
    long executionTime = (long)((double)TASK_COUNT * (1.0 + ERROR_RATE)) * TASK_SLEEP;
    long schedulingOverhead = time - executionTime;
    log.info("Total Time {} execution time {} scheduling overhead {}",
        time, executionTime, schedulingOverhead);

    // check that all counters are 0
    for (Map.Entry<String, AtomicInteger> entry : counters.entrySet()) {
      assertNotNull(entry.getKey(), "expected key");
      assertEquals(entry.getValue().intValue(), 1, "Expected 1 on " +
          entry.getKey());
    }

    assertFalse(store.read(Task.newBuilder()).hasNext(), "Expected all tasks done");

    log.info("SINGLE WORKER STOP");
  }

  @Test(groups = "longTest")
  public void testTwoOrchestrator() throws InterruptedException, CrudException {
    log.info("TEST TWO ORCHESTRATOR START");

    TaskDispatcher dispatcher1 = TaskDispatcher.newBuilder()
        .setIdentity(UUID.randomUUID())
        .setSleepInterval(10)
        .setTaskTimeout(100)
        .setWorkerPoolSize(1)
        .setUncaughtExceptionHandler(new UncaughtExceptionHandler() {
          @Override
          public void uncaughtException(Thread thread, Throwable throwable) {
            log.error("Unhandled exception", throwable);
            throw new RuntimeException("Uncaught error", throwable);
          }
        })
        .setTaskQueueFactory(new TaskQueueFactory() {
          @Override
          public TaskQueue getTaskQueue() throws QueueException {
            return CrudTaskQueue.fromCrudStore(store);
          }

          @Override
          public void returnTaskQueue(TaskQueue queue) throws QueueException {
            // don't need to release resources.
          }
        })
        .registerTaskWorker(new UnreliableWorker(0.1))
        .build();
    Thread thread1 = new Thread(dispatcher1);
    thread1.setDaemon(true);
    thread1.setName("Dispatcher1");
    thread1.setUncaughtExceptionHandler(new UncaughtExceptionHandler() {
      @Override
      public void uncaughtException(Thread thread, Throwable throwable) {
        log.error("uncaught error", throwable);
      }
    });

    TaskDispatcher dispatcher2 = TaskDispatcher.newBuilder()
        .setIdentity(UUID.randomUUID())
        .setSleepInterval(10)
        .setTaskTimeout(100)
        .setWorkerPoolSize(1)
        .setUncaughtExceptionHandler(new UncaughtExceptionHandler() {
          @Override
          public void uncaughtException(Thread thread, Throwable throwable) {
            log.error("Unhandled exception", throwable);
            throw new RuntimeException("Uncaught error", throwable);
          }
        })
        .setTaskQueueFactory(new TaskQueueFactory() {
          @Override
          public TaskQueue getTaskQueue() throws QueueException {
            return CrudTaskQueue.fromCrudStore(store);
          }

          @Override
          public void returnTaskQueue(TaskQueue queue) throws QueueException {
            // don't need to release resources.
          }
        })
        .registerTaskWorker(new UnreliableWorker(0.1))
        .build();
    Thread thread2 = new Thread(dispatcher2);
    thread2.setDaemon(true);
    thread2.setName("Dispatcher2");
    thread2.setUncaughtExceptionHandler(new UncaughtExceptionHandler() {
      @Override
      public void uncaughtException(Thread thread, Throwable throwable) {
        log.error("uncaught error", throwable);
      }
    });

    log.info("Starting dispatchers.");
    thread1.start();
    thread2.start();
    long start = System.currentTimeMillis();
    log.info("Started dispatchers.");

    while (0 == counters.get(last).intValue()) {
      int sum = 0;
      for (Map.Entry<String, AtomicInteger> entry : counters.entrySet()) {
        sum += entry.getValue().intValue();
      }
      CrudIterator<Task> tasks = store.read(Task.newBuilder());
      int claims = 0;
      while (tasks.hasNext()) {
        if (tasks.next().hasClaim()) {
          claims++;
        }
      }
      tasks.close();
      log.info("Sleeping while waiting for counter updates. Currently at {} " +
          "outstanding with {} claims", sum, claims);
      Thread.sleep(1000);
    }
    long end = System.currentTimeMillis();
    log.info("Stopping dispatcher");
    dispatcher2.shutdown();
    dispatcher1.shutdown();

    long time = end - start;
    long executionTime = (long)((double)TASK_COUNT / 2.0 * (1.0 + ERROR_RATE))
        * TASK_SLEEP;
    long schedulingOverhead = time - executionTime;
    log.info("Total Time {} execution time {} scheduling overhead {}",
        time, executionTime, schedulingOverhead);

    // check that all counters are 0
    for (Map.Entry<String, AtomicInteger> entry : counters.entrySet()) {
      assertNotNull(entry.getKey(), "expected key");
      assertEquals(entry.getValue().intValue(), 1, "Expected 1 on " +
          entry.getKey());
    }

    log.info("TEST TWO ORCHESTRATOR END");
  }


  @Test(groups = "longTest")
  public void testMultiOrchestrator() throws InterruptedException, CrudException {
    log.info("TEST MULTI ORCHESTRATOR START");

    Set<TaskDispatcher> dispatchers = Sets.newHashSet();
    int count = 100;
    for (int i = 0; i < count; i++) {
      TaskDispatcher dispatcher = TaskDispatcher.newBuilder()
          .setIdentity(UUID.randomUUID())
          .setSleepInterval(10)
          .setTaskTimeout(10000)
          .setWorkerPoolSize(1)
          .setUncaughtExceptionHandler(new UncaughtExceptionHandler() {
            @Override
            public void uncaughtException(Thread thread, Throwable throwable) {
              log.error("Unhandled exception", throwable);
              throw new RuntimeException("Uncaught error", throwable);
            }
          })
          .setTaskQueueFactory(new TaskQueueFactory() {
            @Override
            public TaskQueue getTaskQueue() throws QueueException {
              return CrudTaskQueue.fromCrudStore(store);
            }

            @Override
            public void returnTaskQueue(TaskQueue queue) throws QueueException {
              // don't need to release resources.
            }
          })
          .registerTaskWorker(new UnreliableWorker(0.1))
          .build();
      dispatchers.add(dispatcher);
    }

    Set<Thread> threads = Sets.newHashSet();
    for (TaskDispatcher dispatcher : dispatchers) {
      Thread thread = new Thread(dispatcher);
      thread.setDaemon(true);
      thread.setUncaughtExceptionHandler(new UncaughtExceptionHandler() {
        @Override
        public void uncaughtException(Thread thread, Throwable throwable) {
          log.error("uncaught error", throwable);
        }
      });
      threads.add(thread);
    }

    log.info("Starting dispatchers.");
    for (Thread thread : threads) {
      thread.start();
    }
    long start = System.currentTimeMillis();
    log.info("Started dispatchers.");

    while (0 == counters.get(last).intValue()) {
      int sum = 0;
      for (Map.Entry<String, AtomicInteger> entry : counters.entrySet()) {
        sum += entry.getValue().intValue();
      }
      CrudIterator<Task> tasks = store.read(Task.newBuilder());
      int claims = 0;
      while (tasks.hasNext()) {
        if (tasks.next().hasClaim()) {
          claims++;
        }
      }
      tasks.close();
      log.info("Sleeping while waiting for counter updates. Currently at {} " +
          "tasks and {} claims", sum, claims);
      Thread.sleep(1000);
    }
    long end = System.currentTimeMillis();
    log.info("Stopping dispatcher");
    for (TaskDispatcher dispatcher : dispatchers) {
      dispatcher.shutdown();
    }

    long time = end - start;
    long executionTime = (long)((double)TASK_COUNT / (double)count * (1.0 + ERROR_RATE))
        * TASK_SLEEP;
    long schedulingOverhead = time - executionTime;
    log.info("Total Time {} execution time {} scheduling overhead {}",
        time, executionTime, schedulingOverhead);

    // check that all counters are 0
    for (Map.Entry<String, AtomicInteger> entry : counters.entrySet()) {
      assertNotNull(entry.getKey(), "expected key");
      assertEquals(entry.getValue().intValue(), 1, "Expected 1 on " +
          entry.getKey());
    }

    log.info("TEST MULTIPLE ORCHESTRATOR END");
  }

  public static class UnreliableWorker implements TaskWorker {

    private double failureProbability = 0.0;
    private volatile TaskStatus status;
    private volatile Task task;
    private volatile TaskDispatcher dispatcher;
    private volatile boolean expectStop = false;

    public UnreliableWorker(double failureProbability) {
      this.failureProbability = failureProbability;
    }

    @Override
    public void reset() {
      status = TaskStatus.RESET;
      expectStop = false;
    }

    @Override
    public boolean isRelevant(Task task) {
      return true;
    }

    @Override
    public void setTask(Task task, TaskDispatcher dispatcher) {
      this.task = task;
      this.dispatcher = dispatcher;
    }

    @Override
    public TaskStatus getStatus() {
      return status;
    }

    @Override
    public void stop() {
      if (null != task && status == TaskStatus.DONE) {
        log.error("Rolling back {}", task.getUrn());
        counters.get(task.getPath()).decrementAndGet();
      }
      log.debug("Stop called");
      if (!expectStop) {
        log.error("Didn't expect stop. Called after done...");
      }
      expectStop = false;
      this.status = TaskStatus.STOPPED;
    }

    @Override
    public void run() {
      if (expectStop) {
        log.error("Expected to have stop called due to error");
      }
      status = TaskStatus.RUNNING;
      try {
        Thread.sleep(TASK_SLEEP);
      } catch (InterruptedException e) {
        log.error("Interrupted");
      }
      if (failureProbability > Math.random()) {
        status = TaskStatus.ERROR;
        expectStop = true;
        error.add(task.getPath());
        log.debug("Error On {}", task.getPath());
      } else {
        counters.get(task.getPath()).incrementAndGet();
        status = TaskStatus.DONE;
      }
    }
  }
}
