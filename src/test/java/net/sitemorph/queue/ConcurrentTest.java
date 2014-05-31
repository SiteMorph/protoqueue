package net.sitemorph.queue;

import net.sitemorph.protostore.CrudException;
import net.sitemorph.protostore.CrudStore;
import net.sitemorph.protostore.InMemoryStore.Builder;
import net.sitemorph.protostore.SortOrder;
import net.sitemorph.queue.Message.Task;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

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

  @BeforeTest
  public void resetTest()  {
    log.info("Creating counters");
    counters.clear();
    for (int i = 0; i < TASK_COUNT; i ++) {
      counters.put(UUID.randomUUID().toString(), new AtomicInteger(0));
    }
    error.clear();
    last = null;
    log.info("Done counters");
  }

  @Test(groups = "longTest")
  public void testSingleWorker() throws InterruptedException, CrudException {

    log.info("SINGLE WORKER START");
    final CrudStore<Task> store = new Builder<Task>()
        .setPrototype(Task.newBuilder())
        .setVectorField("vector")
        .setUrnField("urn")
        .setSortOrder("runTime", SortOrder.ASCENDING)
        .addIndexField("path")
        .build();
    long now = System.currentTimeMillis();
    log.info("Adding tasks to store");
    for (Map.Entry<String, AtomicInteger> entry : counters.entrySet()) {
      store.create(Task.newBuilder()
          .setPath(entry.getKey())
          .setRunTime(now++));
      last = entry.getKey();
    }
    log.info("Done adding tasks.");

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
        .registerTaskWorker(new UnreliableWorker(0.1))
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

    int sleeps = 0;
    while (0 == counters.get(last).intValue()) {
      int sum = 0;
      for (Map.Entry<String, AtomicInteger> entry : counters.entrySet()) {
        sum += entry.getValue().intValue();
      }
      log.info("Sleeping while waiting for counter updates. Currently at " +
          sum);
      Thread.sleep(1000);
      if (9 < sleeps++) {
        break;
      }
    }
    log.info("Stopping dispatcher");
    dispatcher.shutdown();

    // check that all counters are 0
    for (Map.Entry<String, AtomicInteger> entry : counters.entrySet()) {
      assertNotNull(entry.getKey(), "expected key");
      assertEquals(entry.getValue().intValue(), 1, "Expected 1 on " +
          entry.getKey());
    }

    log.info("SINGLE WORKER STOP");
  }

  @Test(groups = "longTest")
  public void testTwoOrchestrator() throws InterruptedException, CrudException {
    log.info("TEST TWO ORCHESTRATOR START");
    final CrudStore<Task> store = new Builder<Task>()
        .setPrototype(Task.newBuilder())
        .setVectorField("vector")
        .setUrnField("urn")
        .setSortOrder("runTime", SortOrder.ASCENDING)
        .addIndexField("path")
        .build();
    long now = System.currentTimeMillis();
    log.info("Adding tasks to store");
    for (Map.Entry<String, AtomicInteger> entry : counters.entrySet()) {
      store.create(Task.newBuilder()
          .setPath(entry.getKey())
          .setRunTime(now++));
      last = entry.getKey();
    }
    log.info("Done adding tasks.");

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
    log.info("Started dispatchers.");

    int sleeps = 0;
    while (0 == counters.get(last).intValue()) {
      int sum = 0;
      for (Map.Entry<String, AtomicInteger> entry : counters.entrySet()) {
        sum += entry.getValue().intValue();
      }
      log.info("Sleeping while waiting for counter updates. Currently at " +
          sum);
      Thread.sleep(1000);
      if (9 < sleeps++) {
        break;
      }
    }
    log.info("Stopping dispatcher");
    dispatcher1.shutdown();

    // check that all counters are 0
    for (Map.Entry<String, AtomicInteger> entry : counters.entrySet()) {
      assertNotNull(entry.getKey(), "expected key");
      assertEquals(entry.getValue().intValue(), 1, "Expected 1 on " +
          entry.getKey());
    }

    log.info("TEST TWO ORCHESTRATOR END");
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
      log.debug("Stop called");
      if (!expectStop) {
        throw new RuntimeException("Didn't expect stop");
      }
      expectStop = false;
      this.status = TaskStatus.STOPPED;
    }

    @Override
    public void run() {
      if (expectStop) {
        log.error("Expected to have stop called due to error");
        throw new RuntimeException("Error task expected stop call");
      }
      status = TaskStatus.RUNNING;
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
