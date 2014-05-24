package net.sitemorph.queue;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import net.sitemorph.protostore.CrudStore;
import net.sitemorph.protostore.InMemoryStore;
import net.sitemorph.protostore.SortOrder;
import net.sitemorph.queue.Message.Task;

import com.beust.jcommander.internal.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import java.util.List;
import java.util.UUID;

/**
 * Tests for the task dispatcher.
 *
 * @author damien@sitemorph.net
 */
public class TaskDispatcherTest {

  private Logger log = LoggerFactory.getLogger("TaskDispatcherTest");
  UUID tester = UUID.randomUUID();
  private long now = System.currentTimeMillis();
  private long timeout = now + 60000;

  TaskDispatcher getDispatcher(final CrudTaskQueue queue, List<TaskWorker> workers)
      throws QueueException {
    TaskDispatcher.Builder builder = TaskDispatcher.newBuilder();

    builder.setSleepInterval(1000)
        .setTaskTimeout(10000)
        .setWorkerPoolSize(1)
        .setIdentity(tester)
        .setTaskQueueFactory(new TaskQueueFactory() {
          @Override
          public TaskQueue getTaskQueue() {
            return queue;
          }

          @Override
          public void returnTaskQueue(TaskQueue queue) {
          }
        });
    for (TaskWorker worker : workers) {
      builder.registerTaskWorker(worker);
    }
    return builder.build();
  }

  CrudTaskQueue getQueue() throws QueueException {
    CrudStore<Task> taskStore = new InMemoryStore.Builder<Task>()
        .setPrototype(Task.newBuilder())
        .setUrnField("urn")
        .setVectorField("vector")
        .addIndexField("runTime")
        .addIndexField("path")
        .setSortOrder("runTime", SortOrder.ASCENDING)
        .build();
    CrudTaskQueue queue = CrudTaskQueue.fromCrudStore(taskStore);
    queue.push(Task.newBuilder()
        .setPath("/")
        .setRunTime(System.currentTimeMillis())
        .setData("")
        .setUrn("abc"));
    return queue;
  }

  @Test(groups = "slowTest")
  public void testTasksRun() throws InterruptedException, QueueException {
    log.debug("TEST TASK RUN SHUTDOWN STARTING");
    List<TaskWorker> workers = Lists.newArrayList();
    TestTaskWorker worker = new TestTaskWorker();
    workers.add(worker);

    CrudTaskQueue queue = getQueue();

    TaskDispatcher dispatcher = getDispatcher(queue, workers);
    worker.setShutdownDispatcher(dispatcher);

    Thread thread = new Thread(dispatcher);
    thread.start();

    Thread.sleep(1500);

    assertTrue(worker.hasRun(), "Worker was not run");
    assertNotNull(queue.claim(tester, now, now + timeout),
        "Task list should still have the task " +
        "as the scheduler was shutdown before the run was complete");
    log.debug("TEST TASK RUN SHUTDOWN DONE");
  }

  @Test(groups = "slowTest")
  public void testTaskRunComplete()
      throws InterruptedException, QueueException {
    log.debug("TEST TASK RUN SUCCESSFUL STARTING");
    CrudTaskQueue queue = getQueue();
    List<TaskWorker> workers = Lists.newArrayList();
    NullTaskWorker worker = new NullTaskWorker();
    workers.add(worker);

    TaskDispatcher dispatcher = getDispatcher(queue, workers);
    // don't set the shudown dispatcher, just let it run
    Thread thread = new Thread(dispatcher);
    thread.start();

    Thread.sleep(2500);

    assertTrue(worker.getStatus() == TaskStatus.DONE,
        "Worker should have been run");
    assertNull(queue.claim(tester, now, timeout),
        "Task queue should be empty after success");
    dispatcher.shutdown();
    log.debug("TEST TASK RUN SUCCESSFUL DONE");

  }

  @Test(groups = "slowTest")
  public void testTwoTaskOneFailNoRun() throws QueueException, InterruptedException {
    CrudTaskQueue queue = getQueue();
    List<TaskWorker> workers = Lists.newArrayList();
    workers.add(new TestTaskWorker());
    workers.add(new TestTaskWorker());
    ((TestTaskWorker) workers.get(0)).setOverrideStatus(TaskStatus.STOPPED);

    TaskDispatcher dispatcher = getDispatcher(queue, workers);

    Thread thread = new Thread(dispatcher);
    thread.start();

    Thread.sleep(1500);

    log.debug("Test sleeping to wait for shutdown");
    dispatcher.shutdown();
    log.debug("Test shutdown complete");

    assertTrue(((TestTaskWorker) workers.get(0)).hasRun(), "Worker 0 was not run");
    assertTrue(((TestTaskWorker) workers.get(1)).hasRun(), "Worker 1 was not run");
    assertNotNull(queue.claim(tester, now, timeout),
        "Queue should not have been emptied");
  }

  @Test(groups = "slowTest")
  public void testFutureTaskNotRun()
      throws QueueException, InterruptedException {
    CrudTaskQueue queue = getQueue();
    // remove the current test task
    queue.claim(tester, now, timeout);

    Task future = queue.push(Task.newBuilder()
        .setPath("/")
        .setRunTime(System.currentTimeMillis() + 1000000)
        .setData("")
        .setUrn("abc"));

    List<TaskWorker> workers = Lists.newArrayList();
    workers.add(new TestTaskWorker());

    TaskDispatcher dispatcher = getDispatcher(queue, workers);

    Thread thread = new Thread(dispatcher);
    thread.start();

    Thread.sleep(1500);

    log.debug("Test sleeping to wait for shutdown");
    dispatcher.shutdown();
    log.debug("Test shutdown complete");

    assertFalse(((TestTaskWorker) workers.get(0)).hasRun(),
        "Worker 0 was not run");
    assertNotNull(queue.claim(tester, now, timeout),
        "Queue should not have been emptied");
  }

  @Test(groups = "slowTest")
  public void testTaskReturnsConnectionToFactoryOnEmpty()
      throws InterruptedException, QueueException {
    final CrudTaskQueue queue = getQueue();
    queue.claim(tester, now, timeout);
    List<TestTaskWorker> workers = Lists.newArrayList();
    workers.add(new TestTaskWorker());
    final boolean[] returnedQueue = new boolean[1];
    returnedQueue[0] = false;

    TaskDispatcher.Builder builder = TaskDispatcher.newBuilder();
    builder.setSleepInterval(1000)
        .setIdentity(tester)
        .setTaskTimeout(10000)
        .setWorkerPoolSize(1)
        .setTaskQueueFactory(new TaskQueueFactory() {
          @Override
          public TaskQueue getTaskQueue() {
            return queue;
          }

          @Override
          public void returnTaskQueue(TaskQueue queue) {
            returnedQueue[0] = true;
          }
        });
    for (TaskWorker worker : workers) {
      builder.registerTaskWorker(worker);
    }

    TaskDispatcher dispatcher = builder.build();

    Thread thread = new Thread(dispatcher);
    thread.start();

    Thread.sleep(1500);

    log.debug("Test sleeping to wait for shutdown");
    dispatcher.shutdown();
    log.debug("Test shutdown complete");

    assertTrue(returnedQueue[0], "Expected task queue to be returned");

  }

}
