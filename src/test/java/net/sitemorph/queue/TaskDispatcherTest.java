package net.sitemorph.queue;

import net.sitemorph.queue.Message.Task;

import com.beust.jcommander.internal.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import java.util.List;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

/**
 * Tests for the task dispatcher.
 *
 * @author damien@sitemorph.net
 */
public class TaskDispatcherTest {

  private Logger log = LoggerFactory.getLogger("TaskDispatcherTest");

  TaskDispatcher getDispatcher(final ListTaskQueue taskList,
      List<TestTaskWorker> workers) {
    TaskDispatcher.Builder builder = TaskDispatcher.newBuilder();
    builder.setSleepInterval(1000)
        .setTaskTimeout(10000)
        .setWorkerPoolSize(1)
        .setTaskQueueFactory(new TaskQueueFactory() {
          @Override
          public TaskQueue getTaskQueue() {
            return taskList;
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

  ListTaskQueue getSingleTask() {
    List<Task> taskList = Lists.newArrayList();
    taskList.add(Task.newBuilder()
        .setPath("/")
        .setRunTime(System.currentTimeMillis())
        .setData("")
        .setUrn("abc")
        .build());
    return new ListTaskQueue(taskList);
  }

  @Test(groups = "slowTest")
  public void testTasksRun() throws InterruptedException, QueueException {
    ListTaskQueue singleTask = getSingleTask();
    List<TestTaskWorker> workers = Lists.newArrayList();
    TestTaskWorker worker = new TestTaskWorker();
    workers.add(worker);

    TaskDispatcher dispatcher = getDispatcher(singleTask, workers);
    worker.setShutdownDispatcher(dispatcher);

    Thread thread = new Thread(dispatcher);
    thread.start();

    Thread.sleep(1500);

    assertTrue(worker.hasRun(), "Worker was not run");
    assertNull(singleTask.peek(), "Task list should be empty");
    log.debug("TEST TASK RUN DONE");
  }

  @Test(groups = "slowTest")
  public void testTwoTaskOneFailNoRun() throws QueueException, InterruptedException {
    ListTaskQueue singleTask = getSingleTask();
    List<TestTaskWorker> workers = Lists.newArrayList();
    workers.add(new TestTaskWorker());
    workers.add(new TestTaskWorker());
    workers.get(0).setOverrideStatus(TaskStatus.STOPPED);

    TaskDispatcher dispatcher = getDispatcher(singleTask, workers);

    Thread thread = new Thread(dispatcher);
    thread.start();

    Thread.sleep(1500);

    log.debug("Test sleeping to wait for shutdown");
    dispatcher.shutdown();
    log.debug("Test shutdown complete");

    assertTrue(workers.get(0).hasRun(), "Worker 0 was not run");
    assertTrue(workers.get(1).hasRun(), "Worker 1 was not run");
    assertNotNull(singleTask.peek(), "Queue should not have been emptied");
  }

  @Test(groups = "slowTest")
  public void testFutureTaskNotRun()
      throws QueueException, InterruptedException {

    List<Task> taskList = Lists.newArrayList();
    taskList.add(Task.newBuilder()
        .setPath("/")
        .setRunTime(System.currentTimeMillis() + 1000000)
        .setData("")
        .setUrn("abc")
        .build());
    ListTaskQueue singleTask = new ListTaskQueue(taskList);

    List<TestTaskWorker> workers = Lists.newArrayList();
    workers.add(new TestTaskWorker());

    TaskDispatcher dispatcher = getDispatcher(singleTask, workers);

    Thread thread = new Thread(dispatcher);
    thread.start();

    Thread.sleep(1500);

    log.debug("Test sleeping to wait for shutdown");
    dispatcher.shutdown();
    log.debug("Test shutdown complete");

    assertFalse(workers.get(0).hasRun(), "Worker 0 was not run");
    assertNotNull(singleTask.peek(), "Queue should not have been emptied");
  }

  @Test(groups = "slowTest")
  public void testTaskReturnsConnectionToFactoryOnEmpty()
      throws InterruptedException {
    final List<Task> taskList = Lists.newArrayList();
    final ListTaskQueue singleTask = new ListTaskQueue(taskList);
    List<TestTaskWorker> workers = Lists.newArrayList();
    workers.add(new TestTaskWorker());
    final boolean[] returnedQueue = new boolean[1];
    returnedQueue[0] = false;

    TaskDispatcher.Builder builder = TaskDispatcher.newBuilder();
    builder.setSleepInterval(1000)
        .setTaskTimeout(10000)
        .setWorkerPoolSize(1)
        .setTaskQueueFactory(new TaskQueueFactory() {
          @Override
          public TaskQueue getTaskQueue() {
            return singleTask;
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
