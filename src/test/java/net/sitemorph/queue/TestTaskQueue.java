package net.sitemorph.queue;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

import net.sitemorph.protostore.CrudException;
import net.sitemorph.protostore.CrudIterator;
import net.sitemorph.protostore.CrudStore;
import net.sitemorph.protostore.InMemoryStore;
import net.sitemorph.protostore.SortOrder;
import net.sitemorph.queue.Message.Task;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.UUID;

/**
 * Test suite for basic queue operations.
 *
 * @author damien@sitemorph.net
 */
public class TestTaskQueue {

  UUID tester = UUID.randomUUID();
  long now = System.currentTimeMillis();
  long timeout = now + 60000;

  @DataProvider(name = "taskQueueImplementations")
  public Object[][] getTaskQueueImplementations() {
    CrudStore<Task> taskStore = new InMemoryStore.Builder<Task>()
        .setPrototype(Task.newBuilder())
        .setUrnField("urn")
        .setVectorField("vector")
        .addIndexField("runTime")
        .addIndexField("path")
        .setSortOrder("runTime", SortOrder.ASCENDING)
        .build();
    CrudTaskQueue queue = CrudTaskQueue.fromCrudStore(taskStore);
    return new Object[][] {{queue}};
  }

  @Test(dataProvider = "taskQueueImplementations")
  public void testQueueSortOrder(TaskQueue queue) throws QueueException, CrudException {
    // empty the queue
    CrudIterator<Task> tasks = queue.tasks();
    while (tasks.hasNext()) {
      queue.remove(tasks.next());
    }
    long firstTime = System.currentTimeMillis();
    long lastTime = firstTime + 10000;
    long middleTime = firstTime  + 5000;

    queue.push(Task.newBuilder()
        .setPath("/")
        .setRunTime(lastTime)
        .setUrn("c"));
    queue.push(Task.newBuilder()
        .setPath("/")
        .setRunTime(firstTime)
        .setUrn("a"));
    queue.push(Task.newBuilder()
        .setPath("/")
        .setRunTime(middleTime)
        .setUrn("b"));
    now = lastTime + 1000;
    timeout = now + 10000;
    tasks = queue.tasks();
    Task first = tasks.next();
    assertEquals(first.getRunTime(), firstTime, "Expected earliest task first");
    Task second = tasks.next();
    assertEquals(second.getRunTime(), middleTime, "Expected middle tassk time " +
        "second");
    Task third = tasks.next();
    assertEquals(third.getRunTime(), lastTime, "Expected last time last");
    assertFalse(tasks.hasNext(), "Expected an empty queue");
  }
}
