package net.sitemorph.queue;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import net.sitemorph.protostore.CrudStore;
import net.sitemorph.protostore.InMemoryStore;
import net.sitemorph.protostore.SortOrder;
import net.sitemorph.queue.Message.Task;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Test suite for basic queue operations.
 *
 * @author damien@sitemorph.net
 */
public class TestTaskQueue {


  @DataProvider(name = "taskQueueImplementations")
  public Object[][] getTaskQueueImplementations() {
    CrudStore<Task> taskStore = new InMemoryStore.Builder<Task>()
        .setPrototype(Task.newBuilder())
        .setUrnField("urn")
        .addIndexField("runTime")
        .addIndexField("path")
        .setSortOrder("runTime", SortOrder.ASCENDING)
        .build();
    CrudTaskQueue queue = CrudTaskQueue.fromCrudStore(taskStore);
    return new Object[][] {{queue}};
  }

  @Test(dataProvider = "taskQueueImplementations")
  public void testQueueSortOrder(TaskQueue queue) throws QueueException {
    // empty the queue
    while (null != queue.peek()) {
      queue.pop();
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
    Task first = queue.pop();
    assertEquals(first.getRunTime(), firstTime, "Expected earliest task first");
    Task second = queue.pop();
    assertEquals(second.getRunTime(), middleTime, "Expected middle tassk time " +
        "second");
    Task third = queue.pop();
    assertEquals(third.getRunTime(), lastTime, "Expected last time last");
    assertNull(queue.peek(), "Expected an empty queue");
  }
}
