package net.sitemorph.queue;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

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
  public void testQueueSortOrder(TaskQueue queue) throws QueueException {
    // empty the queue
    while (null != queue.claim(tester, now, timeout)) {
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
    Task first = queue.claim(tester, now, timeout);
    assertEquals(first.getRunTime(), firstTime, "Expected earliest task first");
    Task second = queue.claim(tester, now, timeout);
    assertEquals(second.getRunTime(), middleTime, "Expected middle tassk time " +
        "second");
    Task third = queue.claim(tester, now, timeout);
    assertEquals(third.getRunTime(), lastTime, "Expected last time last");
    assertNull(queue.claim(tester, now, timeout), "Expected an empty queue");
  }
}
