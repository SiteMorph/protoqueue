package net.sitemorph.queue;

import net.sitemorph.protostore.CrudException;
import net.sitemorph.protostore.CrudIterator;
import net.sitemorph.protostore.CrudStore;
import net.sitemorph.protostore.InMemoryStore;
import net.sitemorph.protostore.SortOrder;
import net.sitemorph.queue.Message.Task;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

/**
 * Test the task picker to make sure that it picks the
 */
public class NaiveTaskPickerTest {

  private static final Logger log = LoggerFactory.getLogger(NaiveTaskPickerTest.class.getName());

  @Test
  public void testNaiveTaskPickerPicksFirstTask()
      throws CrudException, QueueException {
    NaiveTaskPicker picker = new NaiveTaskPicker();
    CrudStore<Task> tasks = new InMemoryStore.Builder<Task>()
        .setPrototype(Task.newBuilder())
        .setSortOrder(Task.getDescriptor().getFields().get(Task.RUNTIME_FIELD_NUMBER).getName(),
            SortOrder.ASCENDING)
        .setUrnField("urn")
        .setVectorField("vector")
        .build();

    long now = System.currentTimeMillis();

    tasks.create(Task.newBuilder()
        .setPath("/")
        .setRunTime(now - 1));
    tasks.create(Task.newBuilder()
        .setPath("/")
        .setRunTime(now - 2));
    tasks.create(Task.newBuilder()
        .setPath("/")
        .setRunTime(now - 2));
    tasks.create(Task.newBuilder()
        .setPath("/")
        .setRunTime(now - 2));

    log.info("Store says tasks are: ");
    CrudIterator<Task> taskList = tasks.read(Task.newBuilder());
    while (taskList.hasNext()) {
      Task current = taskList.next();
      log.info("Task {} is due at {}", current.getUrn(), current.getRunTime() - now);
    }
    taskList.close();

    Task first = tasks.read(Task.newBuilder()).next();
    Task picked = picker.pick(now, tasks);
    assertEquals(picked.getUrn(), first.getUrn(), "Earliest task was not picked");
  }
}
