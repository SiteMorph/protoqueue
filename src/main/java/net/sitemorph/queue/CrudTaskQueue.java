package net.sitemorph.queue;

import net.sitemorph.protostore.CrudException;
import net.sitemorph.protostore.CrudIterator;
import net.sitemorph.protostore.CrudStore;
import net.sitemorph.protostore.DbUrnFieldStore;
import net.sitemorph.protostore.SortOrder;
import net.sitemorph.queue.Message.Task;

import org.joda.time.DateTimeZone;

import java.sql.Connection;

import static org.joda.time.DateTime.now;

/**
 * The task queue builder constructs a task queue from a set of configuration
 * including a crud store. This class is simply a wrapper for a
 *
 * @author damien@sitemorph.net
 */
public class CrudTaskQueue implements TaskQueue {

  private CrudStore<Task> taskStore;


  public CrudTaskQueue(CrudStore<Task> taskStore) {
    this.taskStore = taskStore;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  @Override
  public Task peek() throws QueueException {
    try {
      CrudIterator<Task> tasks = taskStore.read(Task.newBuilder());
      Task result;
      if (tasks.hasNext()) {
        result = tasks.next();
      } else {
        result = null;
      }
      tasks.close();
      return result;
    } catch (CrudException e) {
      throw new QueueException("Storage error peeking at queue", e);
    }
  }

  @Override
  public Task pop() throws QueueException {
    try {
      CrudIterator<Task> tasks = taskStore.read(Task.newBuilder());
      Task result;
      if (tasks.hasNext()) {
        result = tasks.next();
        taskStore.delete(result);
      } else {
        result = null;
      }
      tasks.close();
      return result;
    } catch (CrudException e) {
      throw new QueueException("Storage error popping from queue", e);
    }
  }

  @Override
  public Task push(Task.Builder task) throws QueueException {
    try {
      if (!task.hasRunTime()) {
        task.setRunTime(now(DateTimeZone.UTC).getMillis());
      }
      return taskStore.create(task);
    } catch (CrudException e) {
      throw new QueueException("Error adding task to queue", e);
    }
  }

  @Override
  public void remove(Task task) throws QueueException {
    try {
      taskStore.delete(task);
    } catch (CrudException e) {
      throw new QueueException("Error removing task", e);
    }
  }

  @Override
  public CrudIterator<Task> tasks() throws QueueException {
    try {
      return taskStore.read(Task.newBuilder());
    } catch (CrudException e) {
      throw new QueueException("Error getting task list", e);
    }
  }

  @Override
  public void close() throws QueueException {
    try {
      taskStore.close();
    } catch (CrudException e) {
      throw new QueueException("Error closing task queue", e);
    }
  }

  public static class Builder {

    private Builder() {
      taskStore = new DbUrnFieldStore.Builder<Task>();
    }

    private DbUrnFieldStore.Builder<Task> taskStore;

    public Builder setTableName(String tableName) {
      taskStore.setTableName(tableName);
      return this;
    }

    public Builder setConnection(Connection connection) {
      taskStore.setConnection(connection);
      return this;
    }

    public CrudTaskQueue build() throws QueueException {
      try {
        taskStore
            .setPrototype(Task.newBuilder())
            .setUrnColumn("urn")
            .addIndexField("path")
            .setSortOrder("runTime", SortOrder.ASCENDING);
        CrudStore<Task> store = taskStore.build();
        return new CrudTaskQueue(store);
      } catch (CrudException e) {
        throw new QueueException("Error initialising queue", e);
      }
    }
  }
}
