package net.sitemorph.queue;

import net.sitemorph.protostore.CrudException;
import net.sitemorph.protostore.CrudIterator;
import net.sitemorph.protostore.CrudStore;
import net.sitemorph.protostore.DbUrnFieldStore;
import net.sitemorph.protostore.SortOrder;
import net.sitemorph.queue.Message.Task;

import org.joda.time.DateTimeZone;

import java.sql.Connection;
import java.sql.SQLException;

import static org.joda.time.DateTime.now;

/**
 * The task queue builder constructs a task queue from a set of configuration
 * including a crud store. This class is simply a wrapper for a crud store with
 * convenience builder which only requires a connection and table name.
 *
 * @author damien@sitemorph.net
 */
public class CrudTaskQueue implements TaskQueue {

  private CrudStore<Task> taskStore;
  private Connection connection;

  private CrudTaskQueue(CrudStore<Task> taskStore, Connection connection) {
    this.taskStore = taskStore;
    this.connection = connection;
  }

  private CrudTaskQueue(CrudStore<Task> taskStore) {
    this.taskStore = taskStore;
  }

  public static CrudTaskQueue fromCrudStore(CrudStore<Task> taskStore) {
    return new CrudTaskQueue(taskStore);
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * @see TaskQueue#peek()
   */
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

  /**
   * @see TaskQueue#pop()
   */
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

  /**
   * @see TaskQueue#push(Task.Builder)
   */
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

  /**
   * @see TaskQueue#remove(Task)
   */
  @Override
  public void remove(Task task) throws QueueException {
    try {
      taskStore.delete(task);
    } catch (CrudException e) {
      throw new QueueException("Error removing task", e);
    }
  }

  /**
   * @see TaskQueue#tasks()
   */
  @Override
  public CrudIterator<Task> tasks() throws QueueException {
    try {
      return taskStore.read(Task.newBuilder());
    } catch (CrudException e) {
      throw new QueueException("Error getting task list", e);
    }
  }

  /**
   * @see TaskQueue#close()
   */
  @Override
  public void close() throws QueueException {
    try {
      taskStore.close();
    } catch (CrudException e) {
      throw new QueueException("Error closing task queue", e);
    }
    // also close the underlying connection if it is open.
    try {
      if (null != connection && !connection.isClosed()) {
        connection.close();
      }
    } catch (SQLException e) {
      throw new QueueException("Error closing SQL connection", e);
    }
  }

  /**
   * Build a crud task queue using concrete urn field store implementation.
   */
  public static class Builder {

    private Connection connection;

    private Builder() {
      taskStore = new DbUrnFieldStore.Builder<Task>();
    }

    private DbUrnFieldStore.Builder<Task> taskStore;

    /**
     * Set the name of the store table.
     *
     * @param tableName of the underlying table.
     * @return builder
     */
    public Builder setTableName(String tableName) {
      taskStore.setTableName(tableName);
      return this;
    }

    /**
     * Set the underlying SQL connector
     * @param connection to use to access the queue
     * @return builder
     */
    public Builder setConnection(Connection connection) {
      taskStore.setConnection(connection);
      this.connection = connection;
      return this;
    }

    /**
     * construct teh task queue.
     * @return the task queue
     * @throws QueueException on connector error or table and field name error
     */
    public CrudTaskQueue build() throws QueueException {
      try {
        taskStore
            .setPrototype(Task.newBuilder())
            .setUrnColumn("urn")
            .addIndexField("path")
            .setSortOrder("runTime", SortOrder.ASCENDING);
        CrudStore<Task> store = taskStore.build();
        return new CrudTaskQueue(store, connection);
      } catch (CrudException e) {
        throw new QueueException("Error initialising queue", e);
      }
    }
  }
}
