package com.example.spanner;

import com.google.api.gax.longrunning.OperationFuture;
import com.google.cloud.spanner.*;
import com.google.spanner.admin.database.v1.CreateDatabaseMetadata;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ObservabilityLab {

  private static final int NUM_PROCESSORS = Runtime.getRuntime().availableProcessors();

  /**
   * This method creates Message table which will be used for monotonically increasing writes.
   *
   * @param dbAdminClient DatabaseAdminClient
   * @param id Database object
   */
  public void createMessageForIncreasingWrites(DatabaseAdminClient dbAdminClient, DatabaseId id) {
    createDatabase(
        dbAdminClient,
        id,
        Arrays.asList(
            "CREATE TABLE Message("
                + " msg_id STRING(MAX) NOT NULL,"
                + " body STRING(MAX),"
                + ") PRIMARY KEY (msg_id)"));
  }

  /**
   * This method writes the data into the Message table. Here primary key will be monotonically
   * increasing.
   *
   * @param dbClient DatabaseClient
   * @param mutationsPerTransaction mutations per transaction.
   * @param numMinutes How long this method should execute in minutes.
   */
  public void performMonotonicallyIncreasingWrite(
      DatabaseClient dbClient, int mutationsPerTransaction, int numMinutes) {
    Instant doneTime = Instant.now().plus(numMinutes, ChronoUnit.MINUTES);
    while (Instant.now().isBefore(doneTime)) {
      List<Mutation> mutations = new ArrayList<>();
      for (int i = 0; i < mutationsPerTransaction; ++i) {
        mutations.add(
            Mutation.newInsertBuilder("Message")
                .set("msg_id")
                .to(Instant.now().toString() + i + Thread.currentThread().getId())
                .set("body")
                .to(String.format("test-body-%s", i))
                .build());
      }
      try {
        dbClient.writeWithOptions(
            mutations,
            // Tag has a limit of 50 characters hence keeping the tag shorts
            Options.tag("app=lab3,env=dev,case=incremental,action=insert"));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  /** Calling the method for parallel execution */
  public void performMonotonicallyIncreasingWritesParallel(
      DatabaseClient dbClient, int mutationsPerTransaction, int numMinutes) {
    executeTasksInParallel(
        () -> performMonotonicallyIncreasingWrite(dbClient, mutationsPerTransaction, numMinutes),
        NUM_PROCESSORS);
  }

  /**
   * This method creates the Message and Mailbox table, these two table follows the parent and child
   * relationship without interleaved.
   *
   * @param dbAdminClient DatabaseAdminClient
   * @param id Database object
   */
  public void createMessageForMultiParticipantWrite(
      DatabaseAdminClient dbAdminClient, DatabaseId id) {
    createDatabase(
        dbAdminClient,
        id,
        Arrays.asList(
            "CREATE TABLE Message("
                + " msg_id STRING(MAX) NOT NULL,"
                + " body STRING(MAX),"
                + ") PRIMARY KEY (msg_id)"));
  }

  /**
   * Calling the method for parallel execution
   *
   * @param dbClient DatabaseClient
   * @param mutationsPerTransaction mutations per transaction.
   * @param numMinutes How long this method should execute in minutes.
   */
  public void performMultiParticipantWriteParallel(
      DatabaseClient dbClient, int mutationsPerTransaction, int numMinutes) {
    executeTasksInParallel(
        () -> performMultiParticipantWrite(dbClient, mutationsPerTransaction, numMinutes),
        NUM_PROCESSORS);
  }

  /**
   * Below method inserts the data in the table using random UUID value of primary. This method is
   * expected to generate a mutli participant write in the table. Initial few minutes are required
   * for warm up after which spanner may split the tablet into to or more. Here body is kept smaller
   * so that rows are written faster, and we can see the expected behaviour.
   *
   * @param dbClient DatabaseClient
   * @param mutationsPerTransaction mutations per transaction.
   * @param numMinutes How long this method should execute in minutes.
   */
  public void performMultiParticipantWrite(
      DatabaseClient dbClient, int mutationsPerTransaction, int numMinutes) {
    Instant doneTime = Instant.now().plus(numMinutes, ChronoUnit.MINUTES);
    while (Instant.now().isBefore(doneTime)) {
      List<Mutation> mutationsMessage = new ArrayList<>();
      for (int i = 0; i < mutationsPerTransaction; ++i) {
        mutationsMessage.add(
            Mutation.newInsertBuilder("Message")
                .set("msg_id")
                .to(UUID.randomUUID().toString())
                .set("body")
                .to(String.format("test-body-%s", i))
                .build());
      }
      dbClient.writeWithOptions(
          mutationsMessage, Options.tag("app=lab3,env=dev,case=multi-part,action=insert"));
    }
  }

  /**
   * This method creates Mailbox and Message table which are interleaved.
   *
   * @param dbAdminClient DatabaseAdminClient
   * @param id Database object
   */
  public void createInterleaved(DatabaseAdminClient dbAdminClient, DatabaseId id) {
    createDatabase(
        dbAdminClient,
        id,
        Arrays.asList(
            "CREATE TABLE Mailbox("
                + "sid INT64 NOT NULL,"
                + "state STRING(MAX),"
                + ") PRIMARY KEY (sid)",
            "CREATE TABLE Message("
                + "sid INT64 NOT NULL,"
                + "msg_id STRING(MAX) NOT NULL,"
                + "body STRING(MAX),"
                + "send_timestamp TIMESTAMP  OPTIONS (allow_commit_timestamp=true),"
                + ") PRIMARY KEY (sid, msg_id),"
                + "INTERLEAVE IN PARENT Mailbox ON DELETE CASCADE"));
  }

  /**
   * This method creates the WorkList table.
   *
   * @param dbAdminClient DatabaseAdminClient
   * @param id Database object
   */
  public void createWorkItems(DatabaseAdminClient dbAdminClient, DatabaseId id) {
    createDatabase(
        dbAdminClient,
        id,
        Arrays.asList(
            "CREATE TABLE WorkList("
                + " id STRING(MAX) NOT NULL,"
                + " is_done BOOL,"
                + " timestamp TIMESTAMP  OPTIONS (allow_commit_timestamp=true), "
                + ") PRIMARY KEY (id)"));
  }

  /**
   * This method inserts data in the worklist table which can also be called as work items
   *
   * @param dbClient DatabaseClient
   * @param mutationsPerTransaction mutations per transaction.
   * @param numWorkItems Number of work items to be written in the table. This is basically the
   *     number of rows should be inserted in the worklist table.
   */
  public void writeWorkItems(
      DatabaseClient dbClient, int numWorkItems, int mutationsPerTransaction) {
    // Finding the number of transaction to run the outer loop
    int numTransactions = numWorkItems / mutationsPerTransaction;

    for (int i = 0; i < numTransactions; ++i) {
      List<Mutation> mutations = new ArrayList<>();

      for (int j = 0; j < mutationsPerTransaction; ++j) {
        mutations.add(
            Mutation.newInsertBuilder("WorkList")
                .set("id")
                .to(UUID.randomUUID().toString())
                .set("is_done")
                .to(false)
                .set("timestamp")
                .to(Value.COMMIT_TIMESTAMP)
                .build());
      }
      try {
        dbClient.writeWithOptions(
            mutations, Options.tag("app=lab3,env=dev,case=WorkListItem,action=insert"));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  /** Calling the method for parallel execution */
  public void doWorkSingleTransactionParallel(DatabaseClient dbClient, boolean isLocking) {
    executeTasksInParallel(
        () -> doWorkSingleTransactionSerial(dbClient, isLocking), NUM_PROCESSORS);
  }

  /** Writing the value in the Mailbox table. */
  public void writeMailboxes(DatabaseClient dbClient, int numMailboxes) {
    List<Mutation> mutations = new ArrayList<>();
    for (int sid = 0; sid < numMailboxes; ++sid) {
      mutations.add(
          Mutation.newInsertBuilder("Mailbox")
              .set("sid")
              .to(sid)
              .build());
    }
    try {
      dbClient.writeWithOptions(
          mutations, Options.tag("app=lab3,env=dev,case=writeMailBox,action=insert"));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Parallel writing the values in the Messages table. Here Message table is interleaved in
   * Mailbox.
   *
   * @param numMailboxes number of Mailbox to be used to write values in interleaves Message table.
   * @param mutationsPerTransaction number of Mutations per transaction
   * @param numMinutes how long it should write.
   */
  public void writeMessagesInterleavedParallel(
      DatabaseClient dbClient, int numMailboxes, int mutationsPerTransaction, int numMinutes) {
    executeTasksInParallel(
        () -> writeMessagesInterleaved(dbClient, numMailboxes, mutationsPerTransaction, numMinutes),
        NUM_PROCESSORS);
  }

  /** Insert rows to Mailbox and Message tables which are interleaved */
  private void writeMessagesInterleaved(
      DatabaseClient dbClient, int numMailboxes, int mutationsPerTransaction, int numMinutes) {
    Instant doneTime = Instant.now().plus(numMinutes, ChronoUnit.MINUTES);

    while (Instant.now().isBefore(doneTime)) {
      for (int mailbox = 0; mailbox < numMailboxes; ++mailbox) {
        List<Mutation> mutations = new ArrayList<>();
        for (int i = 0; i < mutationsPerTransaction; ++i) {
          mutations.add(
              Mutation.newInsertBuilder("Message")
                  .set("sid")
                  .to(mailbox)
                  .set("msg_id")
                  .to(UUID.randomUUID().toString())
                  .set("body")
                  .to(String.format("test-body-%s", i))
                  .set("send_timestamp")
                  .to(Value.COMMIT_TIMESTAMP)
                  .build());
        }
        try {
          dbClient.writeWithOptions(
              mutations, Options.tag("app=lab3,env=dev,case=wr-interleave,action=insert"));
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    }
  }

  /**
   * Method to execute the transaction locking or non locking method.
   *
   * @param dbClient DatabaseClient
   * @param isLocking value to decide which method to call. True means locking method will be
   *     called, false means the otherwise.
   */
  private void doWorkSingleTransactionSerial(DatabaseClient dbClient, boolean isLocking) {
    boolean workRemaining = true;
    while (workRemaining) {
      workRemaining =
          isLocking
              ? doWorkSingleTransactionLocking(dbClient)
              : doWorkSingleTransactionNonLocking(dbClient);
    }
  }

  /*
  This method helps reproduce the Lock scenario in Spanner. This method will be executed from multiple thread
  and due to the nature of readWriteTransaction this kind of work can not be expedited because of parallelism.
  It's running the user query  which is using a limit 100 but eventually will lock the whole table due to the full table scan.
  We are also writing the rows value in the table in same transaction so only one thread at a time gets the lock to write
  will succeed.
   */
  private boolean doWorkSingleTransactionLocking(DatabaseClient dbClient) {

    String stmt = "SELECT * FROM WorkList WHERE is_done is false LIMIT 100";
    return Boolean.TRUE.equals(
        dbClient
            // In this logic read and write is happening in same transaction.
            // Setting the transaction tag for main transaction
            .readWriteTransaction(Options.tag("app=lab3,env=dev,case=locking-method"))
            .run(
                transaction -> {
                  List<String> idList = new ArrayList<>();
                  // Executing the above query to get 100 row where is_done is false, it will result
                  // a full table scan
                  ResultSet resultSet =
                      transaction.executeQuery(
                          Statement.of(stmt),
                          // Setting the transaction for specific action
                          Options.tag("app=lab3,env=dev,case=locking,action=select"));
                  // fetching the id value of each row to save in the list.
                  while (resultSet.next()) {
                    idList.add(resultSet.getString("id"));
                  }
                  resultSet.close();
                  // return false if there are no rows.
                  if (idList.size() == 0) return false;

                  // finding a random number between 1 and the size of list and using that
                  // to get the id at that position from the list. This is just a way to select a
                  // random id out of 100 to process. As it will be executed in multithreading
                  // environment each thread may get different rows as much as possible.
                  // One thread at a time will just update a single row.
                  int randIdx = new Random().nextInt(idList.size());
                  String id = idList.get(randIdx);
                  System.out.printf("Doing work: %s\n", id);
                  // updating the rows is_done value to true in the same transaction.
                  transaction.buffer(
                      Mutation.newUpdateBuilder("WorkList")
                          .set("id")
                          .to(id)
                          .set("is_done")
                          .to(true)
                          .set("timestamp")
                          .to(Value.COMMIT_TIMESTAMP)
                          .build());
                  return true;
                }));
  }

  /*
  This method helps reproduce the non Locking scenario, it's the correct way using read and write in Spanner. This method
  will be executed from multiple thread but here we are executing the limit query in separate read transaction(as it does the full table scan)
  and updating the rows value in separate readWriteTransaction. We are also performing a select to validate the row value but that select is on
  primary key column, so it does not do the full table scan and avoids get table level lock. It's running the user query
  which is using a limit 100 in separate read transaction.
   */
  private boolean doWorkSingleTransactionNonLocking(DatabaseClient dbClient) {

    String stmt = "SELECT * FROM WorkList WHERE is_done is false LIMIT 100";
    List<String> idList = new ArrayList<>();
    // Reading the data in seperate read only transaction.
    try (ReadOnlyTransaction transaction = dbClient.readOnlyTransaction()) {
      ResultSet queryResultSet =
          transaction.executeQuery(
              Statement.of(stmt),
              // Setting the transaction for specific action
              Options.tag("app=lab3,env=dev,case=non-locking,action=read-select"));
      // fetching the id value of each row to save in the list.
      while (queryResultSet.next()) {
        idList.add(queryResultSet.getString("id"));
      }
      queryResultSet.close();
    }

    // return false if there are no rows.
    if (idList.size() == 0) return false;

    // Finding a random number between 1 and the size of list and using that
    // to get the id at that position from the list. This is just a way to select a
    // random id out of 100 to process. As it will be executed in multithreading
    // environment each thread may get different rows as much as possible.
    int randIdx = new Random().nextInt(idList.size());
    String id = idList.get(randIdx);

    return Boolean.TRUE.equals(
        dbClient
            // starting a read write transaction
            .readWriteTransaction(Options.tag("app=lab3,env=dev,case=non-locking-method"))
            .run(
                transaction -> {
                  // Reading a specific row based on the primary key column Id from the table, but
                  // this will not lock the table as it's not the full table scan.
                  Struct idRow =
                      transaction.readRow(
                          "WorkList",
                          Key.of(id), // Read single row in a table.
                          Arrays.asList("id", "is_done", "timestamp"));

                  if (idRow == null) {
                    throw new RuntimeException("Table 'Worklist' is empty");
                  }

                  if (idRow.getBoolean("is_done")) return false;
                  // Updating the is_done value to true for the same row.
                  System.out.printf("Doing work: %s\n", id);
                  transaction.buffer(
                      Mutation.newUpdateBuilder("WorkList")
                          .set("id")
                          .to(idRow.getValue("id"))
                          .set("is_done")
                          .to(true)
                          .set("timestamp")
                          .to(Value.COMMIT_TIMESTAMP)
                          .build());
                  return true;
                }));
  }

  private void createDatabase(
      DatabaseAdminClient dbAdminClient, DatabaseId id, List<String> schema) {
    OperationFuture<Database, CreateDatabaseMetadata> op =
        dbAdminClient.createDatabase(id.getInstanceId().getInstance(), id.getDatabase(), schema);
    try {
      // Initiate the request which returns an OperationFuture.
      Database db = op.get();
      System.out.println("Created database [" + db.getId() + "]");
    } catch (ExecutionException e) {
      // If the operation failed during execution, expose the cause.
      throw (SpannerException) e.getCause();
    } catch (InterruptedException e) {
      // Throw when a thread is waiting, sleeping, or otherwise occupied,
      // and the thread is interrupted, either before or during the activity.
      throw SpannerExceptionFactory.propagateInterrupt(e);
    }
  }

  private void executeTasksInParallel(Runnable task, int numProcessors) {
    List<CompletableFuture<Void>> futures = new ArrayList<>();
    ExecutorService executorService = Executors.newFixedThreadPool(numProcessors);
    for (int i = 0; i < numProcessors; ++i) {
      final int threadCount = i;
      CompletableFuture<Void> future =
          CompletableFuture.runAsync(task, executorService)
              .exceptionally(
                  ex -> {
                    System.err.println(
                        "Error in Thread ::" + threadCount + " Exception::" + ex.getMessage());
                    return null;
                  });
      futures.add(future);
    }
    CompletableFuture<Void> allOf =
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
    try {
      allOf.get();
    } catch (InterruptedException | ExecutionException e) {
      e.printStackTrace();
    } finally {
      executorService.shutdown();
    }
  }
}
