package com.example.spanner;

import com.google.api.gax.longrunning.OperationFuture;
import com.google.cloud.spanner.*;
import com.google.spanner.admin.database.v1.CreateDatabaseMetadata;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.*;

/**
 * This class contains all the method and helper method for performing the steps defined for Spanner
 * Lab1
 */
public class CodeLab1 {

  private static final String LOREMIPSUM =
      "Aenean sit amet lectus est. Nullam ornare ligula luctus auctor placerat. Morbi fermentum volutpat massa, sit amet consectetur metus vehicula sed. Sed auctor scelerisque tempus. Morbi hendrerit tortor et felis scelerisque, at fermentum risus posuere. Suspendisse finibus, tellus eu ullamcorper posuere, odio ex pulvinar est, a malesuada libero turpis quis massa. Quisque placerat lacus orci. Proin ac viverra metus. Lorem ipsum dolor sit amet, consectetur adipiscing elit. Phasellus cursus rhoncus iaculis. Pellentesque nisl tellus, venenatis nec erat sit amet, eleifend porttitor nisl. Maecenas fringilla ex id mauris facilisis, sed luctus dui elementum. Suspendisse ac orci lectus. Suspendisse vitae sapien convallis, commodo leo ut, ultricies arcu. Fusce pellentesque sem vestibulum, sodales purus eget, auctor odio. Ut et nunc metus. Aenean ac ex faucibus, tristique nibh ut, euismod lorem. Fusce a ex ut nibh consectetur mollis. Aenean suscipit elit dui, faucibus vestibulum leo commodo a. Nulla ultricies vitae velit cursus commodo. Morbi eu sapien in magna condimentum porta quis eget sem. Etiam tempor auctor diam, quis mollis odio scelerisque et. Fusce tempus mauris mi, et varius enim condimentum in. Aliquam nisi lorem, pulvinar id ullamcorper vitae, fringilla vel leo. Fusce vehicula tincidunt vulputate. Vivamus efficitur nunc quis lorem porttitor elementum. Donec ex neque, vestibulum nec mollis quis, lacinia quis dui. Nullam rhoncus quis lacus nec euismod. Vivamus porttitor sem nec nisl auctor ultrices. Vivamus non laoreet lectus. Aliquam condimentum semper libero eu elementum. Nullam lobortis ultricies gravida. Integer in lacinia lacus, ac consequat magna. Suspendisse et risus vel diam facilisis ornare a in arcu. Nulla nec nunc sem. Cras aliquam nulla sem, luctus maximus est gravida ut. Ut pellentesque pharetra convallis. Quisque molestie, ipsum sit amet scelerisque convallis, magna ante fringilla massa, in blandit turpis nibh ornare magna. Curabitur mi tortor, feugiat id sem ac, scelerisque congue tortor. Aenean non viverra risus. Praesent vel enim quis dolor auctor aliquet. Maecenas faucibus mi at venenatis suscipit. Integer interdum magna vitae mauris interdum, laoreet ullamcorper erat tincidunt. Morbi vel ipsum convallis, semper quam vitae, sagittis tellus. In facilisis eu lorem imperdiet laoreet. Suspendisse gravida a magna et condimentum. Suspendisse vitae risus vitae est pulvinar convallis at sit amet sem. Morbi vel imperdiet leo, sit amet cursus urna. Pellentesque suscipit ut neque non laoreet. Duis non ipsum ipsum. Quisque ut porttitor dui. Duis nulla augue, varius quis tellus sed, efficitur bibendum justo. Cras vestibulum congue ante in gravida. Quisque tincidunt nisi nisl, sed vulputate leo luctus ac. Pellentesque quis tempor leo, sed faucibus ipsum. Donec rutrum turpis nec auctor lobortis. Pellentesque habitant morbi tristique senectus et netus et malesuada fames ac turpis egestas. Sed lobortis sodales risus vitae eleifend. Nullam pharetra dapibus lobortis. Vestibulum nisl diam, dignissim sit amet lacus sit amet, venenatis auctor nisi. Phasellus sollicitudin tortor a mi fringilla, imperdiet tristique ex feugiat. Nulla ut vehicula metus, nec tempus odio. Phasellus pellentesque lacus lacus, fringilla egestas felis hendrerit ac. Nam suscipit orci eros, at molestie nunc tincidunt id. Aliquam maximus finibus leo. Vestibulum orci turpis, fringilla eget lorem id, cursus consequat tortor. Suspendisse efficitur purus lectus, ac ornare sem lobortis a. Interdum et malesuada fames ac ante ipsum primis in faucibus. Sed sodales id mi eu aliquam. Curabitur enim libero, tempor vel vestibulum id, eleifend at lacus. Maecenas ut feugiat justo, nec dictum justo. Nulla porttitor accumsan rhoncus. Cras ligula velit, molestie sit amet molestie sed, commodo et libero. Etiam eu condimentum arcu. Aliquam erat volutpat. Nullam sit amet urna ipsum. Vestibulum at velit id velit tincidunt ultricies a ut metus. In vitae mollis diam. Proin lacinia fringilla purus vitae aliquet. Vestibulum a tincidunt eros. Aenean imperdiet aliquet arcu, vitae euismod turpis fermentum quis. Orci varius natoque penatibus et magnis dis parturient montes, nascetur ridiculus mus. Suspendisse potenti. Pellentesque quis efficitur mauris, pulvinar sagittis ante. Cras efficitur porta convallis. Nullam eleifend congue finibus. Nam sodales tincidunt odio ac elementum. Sed sem risus, imperdiet quis suscipit eu, imperdiet vitae nibh. Morbi gravida neque ac sodales varius. In convallis massa vel lectus fermentum ultricies ac vitae eros. Sed justo sem, dignissim sit amet tempus ultrices, pellentesque at libero. Quisque a quam volutpat, tristique erat a, auctor enim. Suspendisse finibus arcu erat, nec mattis tortor facilisis in. Donec ornare mattis lorem, in interdum turpis venenatis vel. Mauris tempus porttitor libero. Pellentesque habitant morbi tristique senectus et netus et malesuada fames ac turpis egestas. Vivamus eget tempor dui. Nunc et elit eu eros tincidunt sapien.";

  private static final String SUBJECT = "Random subject for the email for testing purpose %s";

  public void hubbleCreateMessages(DatabaseAdminClient dbAdminClient, DatabaseId id) {
    hubbleCreateDatabase(
        dbAdminClient,
        id,
        Collections.singletonList(
            "CREATE TABLE Message("
                + " msg_id STRING(MAX) NOT NULL,"
                + " body STRING(MAX),"
                + ") PRIMARY KEY (msg_id)"));
    System.out.println("Finished execution of hubbleCreateMessages()");
  }

  public void hubbleDoWorkSingleTransactionParallelLocking(DatabaseClient dbClient) {
    // getting the available processor
    int numProcessors = Runtime.getRuntime().availableProcessors();
    ExecutorService executorService = Executors.newFixedThreadPool(numProcessors);

    List<Future<Integer>> futures = new ArrayList<>();
    // List to store the count of total number of workItem(row) which has not yet completed means
    // is_done is false.
    List<Integer> workItemCount = new ArrayList<>();
    // Below logic is reading the generated_value for the rows which are having is_done=false. We
    // have added generated_value column value in incremented order starting from 1 in the table.
    // This will help us to assign executing the random workItem(rows) work without overlapping. As
    // each thread will pick up different generated_value to query.
    try (ResultSet resultSet =
        dbClient
            .singleUse() // Execute a single read or query against Cloud Spanner.
            // Below query can be parameterized to but kept here for simplicity
            .executeQuery(
                Statement.of("SELECT generated_value FROM WorkList WHERE is_done is false"))) {
      while (resultSet.next()) {
        workItemCount.add((int) resultSet.getLong(0));
      }
    }

    if (workItemCount.size() == 0) {
      System.out.println(
          "There is no workItem left to process, please update the is_done value to false, or add more work item in the table");
      return;
    }

    // Assigning each thread the amount of work equally
    for (int i = 0; i < numProcessors; ++i) {
      // Each thread get the index from this loop, total workItem to be processed and number of
      // thread (numProcessors) available for processing.
      // E.g In case of 10 threads and 30 workItem the assignment for this method would be like

      // Thread 1: ThreadIndex(i) 0, workitem 30
      // Thread 2: ThreadIndex(i) 1  workitem 30
      // Thread 3: ThreadIndex(i) 2  workitem 30
      // ...
      // Thread 10: ThreadIndex(i) 9  workitem 30
      // These values will be determined by received by each thread(an object of WorkItemProcessor)
      // while executing based on the input values passed here.
      String LOCK_QUERY_IN_TRANSACTION =
          "SELECT * FROM WorkList WHERE is_done is false and generated_value= %s;";
      Callable<Integer> callable =
          new WorkItemProcessor<>(
              workItemCount, i, numProcessors, dbClient, LOCK_QUERY_IN_TRANSACTION);
      // Submitting each thread
      Future<Integer> future = executorService.submit(callable);
      futures.add(future);
    }
    int totalProcessed = 0;
    // Calling the get on the future to wait for thread to finish and get the result.
    try {
      for (Future<Integer> future : futures) {

        totalProcessed += future.get();
      }

      System.out.printf("Total rows processed:%d ", totalProcessed);
      executorService.shutdown();
      // Waiting for all thread to finish
      executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
    System.out.println("Finished execution of hubbleDoWorkSingleTransactionParallelLocking()");
  }

  public void hubbleDoWorkSingleTransactionParallelNonLocking(DatabaseClient dbClient) {
    // getting the available processor
    int numProcessors = Runtime.getRuntime().availableProcessors();
    ExecutorService executorService = Executors.newFixedThreadPool(numProcessors);

    List<Future<Integer>> futures = new ArrayList<>();
    // This is the store the count of total number of workItem(row) which has not yet completed
    // means  is_done is false.
    List<String> workItemCount = new ArrayList<>();
    // Below logic is reading the id(primary key) for the rows which are having is_done=false. This
    // will help us to read the different ids by different thread randomly.
    try (ResultSet resultSet =
        dbClient
            .singleUse() // Execute a single read or query against Cloud Spanner.
            .executeQuery(Statement.of("SELECT id FROM WorkList WHERE is_done is false;"))) {
      while (resultSet.next()) {
        workItemCount.add(resultSet.getString(0));
      }
    }

    if (workItemCount.size() == 0) {
      System.out.println(
          "There is no workItem left to process, please update the is_done value to false, or add more work item in the table");
      return;
    }

    // Assigning each thread the amount of work equally
    for (int i = 0; i < numProcessors; ++i) {
      // Each thread get the index from this loop, total workItem to be processed and number of
      // thread (numProcessors) available for processing.
      // E.g In case of 10 threads and 30 workItem the assignment for this method would be like

      // Thread 1: ThreadIndex(i) 0, workitem 30
      // Thread 2: ThreadIndex(i) 1  workitem 30
      // Thread 3: ThreadIndex(i) 2  workitem 30
      // ...
      // Thread 10: ThreadIndex(i) 9  workitem 30
      // These values will be determined by received by each thread(an object of WorkItemProcessor)
      // while executing based on the input values passed here.
      String NON_LOCK_QUERY_IN_TRANSACTION =
          "SELECT * FROM WorkList WHERE is_done is false and id= '%s';";
      Callable<Integer> callable =
          new WorkItemProcessor<>(
              workItemCount, i, numProcessors, dbClient, NON_LOCK_QUERY_IN_TRANSACTION);
      // Submitting each thread
      Future<Integer> future = executorService.submit(callable);
      futures.add(future);
    }
    int totalProcessed = 0;
    // Calling the get on the future to wait for thread to finish and get the result.
    try {
      for (Future<Integer> future : futures) {

        totalProcessed += future.get();
      }

      System.out.printf("Total rows processed:%d ", totalProcessed);
      executorService.shutdown();
      executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
    System.out.println("Finished execution of hubbleDoWorkSingleTransactionParallelNonLocking()");
  }

  public void hubbleWriteMailboxes(DatabaseClient dbClient, int numMailboxes) {
    List<Mutation> mutations = new ArrayList<>();
    for (int sid = 0; sid < numMailboxes; ++sid) {
      mutations.add(
          Mutation.newInsertBuilder("Mailbox")
              .set("sid")
              .to(sid)
              .set("guid")
              .to(UUID.randomUUID().toString())
              .build());
    }
    try {
      dbClient.write(mutations);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    System.out.println("Finished execution of hubbleWriteMailboxes()");
  }

  public void hubbleWriteMessagesInterleavedParallel(
      DatabaseClient dbClient, int numMailboxes, int mutationsPerTransaction, int numMinutes) {
    int numProcessors = Runtime.getRuntime().availableProcessors();
    ExecutorService executorService = Executors.newFixedThreadPool(numProcessors);
    executeTasksInParallel(
        () ->
            hubbleWriteMessagesInterleaved(
                dbClient, numMailboxes, mutationsPerTransaction, numMinutes),
        executorService);
    System.out.println("Finished execution of hubbleWriteMessagesInterleavedParallel()");
  }

  public void hubbleWriteMessagesInterleavedParallel100k(
      DatabaseClient dbClient,
      int numMailboxes,
      int mutationsPerTransaction,
      int numberOfMessages) {
    int numProcessors = Runtime.getRuntime().availableProcessors();
    ExecutorService executorService = Executors.newFixedThreadPool(numProcessors);
    System.out.println(
        "Starting hubbleWriteMessagesInterleavedParallel100k with parallel thread count::"
            + numProcessors);
    final int mutationsPerTransactionTemp = numberOfMessages / numMailboxes / numProcessors;

    executeTasksInParallel(
        () ->
            hubbleWriteMessagesInterleaved100K(dbClient, numMailboxes, mutationsPerTransactionTemp),
        executorService);
    System.out.println("Finished execution of hubbleWriteMessagesInterleavedParallel100k()");
  }

  public void hubbleWriteMessagesInterleaved100K(
      DatabaseClient dbClient, int numMailboxes, int mutationsPerTransaction) {
    System.out.println("Starting execution of hubbleWriteMessagesInterleaved100K");
    long startTime = System.currentTimeMillis();
    for (int mailbox = 0; mailbox < numMailboxes; ++mailbox) {

      List<Mutation> mutations = new ArrayList<>();
      for (int i = 0; i < mutationsPerTransaction; ++i) {
        mutations.add(
            Mutation.newInsertBuilder("Message")
                .set("sid")
                .to(mailbox)
                .set("msg_id")
                .to(UUID.randomUUID().toString())
                .set("subject")
                .to(String.format(SUBJECT, i))
                .set("body")
                .to(LOREMIPSUM)
                .set("send_timestamp")
                .to(Value.COMMIT_TIMESTAMP)
                .build());
        System.out.println("mutationsPerTransaction value::" + i);
      }
      System.out.println("Mailbox value::" + mailbox);
      long startTime1 = System.currentTimeMillis();
      dbClient.write(mutations);
      System.out.println(
          "Size in MB"
              + (mutations.size() * 5170) / (1024 * 1024)
              + " Size of the mutation list:"
              + mutations.size());
      System.out.println(
          "Total time taken in finishing mutation write in database:"
              + (System.currentTimeMillis() - startTime1));
    }
    System.out.println(
        "Total time taken in finishing the thread forloop is:"
            + (System.currentTimeMillis() - startTime));
    System.out.println("Finished execution of hubbleWriteMessagesInterleaved100K");
  }

  public void hubbleUpdatesAndReads(
      DatabaseClient dbClient,
      int numMailboxes,
      int mutationsPerTransaction,
      int numRows,
      int numMinutes,
      boolean isStrong) {
    List<List<String>> keys = new ArrayList<>(numMailboxes);
    for (int i = 0; i < numMailboxes; ++i) {
      keys.add(new ArrayList<>(numRows / numMailboxes));
    }

    for (int sid = 0; sid < numMailboxes; ++sid) {
      for (int i = 0; i < numRows / numMailboxes / mutationsPerTransaction; ++i) {
        List<Mutation> mutations = new ArrayList<>();
        for (int j = 0; j < mutationsPerTransaction; ++j) {
          String uuid = UUID.randomUUID().toString();
          mutations.add(
              Mutation.newInsertBuilder("Message")
                  .set("sid")
                  .to(sid)
                  .set("msg_id")
                  .to(uuid)
                  .set("body")
                  .to(LOREMIPSUM)
                  .build());
          keys.get(sid).add(uuid);
        }
        dbClient.write(mutations);
      }
    }

    System.out.println("Beginning workload.");
    int numProcessors = Runtime.getRuntime().availableProcessors() / 2;
    ExecutorService writeExecutor = Executors.newFixedThreadPool(numProcessors);
    ExecutorService readExecutor = Executors.newFixedThreadPool(numProcessors);
    executeTasksInParallel(
        () -> hubbleUpdates(dbClient, keys, mutationsPerTransaction, numMinutes), writeExecutor);
    executeTasksInParallel(
        () -> hubbleReads(dbClient, keys, mutationsPerTransaction, numMinutes, isStrong),
        readExecutor);
    System.out.println("Finished execution of hubbleUpdatesAndReads()");
  }

  public void hubbleWriteWorkItems(
      DatabaseClient dbClient, int numWorkItems, int mutationsPerTransaction) {
    int numTransactions = numWorkItems / mutationsPerTransaction;
    int generatedValueCount = 1;
    for (int i = 0; i < numTransactions; ++i) {
      List<Mutation> mutations = new ArrayList<>();
      for (int j = 0; j < mutationsPerTransaction; ++j) {
        mutations.add(
            Mutation.newInsertBuilder("WorkList")
                .set("id")
                .to(UUID.randomUUID().toString())
                .set("is_done")
                .to(false)
                .set("generated_value")
                .to(generatedValueCount)
                .set("timestamp")
                .to(Value.COMMIT_TIMESTAMP)
                .build());
        generatedValueCount++;
      }
      try {
        dbClient.write(mutations);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    System.out.println("Finished execution of hubbleWriteWorkItems()");
  }

  public void hubbleWriteMessagesParallel(
      DatabaseClient dbClient, int mutationsPerTransaction, int numMinutes) {
    int numProcessors = Runtime.getRuntime().availableProcessors();
    ExecutorService executorService = Executors.newFixedThreadPool(numProcessors);
    executeTasksInParallel(
        () -> hubbleWriteMessages(dbClient, mutationsPerTransaction, numMinutes), executorService);
    System.out.println("Finished execution of hubbleWriteMessagesParallel()");
  }

  public void hubbleWriteMessagesParallelUUID(
      DatabaseClient dbClient, int mutationsPerTransaction, int numMinutes) {
    int numProcessors = Runtime.getRuntime().availableProcessors();
    ExecutorService executorService = Executors.newFixedThreadPool(numProcessors);
    executeTasksInParallel(
        () -> hubbleWriteMessagesUUID(dbClient, mutationsPerTransaction, numMinutes),
        executorService);
    System.out.println("Finished execution of hubbleWriteMessagesParallelUUID()");
  }

  public void hubbleWriteMessages(
      DatabaseClient dbClient, int mutationsPerTransaction, int numMinutes) {
    Instant doneTime = Instant.now().plus(numMinutes, ChronoUnit.MINUTES);
    while (Instant.now().isBefore(doneTime)) {
      List<Mutation> mutations = new ArrayList<>();
      for (int i = 0; i < mutationsPerTransaction; ++i) {
        mutations.add(
            Mutation.newInsertBuilder("Message")
                .set("msg_id")
                .to(Instant.now().toString() + i)
                .set("body")
                .to(LOREMIPSUM)
                .build());
      }
      try {
        dbClient.write(mutations);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    System.out.println("Finished execution of hubbleWriteMessages()");
  }

  public void hubbleCreateInterleaved(DatabaseAdminClient dbAdminClient, DatabaseId id) {
    hubbleCreateDatabase(
        dbAdminClient,
        id,
        Arrays.asList(
            "CREATE TABLE Mailbox("
                + "sid INT64 NOT NULL,"
                + "guid STRING(MAX),\n"
                + "state STRING(MAX),\n"
                + ") PRIMARY KEY (sid)\n",
            "CREATE TABLE Message(\n"
                + "\tsid INT64 NOT NULL,\n"
                + "\tmsg_id STRING(MAX) NOT NULL,\n"
                + "\tsubject STRING(MAX),\n"
                + "\tbody STRING(MAX),\n"
                + "\tsend_timestamp TIMESTAMP  OPTIONS (allow_commit_timestamp=true),\n"
                + ") PRIMARY KEY (sid, msg_id),\n"
                + "INTERLEAVE IN PARENT Mailbox ON DELETE CASCADE\n"));

    System.out.println("Finished execution of hubbleCreateInterleaved()");
  }

  public void hubbleCreateWorkItems(DatabaseAdminClient dbAdminClient, DatabaseId id) {
    hubbleCreateDatabase(
        dbAdminClient,
        id,
        Collections.singletonList(
            "CREATE TABLE WorkList("
                + " id STRING(MAX) NOT NULL,"
                + " is_done BOOL,"
                + "generated_value Int64,"
                + " timestamp TIMESTAMP  OPTIONS (allow_commit_timestamp=true), "
                + ") PRIMARY KEY (id)"));
    System.out.println("Finished execution of hubbleCreateWorkItems()");
  }

  private void executeTasksInParallel(Runnable task, ExecutorService executorService) {
    int numProcessors = ((ThreadPoolExecutor) executorService).getMaximumPoolSize();
    System.out.println(
        "Inside executeTasksInParallel, total number of thread will be :" + numProcessors);
    List<CompletableFuture<Void>> futures = new ArrayList<>();
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
    System.out.println("Submitted all the threads for execution");
    CompletableFuture<Void> allOf =
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
    try {
      allOf.get(); // Wait for all threads to complete
      System.out.println("All threads have finished.");
    } catch (InterruptedException | ExecutionException e) {
      e.printStackTrace();
    } finally {
      executorService.shutdown();
    }
    System.out.println("Finished execution of executeTasksInParallel()");
  }

  private void hubbleWriteMessagesUUID(
      DatabaseClient dbClient, int mutationsPerTransaction, int numMinutes) {
    Instant doneTime = Instant.now().plus(numMinutes, ChronoUnit.MINUTES);
    while (Instant.now().isBefore(doneTime)) {
      List<Mutation> mutations = new ArrayList<>();
      for (int i = 0; i < mutationsPerTransaction; ++i) {
        mutations.add(
            Mutation.newInsertBuilder("Message")
                .set("msg_id")
                .to(UUID.randomUUID().toString())
                .set("body")
                .to(LOREMIPSUM)
                .build());
      }
      try {
        dbClient.write(mutations);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    System.out.println("Finished execution of hubbleWriteMessagesUUID()");
  }

  private void hubbleUpdates(
      DatabaseClient dbClient,
      List<List<String>> keys,
      int mutationsPerTransaction,
      int numMinutes) {
    Instant doneTime = Instant.now().plus(numMinutes, ChronoUnit.MINUTES);
    Random random = new Random();
    while (Instant.now().isBefore(doneTime)) {
      List<Mutation> mutations = new ArrayList<>();
      int sid = random.nextInt(keys.size());
      for (int i = 0; i < mutationsPerTransaction; ++i) {
        String uuid = keys.get(sid).get(random.nextInt(keys.get(sid).size()));
        mutations.add(
            Mutation.newUpdateBuilder("Message")
                .set("sid")
                .to(sid)
                .set("msg_id")
                .to(uuid)
                .set("body")
                .to("updated!")
                .build());
      }
      try {
        dbClient.write(mutations);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    System.out.println("Finished execution of hubbleUpdates()");
  }

  private void hubbleReads(
      DatabaseClient dbClient,
      List<List<String>> keys,
      int mutationsPerTransaction,
      int numMinutes,
      boolean isStrong) {
    Instant doneTime = Instant.now().plus(numMinutes, ChronoUnit.MINUTES);
    Random random = new Random();
    while (Instant.now().isBefore(doneTime)) {
      KeySet.Builder keySetBuilder = KeySet.newBuilder();
      int sid = random.nextInt(keys.size());
      for (int i = 0; i < mutationsPerTransaction; ++i) {
        String uuid = keys.get(sid).get(random.nextInt(keys.get(sid).size()));
        keySetBuilder.addKey(Key.of(sid, uuid));
      }
      if (isStrong) {
        ResultSet throwaway =
            dbClient
                .singleUse()
                .read("Message", keySetBuilder.build(), Collections.singletonList("body"));
        while (throwaway.next()) {}
      } else {
        ResultSet throwaway =
            dbClient
                .singleUse(TimestampBound.ofMaxStaleness(15, TimeUnit.SECONDS))
                .read("Message", keySetBuilder.build(), Collections.singletonList("body"));
        while (throwaway.next()) {}
      }
    }
    System.out.println("Finished execution of hubbleReads()");
  }

  private void hubbleWriteMessagesInterleaved(
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
                  .set("subject")
                  .to(String.format(SUBJECT, i))
                  .set("body")
                  .to(LOREMIPSUM)
                  .set("send_timestamp")
                  .to(Value.COMMIT_TIMESTAMP)
                  .build());
        }
        try {
          dbClient.write(mutations);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
      System.out.println("Finished execution of hubbleWriteMessagesInterleaved()");
    }
  }

  /**
   * Represents a task to process workItem(rows) in parallel, distributing work among threads based
   * on a specific value.
   */
  static class WorkItemProcessor<T> implements Callable<Integer> {
    private final List<T> totalWorkItem;
    private final int threadIndex;
    private final int numThreads;
    private final DatabaseClient dbClient;
    private final String query;

    /**
     * Constructs a WorkItemProcessor with the given parameters.
     *
     * @param totalWorkItem The list of workitems(rows) to be processed.
     * @param numThreads The total number of threads.
     * @param threadIndex The index of the current thread.
     * @param dbClient Spanner DatabaseClient.
     */
    public WorkItemProcessor(
        List<T> totalWorkItem,
        int threadIndex,
        int numThreads,
        DatabaseClient dbClient,
        String query) {
      this.totalWorkItem = totalWorkItem;
      this.threadIndex = threadIndex;
      this.numThreads = numThreads;
      this.dbClient = dbClient;
      this.query = query;
    }

    /**
     * Runs the thread, processing rows based on distribution values.
     *
     * <p>The method iterates through the rows, distributing work among threads based on their
     * indices. Each thread processes rows starting from its {@code threadIndex} and then
     * incrementing by {@code numThreads} for subsequent rows, ensuring even distribution of work
     * among threads. If the number of rows is not a multiple of the number of threads, some threads
     * may process fewer rows.
     */
    public Integer call() {
      int count = 0;
      // Each thread will loop here to process as many rows as possible with in the given limit.
      // E.g Thread 1 will have row=1(startrow/threadIndex), then 11,21,31,41(using logic row +=
      // numThreads)...
      // Thread 2 will have row=2(startrow/threadIndex), then 22,32,42,52(using logic row +=
      // numThreads)...
      // Due to this reason while creating the thread object all these values are passed and
      // startRow value is set.
      for (int i = threadIndex; i <= totalWorkItem.size(); i += numThreads) {

        if (i < totalWorkItem.size()) {
          T row = totalWorkItem.get(i);
          processWorkItem(row, dbClient);
        }
        count++;
      }
      return count;
    }

    /*
        Method to process the workItem using the readWriteTransaction(). In case of a locking method, you may observe
        extended processing time for the line "Doing work Id: ..", involving the same 'generated_value' and 'id'. This behavior
        stems from issues like lock contention and other locking-related problems in Spanner. As a result,
        Spanner can't complete thread execution within a single execution cycle (which is the expected behavior for this test).

        Despite using a specific value for row processing, the 'column' isn't indexed. Consequently, a
        full table scan occurs. This is exacerbated by the read/write transaction nature, which prevents
        the realization of multithreading benefits.

        When this method is invoked by another non-locking method that uses the 'id' (primary key) column
        to fetch data, this issue won't be encountered.
    */
    private void processWorkItem(T row, DatabaseClient dbClient) {
      // Select the specific row from the table. generated_value is passing in the query which will
      // fetch a specific row from the table.
      // This way distribution of random processing is easy and deterministic.
      String stmt = String.format(query, row);
      boolean didWork =
          // Starting a readwrite transaction.
          Boolean.TRUE.equals(
              dbClient
                  .readWriteTransaction()
                  .run(
                      transaction -> {
                        ResultSet resultSet = transaction.executeQuery(Statement.of(stmt));
                        while (resultSet.next()) {
                          String id = resultSet.getString("id");
                          int generatedValue = (int) resultSet.getLong("generated_value");
                          System.out.printf(
                              "Doing work Id: %s, Generated Value: %d\n", id, generatedValue);
                          transaction.buffer(
                              Mutation.newUpdateBuilder("WorkList")
                                  .set("id")
                                  .to(id)
                                  .set("is_done")
                                  .to(true)
                                  .set("timestamp")
                                  .to(Value.COMMIT_TIMESTAMP)
                                  .build());
                        }
                        return true;
                      }));
      System.out.println("WorkItem is completed ::" + didWork + ",processing row:" + row);
    }
  }

  private void hubbleCreateDatabase(
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
}
