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
public class HubbleTransactionsCodeLab {

  private static final String LOREMIPSUM =
      "Aenean sit amet lectus est. Nullam ornare ligula luctus auctor placerat. Morbi fermentum volutpat massa, sit amet consectetur metus vehicula sed. Sed auctor scelerisque tempus. Morbi hendrerit tortor et felis scelerisque, at fermentum risus posuere. Suspendisse finibus, tellus eu ullamcorper posuere, odio ex pulvinar est, a malesuada libero turpis quis massa. Quisque placerat lacus orci. Proin ac viverra metus. Lorem ipsum dolor sit amet, consectetur adipiscing elit. Phasellus cursus rhoncus iaculis. Pellentesque nisl tellus, venenatis nec erat sit amet, eleifend porttitor nisl. Maecenas fringilla ex id mauris facilisis, sed luctus dui elementum. Suspendisse ac orci lectus. Suspendisse vitae sapien convallis, commodo leo ut, ultricies arcu. Fusce pellentesque sem vestibulum, sodales purus eget, auctor odio. Ut et nunc metus. Aenean ac ex faucibus, tristique nibh ut, euismod lorem. Fusce a ex ut nibh consectetur mollis. Aenean suscipit elit dui, faucibus vestibulum leo commodo a. Nulla ultricies vitae velit cursus commodo. Morbi eu sapien in magna condimentum porta quis eget sem. Etiam tempor auctor diam, quis mollis odio scelerisque et. Fusce tempus mauris mi, et varius enim condimentum in. Aliquam nisi lorem, pulvinar id ullamcorper vitae, fringilla vel leo. Fusce vehicula tincidunt vulputate. Vivamus efficitur nunc quis lorem porttitor elementum. Donec ex neque, vestibulum nec mollis quis, lacinia quis dui. Nullam rhoncus quis lacus nec euismod. Vivamus porttitor sem nec nisl auctor ultrices. Vivamus non laoreet lectus. Aliquam condimentum semper libero eu elementum. Nullam lobortis ultricies gravida. Integer in lacinia lacus, ac consequat magna. Suspendisse et risus vel diam facilisis ornare a in arcu. Nulla nec nunc sem. Cras aliquam nulla sem, luctus maximus est gravida ut. Ut pellentesque pharetra convallis. Quisque molestie, ipsum sit amet scelerisque convallis, magna ante fringilla massa, in blandit turpis nibh ornare magna. Curabitur mi tortor, feugiat id sem ac, scelerisque congue tortor. Aenean non viverra risus. Praesent vel enim quis dolor auctor aliquet. Maecenas faucibus mi at venenatis suscipit. Integer interdum magna vitae mauris interdum, laoreet ullamcorper erat tincidunt. Morbi vel ipsum convallis, semper quam vitae, sagittis tellus. In facilisis eu lorem imperdiet laoreet. Suspendisse gravida a magna et condimentum. Suspendisse vitae risus vitae est pulvinar convallis at sit amet sem. Morbi vel imperdiet leo, sit amet cursus urna. Pellentesque suscipit ut neque non laoreet. Duis non ipsum ipsum. Quisque ut porttitor dui. Duis nulla augue, varius quis tellus sed, efficitur bibendum justo. Cras vestibulum congue ante in gravida. Quisque tincidunt nisi nisl, sed vulputate leo luctus ac. Pellentesque quis tempor leo, sed faucibus ipsum. Donec rutrum turpis nec auctor lobortis. Pellentesque habitant morbi tristique senectus et netus et malesuada fames ac turpis egestas. Sed lobortis sodales risus vitae eleifend. Nullam pharetra dapibus lobortis. Vestibulum nisl diam, dignissim sit amet lacus sit amet, venenatis auctor nisi. Phasellus sollicitudin tortor a mi fringilla, imperdiet tristique ex feugiat. Nulla ut vehicula metus, nec tempus odio. Phasellus pellentesque lacus lacus, fringilla egestas felis hendrerit ac. Nam suscipit orci eros, at molestie nunc tincidunt id. Aliquam maximus finibus leo. Vestibulum orci turpis, fringilla eget lorem id, cursus consequat tortor. Suspendisse efficitur purus lectus, ac ornare sem lobortis a. Interdum et malesuada fames ac ante ipsum primis in faucibus. Sed sodales id mi eu aliquam. Curabitur enim libero, tempor vel vestibulum id, eleifend at lacus. Maecenas ut feugiat justo, nec dictum justo. Nulla porttitor accumsan rhoncus. Cras ligula velit, molestie sit amet molestie sed, commodo et libero. Etiam eu condimentum arcu. Aliquam erat volutpat. Nullam sit amet urna ipsum. Vestibulum at velit id velit tincidunt ultricies a ut metus. In vitae mollis diam. Proin lacinia fringilla purus vitae aliquet. Vestibulum a tincidunt eros. Aenean imperdiet aliquet arcu, vitae euismod turpis fermentum quis. Orci varius natoque penatibus et magnis dis parturient montes, nascetur ridiculus mus. Suspendisse potenti. Pellentesque quis efficitur mauris, pulvinar sagittis ante. Cras efficitur porta convallis. Nullam eleifend congue finibus. Nam sodales tincidunt odio ac elementum. Sed sem risus, imperdiet quis suscipit eu, imperdiet vitae nibh. Morbi gravida neque ac sodales varius. In convallis massa vel lectus fermentum ultricies ac vitae eros. Sed justo sem, dignissim sit amet tempus ultrices, pellentesque at libero. Quisque a quam volutpat, tristique erat a, auctor enim. Suspendisse finibus arcu erat, nec mattis tortor facilisis in. Donec ornare mattis lorem, in interdum turpis venenatis vel. Mauris tempus porttitor libero. Pellentesque habitant morbi tristique senectus et netus et malesuada fames ac turpis egestas. Vivamus eget tempor dui. Nunc et elit eu eros tincidunt sapien.";

  private static final String SUBJECT = "Random subject for the email for testing purpose %s";

  public void createMessages(DatabaseAdminClient dbAdminClient, DatabaseId id) {
    createDatabase(
        dbAdminClient,
        id,
        Collections.singletonList(
            "CREATE TABLE Message("
                + " msg_id STRING(MAX) NOT NULL,"
                + " body STRING(MAX),"
                + ") PRIMARY KEY (msg_id)"));
    System.out.println("Finished execution of createMessages()");
  }

  public void createInterleaved(DatabaseAdminClient dbAdminClient, DatabaseId id) {
    createDatabase(
        dbAdminClient,
        id,
        Arrays.asList(
            "CREATE TABLE Mailbox("
                + "sid INT64 NOT NULL,"
                + "guid STRING(MAX),"
                + "state STRING(MAX),"
                + ") PRIMARY KEY (sid)",
            "CREATE TABLE Message("
                + " sid INT64 NOT NULL,"
                + " msg_id STRING(MAX) NOT NULL,"
                + " subject STRING(MAX),"
                + " body STRING(MAX),"
                + " send_timestamp TIMESTAMP  OPTIONS (allow_commit_timestamp=true),"
                + ") PRIMARY KEY (sid, msg_id),"
                + "INTERLEAVE IN PARENT Mailbox ON DELETE CASCADE"));

    System.out.println("Finished execution of createInterleaved()");
  }

  public void createWorkItems(DatabaseAdminClient dbAdminClient, DatabaseId id) {
    createDatabase(
        dbAdminClient,
        id,
        Collections.singletonList(
            "CREATE TABLE WorkList("
                + " id STRING(MAX) NOT NULL,"
                + " is_done BOOL,"
                + " generated_value Int64,"
                + " timestamp TIMESTAMP  OPTIONS (allow_commit_timestamp=true), "
                + ") PRIMARY KEY (id)"));
    System.out.println("Finished execution of createWorkItems()");
  }

  public void writeWorkItems(
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
    System.out.println("Finished execution of writeWorkItems()");
  }

  public boolean doWorkSingleTransaction(DatabaseClient dbClient, boolean doneValue) {
    String stmt = String.format("SELECT * FROM WorkList WHERE is_done is %b LIMIT 100", !doneValue);
    // Below code will be removed in next PR.
    //    boolean didWork =
    //            dbClient.readWriteTransaction()
    //                    .run(transaction -> {
    //                      Random random = new Random();
    //                      ResultSet resultSet = transaction.executeQuery(Statement.of(stmt));
    //                      List<List<Object>> rows = resultSet.getRows();
    //                      resultSet.close();
    //                      if (rows.size() == 0) return false;
    //                      int randIdx = random.nextInt(resultSet.size());
    //                      String id = rows.get(randIdx).get(0);
    //                      System.out.printf("Doing work: %s\n", id);
    //                      transaction.buffer(
    //                              Mutation.newUpdateBuilder("WorkList")
    //                                      .set("id").to(id)
    //                                      .set("is_done").to(doneValue)
    //                                      .build());
    //                      return true;
    //                    });
    //    return didWork;
    return true;
  }

  public void doWorkSingleTransactionSerial(DatabaseClient dbClient, boolean doneValue) {
    boolean workRemaining = true;
    int workDone = 0;
    while (workRemaining) {
      workRemaining = doWorkSingleTransaction(dbClient, doneValue);
      if (workDone++ % 1000 == 0) {
        System.out.printf(">>>>>>>>>>>>>>>> Done %d work <<<<<<<<<<<<<\n", workDone);
      }
    }
  }

  public void doWorkSingleTransactionParallel(DatabaseClient dbClient, boolean doneValue) {
    int numProcessors = Runtime.getRuntime().availableProcessors();
    ExecutorService executorService = Executors.newFixedThreadPool(numProcessors);

    for (int i = 0; i < numProcessors; ++i) {
      executorService.submit(
          new Runnable() {
            @Override
            public void run() {
              doWorkSingleTransactionSerial(dbClient, doneValue);
            }
          });
    }
    executorService.shutdown();
    while (!executorService.isTerminated()) {}
  }

  public void writeMailboxes(DatabaseClient dbClient, int numMailboxes) {
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
    System.out.println("Finished execution of writeMailboxes()");
  }

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
      System.out.println("Finished execution of writeMessagesInterleaved()");
    }
  }

  public void writeMessagesInterleavedParallel(
      DatabaseClient dbClient, int numMailboxes, int mutationsPerTransaction, int numMinutes) {
    int numProcessors = Runtime.getRuntime().availableProcessors();
    ExecutorService executorService = Executors.newFixedThreadPool(numProcessors);
    executeTasksInParallel(
        () -> writeMessagesInterleaved(dbClient, numMailboxes, mutationsPerTransaction, numMinutes),
        executorService);
    System.out.println("Finished execution of writeMessagesInterleavedParallel()");
  }

  public void writeMessages(DatabaseClient dbClient, int mutationsPerTransaction, int numMinutes) {
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
    System.out.println("Finished execution of writeMessages()");
  }

  private void writeMessagesUUID(
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
    System.out.println("Finished execution of writeMessagesUUID()");
  }

  public void writeMessagesParallel(
      DatabaseClient dbClient, int mutationsPerTransaction, int numMinutes) {
    int numProcessors = Runtime.getRuntime().availableProcessors();
    ExecutorService executorService = Executors.newFixedThreadPool(numProcessors);
    executeTasksInParallel(
        () -> writeMessages(dbClient, mutationsPerTransaction, numMinutes), executorService);
    System.out.println("Finished execution of writeMessagesParallel()");
  }

  public void writeMessagesParallelUUID(
      DatabaseClient dbClient, int mutationsPerTransaction, int numMinutes) {
    int numProcessors = Runtime.getRuntime().availableProcessors();
    ExecutorService executorService = Executors.newFixedThreadPool(numProcessors);
    executeTasksInParallel(
        () -> writeMessagesUUID(dbClient, mutationsPerTransaction, numMinutes), executorService);
    System.out.println("Finished execution of writeMessagesParallelUUID()");
  }

  public void updatesAndReads(
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
        () -> updates(dbClient, keys, mutationsPerTransaction, numMinutes), writeExecutor);
    executeTasksInParallel(
        () -> reads(dbClient, keys, mutationsPerTransaction, numMinutes, isStrong), readExecutor);
    System.out.println("Finished execution of updatesAndReads()");
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

  private void updates(
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
    System.out.println("Finished execution of updates()");
  }

  private void reads(
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
    System.out.println("Finished execution of reads()");
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
}
