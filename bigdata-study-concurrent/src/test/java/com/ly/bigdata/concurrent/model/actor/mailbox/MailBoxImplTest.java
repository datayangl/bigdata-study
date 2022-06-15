package com.ly.bigdata.concurrent.model.actor.mailbox;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.ly.bigdata.concurrent.model.actor.mailbox.Mailbox.MailboxClosedException;
import static com.ly.bigdata.concurrent.model.actor.mailbox.Mailbox.MAX_PRIORITY;
import static org.junit.Assert.assertEquals;

/** Unit tests for {@link MailboxImpl}. */
public class MailBoxImplTest {
    private static final Runnable NO_OP = () -> {};
    private static final int DEFAULT_PRIORITY = 0;
    private Mailbox mailbox;

    @Before
    public void setUp() {
        mailbox = new MailboxImpl();
    }

    @After
    public void tearDown() {
        mailbox.close();
    }

    @Test
    public void testPutAsHead() throws InterruptedException {
        Mail mailA = new Mail(() -> {}, MAX_PRIORITY, "mailA");
        Mail mailB = new Mail(() -> {}, MAX_PRIORITY, "mailB");
        Mail mailC = new Mail(() -> {}, DEFAULT_PRIORITY, "mailC, DEFAULT_PRIORITY");
        Mail mailD = new Mail(() -> {}, DEFAULT_PRIORITY, "mailD, DEFAULT_PRIORITY");

        mailbox.put(mailC);
        mailbox.putFirst(mailB);
        mailbox.put(mailD);
        mailbox.putFirst(mailA);

        Assert.assertSame(mailA, mailbox.take(DEFAULT_PRIORITY));
        Assert.assertSame(mailB, mailbox.take(DEFAULT_PRIORITY));
        Assert.assertSame(mailC, mailbox.take(DEFAULT_PRIORITY));
        Assert.assertSame(mailD, mailbox.take(DEFAULT_PRIORITY));

        Assert.assertFalse(mailbox.tryTake(DEFAULT_PRIORITY).isPresent());
    }

    @Test
    public void testContracts() throws InterruptedException {
        final Queue<Mail> testObjects = new LinkedList<>();
        Assert.assertFalse(mailbox.hasMail());

        for (int i = 0; i < 10; ++i) {
            final Mail mail = new Mail(NO_OP, DEFAULT_PRIORITY, "mail, DEFAULT_PRIORITY");
            testObjects.add(mail);
            mailbox.put(mail);
            Assert.assertTrue(mailbox.hasMail());
        }

        while (!testObjects.isEmpty()) {
            assertEquals(testObjects.remove(), mailbox.take(DEFAULT_PRIORITY));
            assertEquals(!testObjects.isEmpty(), mailbox.hasMail());
        }
    }

    /** Test the producer-consumer pattern using the blocking methods on the mailbox. */
    @Test
    public void testConcurrentPutTakeBlocking() throws Exception {
        testPutTake(mailbox -> mailbox.take(DEFAULT_PRIORITY));
    }

    @Test
    public void testConcurrentPutTakeNonBlockingAndWait() throws Exception {
        testPutTake(mailbox -> {
            Optional<Mail> optionalMail = mailbox.tryTake(DEFAULT_PRIORITY);
            while (!optionalMail.isPresent()) {
                optionalMail = mailbox.tryTake(DEFAULT_PRIORITY);
            }
            return optionalMail.get();
        });
    }

    /** Test that closing the mailbox unblocks pending accesses with correct exceptions. */
    @Test
    public void testCloseUnblocks() throws InterruptedException {
        testAllPuttingUnblocksInternal(Mailbox::close);
    }

    /** Test that silencing the mailbox unblocks pending accesses with correct exceptions. */
    @Test
    public void testQuiesceUnblocks() throws InterruptedException {
        testAllPuttingUnblocksInternal(Mailbox::quiesce);
    }

    @Test
    public void testLifeCycleQuiesce() throws InterruptedException {
        mailbox.put(new Mail(NO_OP, DEFAULT_PRIORITY, "NO_OP, DEFAULT_PRIORITY"));
        mailbox.put(new Mail(NO_OP, DEFAULT_PRIORITY, "NO_OP, DEFAULT_PRIORITY"));
        mailbox.quiesce();
        testLifecyclePuttingInternal();
        mailbox.take(DEFAULT_PRIORITY);
        Assert.assertTrue(mailbox.tryTake(DEFAULT_PRIORITY).isPresent());
        Assert.assertFalse(mailbox.tryTake(DEFAULT_PRIORITY).isPresent());
    }

    @Test
    public void testLifeCycleClose() throws InterruptedException {
        mailbox.close();
        testLifecyclePuttingInternal();

        try {
            mailbox.take(DEFAULT_PRIORITY);
            Assert.fail();
        } catch (MailboxClosedException ignore) {
        }

        try {
            mailbox.tryTake(DEFAULT_PRIORITY);
            Assert.fail();
        } catch (MailboxClosedException ignore) {
        }
    }

    private void testLifecyclePuttingInternal() {
        try {
            mailbox.put(new Mail(NO_OP, DEFAULT_PRIORITY, "NO_OP, DEFAULT_PRIORITY"));
            Assert.fail();
        } catch (MailboxClosedException ignore) {
        }
        try {
            mailbox.putFirst(new Mail(NO_OP, MAX_PRIORITY, "NO_OP"));
            Assert.fail();
        } catch (MailboxClosedException ignore) {
        }
    }

    private void testAllPuttingUnblocksInternal(Consumer<Mailbox> unblockMethod)
            throws InterruptedException {
        testUnblocksInternal(
                () -> mailbox.put(new Mail(NO_OP, DEFAULT_PRIORITY, "NO_OP, DEFAULT_PRIORITY")),
                unblockMethod);
        setUp();
        testUnblocksInternal(
                () -> mailbox.putFirst(new Mail(NO_OP, MAX_PRIORITY, "NO_OP")), unblockMethod);
    }

    private void testUnblocksInternal(Runnable testMethod, Consumer<Mailbox> unblockMethod) throws InterruptedException {
        final Thread[] blockedThreads = new Thread[8];
        final Exception[] exceptions = new Exception[blockedThreads.length];

        CountDownLatch countDownLatch = new CountDownLatch(blockedThreads.length);

        for (int i=0; i < blockedThreads.length; ++i) {
            final int id = i;
            Thread blocked = new Thread(
                    () -> {
                        try {
                           countDownLatch.countDown();
                           while (true) {
                               testMethod.run();
                           }
                        } catch (Exception e) {
                            exceptions[id] = e;
                        }
                    }
            );
            blockedThreads[i] = blocked;
            blocked.start();
        }

        countDownLatch.await();
        unblockMethod.accept(mailbox);

        for (Thread blockedThread : blockedThreads) {
            blockedThread.join();
        }

        for (Exception exception : exceptions) {
            assertEquals(MailboxClosedException.class, exception.getClass());
        }
    }

    /**
     * Test producer-consumer pattern through the mailbox in a concurrent setting (n-writer /
     * 1-reader).
     */
    private void testPutTake(TestFunction<Mailbox, Mail, InterruptedException> takeMethod) throws Exception {
        final int numThreads = 10;
        final int numMailsPerThread = 1000;
        final int[] results = new int[numThreads];
        Thread[] writeThreads = new Thread[numThreads];

        for (int i=0; i < writeThreads.length; ++i) {
            final int threadId = i;
            writeThreads[i] = new Thread(() -> {
               for (int k = 0; k < numMailsPerThread; ++k) {
                   mailbox.put(new Mail(
                           () -> ++results[threadId],
                           DEFAULT_PRIORITY,
                           "result" + k
                           ));
               }
            });
        }

        for (Thread writeThread : writeThreads) {
            writeThread.start();
        }

        for (Thread writeThread : writeThreads) {
            writeThread.join();
        }

        AtomicBoolean isRunning = new AtomicBoolean(true);
        mailbox.put(
                new Mail(() -> isRunning.set(false),
                DEFAULT_PRIORITY,
                        "POISON_MAIL, DEFAULT_PRIORITY"
                )
        );

        while (isRunning.get()) {
            takeMethod.apply(mailbox).run();
        }
        for (int perThreadResults : results) {
            assertEquals(numMailsPerThread, perThreadResults);
        }
    }

    @Test
    public void testPutAsHeadWithPriority() throws InterruptedException {
        Mail mailA = new Mail(() -> {}, 2, "mailA");
        Mail mailB = new Mail(() -> {}, 2, "mailB");
        Mail mailC = new Mail(() -> {}, 1, "mailC");
        Mail mailD = new Mail(() -> {}, 1, "mailD");

        mailbox.put(mailC);
        mailbox.put(mailB);
        mailbox.put(mailD);
        mailbox.putFirst(mailA);

        Assert.assertSame(mailA, mailbox.take(2));
        Assert.assertSame(mailB, mailbox.take(2));
        Assert.assertFalse(mailbox.tryTake(2).isPresent());

        Assert.assertSame(mailC, mailbox.take(1));
        Assert.assertSame(mailD, mailbox.take(1));

        Assert.assertFalse(mailbox.tryTake(1).isPresent());
    }

    @Test
    public void testPutWithPriorityAndReadingFromMainMailbox() throws InterruptedException {
        Mail mailA = new Mail(() -> {}, 2, "mailA");
        Mail mailB = new Mail(() -> {}, 2, "mailB");
        Mail mailC = new Mail(() -> {}, 1, "mailC");
        Mail mailD = new Mail(() -> {}, 1, "mailD");

        mailbox.put(mailC);
        mailbox.put(mailB);
        mailbox.put(mailD);
        mailbox.putFirst(mailA);

        // same order for non-priority and priority on top
        Assert.assertSame(mailA, mailbox.take(Mailbox.MIN_PRIORITY));
        Assert.assertSame(mailC, mailbox.take(Mailbox.MIN_PRIORITY));
        Assert.assertSame(mailB, mailbox.take(Mailbox.MIN_PRIORITY));
        Assert.assertSame(mailD, mailbox.take(Mailbox.MIN_PRIORITY));
    }

    /**
     * Tests the interaction of batch and non-batch methods.
     *
     * <p>Both {@link Mailbox#take(int)} and {@link Mailbox#tryTake(int)} consume the batch
     * but once drained will fetch elements from the remaining mails.
     *
     * <p>In contrast, {@link Mailbox#tryTakeFromBatch()} will not return any mail once the
     * batch is drained.
     */
    @Test
    public void testBatchAndNonBatchTake() throws InterruptedException {
        final List<Mail> mails =
                IntStream.range(0, 6)
                        .mapToObj(i -> new Mail(NO_OP, DEFAULT_PRIORITY, String.valueOf(i)))
                        .collect(Collectors.toList());

        // create a batch with 3 mails
        mails.subList(0, 3).forEach(mailbox::put);
        Assert.assertTrue(mailbox.createBatch());
        // add 3 more mails after the batch
        mails.subList(3, 6).forEach(mailbox::put);

        // now take all mails in the batch with all available methods
        assertEquals(Optional.ofNullable(mails.get(0)), mailbox.tryTakeFromBatch());
        assertEquals(Optional.ofNullable(mails.get(1)), mailbox.tryTake(DEFAULT_PRIORITY));
        assertEquals(mails.get(2), mailbox.take(DEFAULT_PRIORITY));

        // batch empty, so only regular methods work
        assertEquals(Optional.empty(), mailbox.tryTakeFromBatch());
        assertEquals(Optional.ofNullable(mails.get(3)), mailbox.tryTake(DEFAULT_PRIORITY));
        assertEquals(mails.get(4), mailbox.take(DEFAULT_PRIORITY));

        // one unprocessed mail left
        assertEquals(Collections.singletonList(mails.get(5)), mailbox.close());
    }

    @Test
    public void testBatchDrain() throws Exception {
        Mail mailA = new Mail(() -> {}, MAX_PRIORITY, "mailA");
        Mail mailB = new Mail(() -> {}, MAX_PRIORITY, "mailB");

        mailbox.put(mailA);
        Assert.assertTrue(mailbox.createBatch());
        mailbox.put(mailB);

        assertEquals(Arrays.asList(mailA, mailB), mailbox.drain());
    }

    @Test
    public void testBatchPriority() throws Exception {

        Mail mailA = new Mail(() -> {}, 1, "mailA");
        Mail mailB = new Mail(() -> {}, 2, "mailB");

        mailbox.put(mailA);
        Assert.assertTrue(mailbox.createBatch());
        mailbox.put(mailB);

        assertEquals(mailB, mailbox.take(2));
        assertEquals(Optional.of(mailA), mailbox.tryTakeFromBatch());
    }

    @Test
    public void testRunExclusively() throws InterruptedException {
        CountDownLatch exclusiveCodeStarted = new CountDownLatch(1);

        final int mailSize = 10;

        // send 10 mails in an atomic operation
        new Thread( () -> mailbox.runExclusively(
                () -> {
                    exclusiveCodeStarted.countDown();
                    for (int i=0; i < mailSize; i++) {
                        try {
                            mailbox.put(new Mail(() -> {}, 1, "mail"));
                            Thread.sleep(1);
                        } catch (Exception e) {

                        }
                    }
                }))
        .start();

        exclusiveCodeStarted.await();
        // make sure that all 10 messages have been actually enqueued.
        assertEquals(mailSize, mailbox.close().size());
    }

    interface TestFunction<T,R, E extends Throwable> {
        R apply(T value) throws E;
    }
}
