package com.ly.bigdata.concurrent.model.actor.mailbox;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import static com.ly.bigdata.concurrent.model.actor.mailbox.Mailbox.State.*;

/**
 *  Producer-Consumer Model
 *
 *  Implementation of {@link Mailbox} in a {@link java.util.concurrent.BlockingQueue} fashion and
 *  tailored towards our use case with multiple writers and single reader.
 */
public class MailboxImpl implements Mailbox{
    private final ReentrantLock lock = new ReentrantLock();

    /** Internal queue of mails. */
    private final Deque<Mail> queue = new ArrayDeque<>();

    /** Condition that is triggered when the mailbox is no longer empty. */
    private final Condition notEmpty = lock.newCondition();

    /** The state of the mailbox in the lifecycle of open, quiesced, and closed. */
    private State state = OPEN;

    /** Reference to the thread that executes the mailbox mails. */
    private final Thread mailboxThread;

    /**
     * The current batch of mails. A new batch can be created with {@link #createBatch()} and
     * consumed with {@link #tryTakeFromBatch()}.
     */
    private final Deque<Mail> batch = new ArrayDeque<>();

    /**
     * Performance optimization where hasNewMail == !queue.isEmpty(). Will not reflect the state of
     * {@link #batch}.
     */
    private volatile boolean hasNewMail = false;

    public MailboxImpl(final Thread mailboxThread) {
        this.mailboxThread = mailboxThread;
    }

    public MailboxImpl() {
        this(Thread.currentThread());
    }

    public State getState() {
        return state;
    }

    @Override
    public boolean isMailboxThread() {
        return Thread.currentThread() == mailboxThread;
    }

    @Override
    public boolean hasMail() {
        checkIsMailboxThread();
        return !batch.isEmpty() || hasNewMail;
    }

    public int size() {
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            return batch.size() + queue.size();
        } finally {
            lock.unlock();
        }
    }

    /**
     * get mail unblockingly
     * @param priority
     * @return
     */
    @Override
    public Optional<Mail> tryTake(int priority) {
        checkIsMailboxThread();
        checkTakeStateConditions();
        Mail head = takeOrNull(batch, priority);
        if (head != null) {
            return Optional.of(head);
        }
        if (!hasNewMail) {
            return Optional.empty();
        }
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            final Mail value = takeOrNull(queue, priority);
            if (value == null) {
                return Optional.empty();
            }
            // check hasNewMail in case it is the last mail in queue
            hasNewMail = !queue.isEmpty();
            return Optional.ofNullable(value);
        } finally {
            lock.unlock();
        }
    }

    /**
     * get mail blockingly
     * @param priority
     * @return
     * @throws InterruptedException
     * @throws IllegalStateException
     */
    @Override
    public Mail take(int priority) throws InterruptedException, IllegalStateException {
        checkIsMailboxThread();
        checkTakeStateConditions();
        Mail head = takeOrNull(batch, priority);
        if (head != null) {
            return head;
        }
        final ReentrantLock lock = this.lock;
        lock.lockInterruptibly();
        try {
            Mail headMail;
            // block until get new mail
            while ((headMail = takeOrNull(queue, priority)) == null) {
                // to ease debugging
                notEmpty.await(1, TimeUnit.SECONDS);
            }
            hasNewMail = !queue.isEmpty();
            return headMail;
        } finally {
            lock.unlock();
        }
    }

    /**
     *  create batch
     * @return
     */
    @Override
    public boolean createBatch() {
        checkIsMailboxThread();
        if (!hasNewMail) {
            // batch is usually depleted by previous MailboxProcessor#runMainLoop
            // however, putFirst may add a message directly to the batch if called from mailbox
            // thread
            return !batch.isEmpty();
        }

        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            Mail mail;
            while ((mail = queue.pollFirst()) != null) {
                batch.addLast(mail);
            }
            hasNewMail = false;
            return !batch.isEmpty();
        } finally {
            lock.unlock();
        }
    }

    /**
     * take mail from batch queue
     * @return
     */
    @Override
    public Optional<Mail> tryTakeFromBatch() {
        checkIsMailboxThread();
        checkTakeStateConditions();
        return Optional.ofNullable(batch.pollFirst());
    }

    // ---------------------------------------------------------------------------------------------------------------------------------

    @Override
    public void put(Mail mail) {
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            checkPutStateConditions();
            queue.addLast(mail);
            hasNewMail = true;
            notEmpty.signal();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void putFirst(Mail mail) {
        if (isMailboxThread()) {
            checkPutStateConditions();
            batch.addFirst(mail);
        } else {
            final ReentrantLock lock = this.lock;
            lock.lock();
            try {
                checkPutStateConditions();
                queue.addFirst(mail);
                hasNewMail = true;
                notEmpty.signal();
            } finally {
                lock.unlock();
            }
        }
    }

    // ------------------------------------------------------------------------------------------------------------------
    private Mail takeOrNull(Deque<Mail> queue, int priority) {
        if (queue.isEmpty()) {
            return null;
        }

        Iterator<Mail> iterator = queue.iterator();
        while (iterator.hasNext()) {
            Mail mail = iterator.next();
            if (mail.getPriority() >= priority) {
                iterator.remove();
                return mail;
            }
        }
        return null;
    }

    @Override
    public List<Mail> drain() {
        List<Mail> drainedMails = new ArrayList<>(batch);
        batch.clear();
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            drainedMails.addAll(queue);
            queue.clear();
            hasNewMail = false;
            return drainedMails;
        } finally {
            lock.unlock();
        }
    }

    private void checkIsMailboxThread() {
        if (!isMailboxThread()) {
            throw new IllegalStateException(
                    "Illegal thread detected. This method must be called from inside the mailbox thread!");
        }
    }

    private void checkPutStateConditions() {
        if (state != OPEN) {
            throw new MailboxClosedException(
                    "Mailbox is in state "
                            + state
                            + ", but is required to be in state "
                            + OPEN
                            + " for put operations.");
        }
    }

    private void checkTakeStateConditions() {
        if (state == CLOSED) {
            throw new MailboxClosedException(
                    "Mailbox is in state "
                            + state
                            + ", but is required to be in state "
                            + OPEN
                            + " or "
                            + QUIESCED
                            + " for take operations.");
        }
    }

    @Override
    public void quiesce() {
        checkIsMailboxThread();
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            if (state == OPEN) {
                state = QUIESCED;
            }
        } finally {
            this.lock.unlock();
        }
    }

    /**
     * return all enqueued mails
     * @return
     */
    @Override
    public List<Mail> close() {
        checkIsMailboxThread();
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            if (state == CLOSED) {
                return Collections.emptyList();
            }
            List<Mail> droppedMails = drain();
            state = CLOSED;
            // to unblock all
            notEmpty.signalAll();
            return droppedMails;
        } finally {
            lock.unlock();
        }
    }

    /**
     *  runExclusively() and close() all acquire lock, so that runExclusively() will not be
     *  interrupted even close() is invoked. BTW, since runExclusively() acquire lock, other operations like put()
     *  would be blocked, so make sure runExclusively() don't cost too much time or it would lead to terrible performance.
     * @param runnable the runnable to execute
     */
    @Override
    public void runExclusively(Runnable runnable) {
        lock.lock();
        try {
            runnable.run();
        } finally {
            lock.unlock();
        }
    }
}
