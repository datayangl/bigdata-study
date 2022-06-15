package com.ly.bigdata.concurrent.model.actor.mailbox;

import com.ly.bigdata.common.util.ExceptionUtils;
import com.ly.bigdata.common.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static com.ly.bigdata.common.util.Preconditions.checkState;
import static com.ly.bigdata.concurrent.model.actor.mailbox.Mailbox.MIN_PRIORITY;

/**
 * MailboxProcessor 实现了 mailbox-based 执行模型。核心方法 {@link #runMailboxLoop()} 会在循环中不断执行
 * {@link MailboxDefaultAction}。每次遍历中，这个方法会先检查 mailbox 中是否有堆积的 mail action 然后执行
 * 这些action，之后再去执行 default action。这个模型确保单线程执行 default action (比如 数据处理) 和
 * mailbox actions (比如 checkpoint trigger, timer firing)。在 flink 中 default action 用于数据处理，
 * 而 mailbox actions 则用于处理控制类操作。
 *
 * {@link MailboxDefaultAction} 使用 {@link MailboxController} 和 mailbox loop 中的控制流交互。
 *
 * {@link #runMailboxLoop()} 的设计核心是尽量快速的处理 hot path(default action, no mail)。这意味着其他的控制
 * 标志 (mailboxLoopRunning, suspendedDefaultAction) 需要确定 #hasMail 是否为 true。这样 mailbox actions
 * 可以优先于 default action 处理，但是我们需要确保每一个控制类操作在 mailbox 中至少有一个对应的 action 这样才能确保
 * 控制类操作被执行。
 *
 * -----> control flow change
 *               |
 *              \/
 *           mail action (at least one)
 *               |
 *              \/
 *          ________________
 *          / MailBox      /
 *          ---------------
 *
 */
public class MailboxProcessor implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(MailboxProcessor.class);

    protected final Mailbox mailbox;

    /**
     * Action that is repeatedly executed if no action request is in the mailbox. Typically record
     * processing.
     */
    protected final MailboxDefaultAction mailboxDefaultAction;

    /**
     * Control flag to terminate the mailbox processor. Once it was terminated could not be
     * restarted again. Must only be accessed from mailbox thread.
     */
    private boolean mailboxLoopRunning;

    /**
     * Control flag to temporary suspend the mailbox loop/processor. After suspending the mailbox
     * processor can be still later resumed. Must only be accessed from mailbox thread.
     */
    private boolean suspended;

    /**
     * Remembers a currently active suspension of the default action. Serves as flag to indicate a
     * suspended default action (suspended if not-null) and to reuse the object as return value in
     * consecutive suspend attempts. Must only be accessed from mailbox thread.
     */
    private DefaultActionSuspension suspendedDefaultAction;

    private final ActionExecutor actionExecutor;

    public MailboxProcessor() {
        this(MailboxDefaultAction.Controller::suspendDefaultAction);
    }

    public MailboxProcessor(MailboxDefaultAction mailboxDefaultAction) {
        this(mailboxDefaultAction, ActionExecutor.IMMEDIATE);
    }

    public MailboxProcessor(
            MailboxDefaultAction mailboxDefaultAction, ActionExecutor actionExecutor) {
        this(mailboxDefaultAction, new MailboxImpl(Thread.currentThread()), actionExecutor);
    }

    public MailboxProcessor(MailboxDefaultAction mailboxDefaultAction, Mailbox mailbox, ActionExecutor actionExecutor) {
        this.mailboxDefaultAction = Preconditions.checkNotNull(mailboxDefaultAction);
        this.mailbox = Preconditions.checkNotNull(mailbox);
        this.actionExecutor = Preconditions.checkNotNull(actionExecutor);
    }


    /** Lifecycle method to close the mailbox for action submission. */
    public void prepareClose() {
        mailbox.quiesce();
    }

    /**
     * Lifecycle method to close the mailbox for action submission/retrieval. This will cancel all
     * instances of {@link java.util.concurrent.RunnableFuture} that are still contained in the
     * mailbox.
     */
    @Override
    public void close() throws IOException {
        List<Mail> droppedMails = mailbox.close();
        if (!droppedMails.isEmpty()) {
            LOG.debug("Closing the mailbox dropped mails {}.", droppedMails);
            Optional<RuntimeException> maybeErr = Optional.empty();
            for (Mail droppedMail : droppedMails) {
                try {
                    droppedMail.tryCancel(false);
                } catch (RuntimeException e) {
                    maybeErr = Optional.of(ExceptionUtils.firstOrSuppressed(e, maybeErr.orElse(null)));
                }
            }
            maybeErr.ifPresent(
                    e -> {throw e;
                    });
        }
    }

    /**
     * Finishes running all mails in the mailbox. If no concurrent write operations occurred, the
     * mailbox must be empty after this method.
     */
    public void drain() throws Exception {
        for (final Mail mail : mailbox.drain()) {
            mail.run();
        }
    }

    /**
     * Runs the mailbox processing loop. This is where the main work is done. This loop can be
     * suspended at any time by calling {@link #suspend()}. For resuming the loop this method should
     * be called again.
     */
    public void runMailboxLoop() throws Exception {
        suspended = !mailboxLoopRunning;

        final Mailbox localMailbox = mailbox;

        checkState(
                localMailbox.isMailboxThread(),
                "Method must be executed by declared mailbox thread!");

        assert localMailbox.getState() == MailboxImpl.State.OPEN : "Mailbox must be opened!";

        final MailboxController defaultActionContext = new MailboxController(this);

        while (isNextLoopPossible()) {
            // The blocking `processMail` call will not return until default action is available.
            processMail(localMailbox, false);
            if (isNextLoopPossible()) {
                mailboxDefaultAction.runDefaultAction(defaultActionContext); // lock is acquired inside default action as needed
            }
        }
    }

    /** Suspend the running of the loop which was started by {@link #runMailboxLoop()}}. */
    public void suspend() {
        sendPoisonMail(() -> suspended = true);
    }

    /**
     * Check if the current thread is the mailbox thread.
     *
     * @return only true if called from the mailbox thread.
     */
    public boolean isMailboxThread() {
        return mailbox.isMailboxThread();
    }

    /**
     * This method must be called to end the stream task when all actions for the tasks have been
     * performed.
     */
    public void allActionsCompleted() {
        sendPoisonMail(
                () -> {
                    mailboxLoopRunning = false;
                    suspended = true;
                });
    }

    /** Send mail in first priority for internal needs. */
    private void sendPoisonMail(Runnable mail) {
        // run exclusively to be executed directly as long as we get the lock
        mailbox.runExclusively(
                () -> {
                    // keep state check and poison mail enqueuing atomic, such that no intermediate
                    // #close may cause a
                    // MailboxStateException in #sendPriorityMail.
                    if (mailbox.getState() == Mailbox.State.OPEN) {
                        sendControlMail(mail, "poison mail");
                    }
                });
    }



    private void sendControlMail(Runnable mail,  String descriptionFormat, Object... descriptionArgs) {
        mailbox.putFirst(
                new Mail(
                        mail,
                        Integer.MAX_VALUE,
                        descriptionFormat,
                        descriptionArgs));
    }

    /**
     * This helper method handles all special actions from the mailbox. In the current design, this
     * method also evaluates all control flag changes. This keeps the hot path in {@link
     * #runMailboxLoop()} free from any other flag checking, at the cost that all flag changes must
     * make sure that the mailbox signals mailbox#hasMail.
     *
     * @return true if a mail has been processed.
     */
    private boolean processMail(Mailbox mailbox, boolean singleStep) throws Exception {
        // 优化：从原队列中生成一批数据放入批队列，这样消费者只从批队列中消费，这样就避免和生产者竞争锁去操作原队列。
        // Doing this check is an optimization to only have a volatile read in the expected hot
        // path, locks are only
        // acquired after this point.
        if (!mailbox.createBatch()) {
            return false;
        }

        // 非阻塞获取 mail 并执行
        // Take mails in a non-blockingly and execute them.
        boolean processed = processMailsNonBlocking(singleStep);
        if (singleStep) {
            return processed;
        }

        // If the default action is currently not available, we can run a blocking mailbox execution
        // until the default action becomes available again.
        processed |= processMailsWhenDefaultActionUnavailable();
        return processed;
    }

    /**
     * 当 DefaultAction 不可执行（比如suspended)，循环执行 mailbox 的action
     * @return DefaultAction
     * @throws Exception
     */
    private boolean processMailsWhenDefaultActionUnavailable() throws Exception {
        boolean processedSomething = false;
        Optional<Mail> maybeMail;
        while (isDefaultActionUnavailable() && isNextLoopPossible()) {
            maybeMail = mailbox.tryTake(MIN_PRIORITY);
            if (!maybeMail.isPresent()) {
                maybeMail = Optional.of(mailbox.take(MIN_PRIORITY));
            }
            //maybePauseIdleTimer();
            maybeMail.get().run();
            //maybeRestartIdleTimer();
            processedSomething = true;
        }
        return processedSomething;
    }
    /**
     *
     * @param singleStep 如果为true，每次处理一个action，否则，全量处理。通常为false。批次处理提高性能。
     * @return
     * @throws Exception
     */
    private boolean processMailsNonBlocking(boolean singleStep) throws Exception {
        long processedMails = 0;
        Optional<Mail> maybeMail;

        while (isNextLoopPossible() && (maybeMail = mailbox.tryTakeFromBatch()).isPresent()) {
            if (processedMails++ == 0) {
                //todo maybe pause idle timer
            }
            maybeMail.get().run();

            if (singleStep) {
                break;
            }
            if (processedMails > 0){
                // todo maybe restart idle timer
                return true;
            } else {
                return false;
            }
        }
        return false;
    }

    public boolean isDefaultActionUnavailable() {
        //todo
        return false;
    }

    private boolean isNextLoopPossible() {
        // 'Suspended' can be false only when 'mailboxLoopRunning' is true.
        return !suspended;
    }

    /**
     * Represents the suspended state of the default action and offers an idempotent method to
     * resume execution.
     */
    protected static final class MailboxController implements MailboxDefaultAction.Controller {
        private final MailboxProcessor mailboxProcessor;

        protected MailboxController(MailboxProcessor mailboxProcessor) {
            this.mailboxProcessor = mailboxProcessor;
        }

        @Override
        public void allActionsCompleted() {

        }

        @Override
        public MailboxDefaultAction.Suspension suspendDefaultAction(long suspensionIdleTimer) {
            return null;
        }

        @Override
        public MailboxDefaultAction.Suspension suspendDefaultAction() {
            return null;
        }
    }


    private final class DefaultActionSuspension implements MailboxDefaultAction.Suspension {
        @Override
        public void resume() {
            if (mailbox.isMailboxThread()) {
                resumeInternal();
            } else {
                try {
                    // send suspension mail
                    sendControlMail(this::resumeInternal, "resume default action");
                } catch (Mailbox.MailboxClosedException e) {
                    // Ignored
                }
            }
        }

        private void resumeInternal() {
            if (suspendedDefaultAction != null) {
                suspendedDefaultAction = null;
            }
        }
    }

}