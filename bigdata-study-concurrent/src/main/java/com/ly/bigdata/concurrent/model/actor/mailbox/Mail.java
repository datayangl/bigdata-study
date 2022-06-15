package com.ly.bigdata.concurrent.model.actor.mailbox;

import java.util.concurrent.Future;

public class Mail {
    /** The action to execute. */
    private final Runnable runnable;
    /**
     * The priority of the mail. The priority does not determine the order, but helps to hide
     * upstream mails from downstream processors to avoid live/deadlocks.
     */
    private final int priority;

    /** The description of the mail that is used for debugging and error-reporting. */
    private final String descriptionFormat;

    private final Object[] descriptionArgs;

    private ActionExecutor actionExecutor;

    public Mail(Runnable runnable, int priority,String descriptionFormat,
                Object... descriptionArgs) {
        this(
                runnable,
                priority,
                ActionExecutor.IMMEDIATE,
                descriptionFormat,
                descriptionArgs);
    }

    public Mail(Runnable runnable, int priority, ActionExecutor actionExecutor, String descriptionFormat, Object[] descriptionArgs) {
        this.runnable = runnable;
        this.priority = priority;
        this.actionExecutor = actionExecutor;
        this.descriptionFormat = descriptionFormat;
        this.descriptionArgs = descriptionArgs;
    }

    public int getPriority() {
        return priority;
    }

    public void tryCancel(boolean mayInterruptIfRunning) {
        if (runnable instanceof Future) {
            ((Future<?>) runnable).cancel(mayInterruptIfRunning);
        }
    }

    public void run() throws Exception {
        actionExecutor.run(runnable);
    }
}
