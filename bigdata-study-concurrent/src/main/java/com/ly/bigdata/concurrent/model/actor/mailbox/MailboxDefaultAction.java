

package com.ly.bigdata.concurrent.model.actor.mailbox;


/** Interface for the default action that is repeatedly invoked in the mailbox-loop. */
public interface MailboxDefaultAction {

    /**
     * This method implements the default action of the mailbox loop (e.g. processing one event from
     * the input). Implementations should (in general) be non-blocking.
     *
     * @param controller controller object for collaborative interaction between the default action
     *     and the mailbox loop.
     * @throws Exception on any problems in the action.
     */
    void runDefaultAction(Controller controller) throws Exception;

    /** Represents the suspended state of a {@link MailboxDefaultAction}, ready to resume. */

    interface Suspension {

        /** Resume execution of the default action. */
        void resume();
    }

    /**
     * This context is a feedback interface for the default action to interact with the mailbox
     * execution. In particular it offers ways to signal that the execution of the default action
     * should be finished or temporarily suspended.
     */
    interface Controller {

        /**
         * This method must be called to end the stream task when all actions for the tasks have
         * been performed. This method can be invoked from any thread.
         */
        void allActionsCompleted();

        /**
         * Calling this method signals that the mailbox-thread should (temporarily) stop invoking
         * the default action, e.g. because there is currently no input available. This method must
         * be invoked from the mailbox-thread only!
         *
         * @param suspensionIdleTimer started (ticking) that measures how long
         *     the default action was suspended/idling. If mailbox loop is busy processing mails,
         *     this timer should be paused for the time required to process the mails.
         */
        Suspension suspendDefaultAction(long suspensionIdleTimer);

        Suspension suspendDefaultAction();

    }
}
