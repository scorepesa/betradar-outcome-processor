package system;

import db.MySQL3;
import java.util.Timer;

import utils.Logging;
import utils.Props;

import org.apache.commons.daemon.Daemon;
import org.apache.commons.daemon.DaemonContext;
import org.apache.commons.daemon.DaemonInitException;
import queue.Publisher;
import queue.QueueConnection;

/*
 * @since 1.0
 * @author Reuben Paul Wafula
 * @version Version 1.0
 */
@SuppressWarnings({"ClassWithoutLogger", "FinalClass"})
public final class OutcomeProcessorDaemon implements Daemon, Runnable {

    /**
     * The worker thread that does all the work.
     */
    private transient Thread worker;
    /**
     * Flag to check if the worker thread should processRequests.
     */
    private transient boolean working = false;
    /**
     * Loads system properties.
     */
    private transient final Props props = new Props();

    /**
     * Logger for this application.
     */
    private transient final Logging log = new Logging();

    private transient final MySQL3 mysql3 = new MySQL3();

    /**
     * The main processRequests class.
     */
    private transient OutcomeProcessor alubet;

    /**
     * Timer to log every 5 second
     */
    private transient Timer timer;
    private transient LogWorking logWorking;

    private transient final QueueConnection queueConnection = new QueueConnection();
    private transient final Publisher publisher = new Publisher();

    /**
     * Used to configuration files, create a trace file, create ServerSockets,
     * Threads, etc.
     *
     * @param context the DaemonContext
     *
     * @throws DaemonInitException on error
     */
    @Override
    public void init(final DaemonContext context) throws DaemonInitException {
        worker = new Thread(this);
        Logging.info("Initializing MD daemon...");
        alubet = new OutcomeProcessor();
        timer = new Timer();
        logWorking = new LogWorking();
    }

    /**
     * Starts the daemon.
     */
    @Override
    public void start() {
        working = true;
        worker.start();
        Logging.info("Starting MD daemon...");
    }

    /**
     * Stops the daemon. Informs the thread to terminate the processRequests().
     */
    @Override
    @SuppressWarnings("SleepWhileInLoop")
    public void stop() {
        Logging.info("Stopping BetikoDaemon Winner Processor daemon...");

        working = false;

        while (!alubet.getIsCurrentPoolShutDown()) {
            Logging.info("Waiting for current thread pool to complete tasks...");
            try {
                Thread.sleep(2000);
            } catch (InterruptedException ex) {
                Logging.error("InterruptedException occured while waiting for "
                        + "tasks to complete: " + ex.getMessage());
            }
        }

        Logging.info("Completed tasks in current thread pool, continuing daemon "
                + "shutdown");

    }

    /**
     * Destroys the daemon. Destroys any object created in init().
     */
    @Override
    public void destroy() {
        Logging.info("Destroying MD daemon...");
        Logging.info("Exiting...");
    }

    /**
     * Runs the thread. The application runs inside an "infinite" loop.
     */
    @Override
    @SuppressWarnings({"SleepWhileHoldingLock", "SleepWhileInLoop"})
    public void run() {
        //while (working) {
        try {
            Logging.info("starting processRequests ... ");
            try {
                if (!logWorking.hasRunStarted()) {
                    timer.scheduleAtFixedRate(logWorking, 0, 5000);
                }
            } catch (Exception e) {
                Logging.error("Error Logging Timer", e);
            }
            alubet.processRequests();
        } catch (Exception ex) {
            Logging.error("General error occured: " + ex.getMessage(), ex);
        }
        //}
    }
}
