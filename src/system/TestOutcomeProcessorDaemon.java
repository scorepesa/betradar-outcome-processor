package system;

import db.MySQL3;
import java.util.Timer;
import queue.Publisher;
import queue.QueueConnection;
import utils.Logging;
import utils.Props;

/**
 * <p>
 * Java UNIX daemon test file.</p>
 * <p>
 * Title: TestAlertSchedulerDaemon.java</p>
 * <p>
 * Description: This class is used to test the functionality of the Java
 * Daemon.</p>
 * <p>
 * Created on 21 March 2012, 10:48</p>
 * <hr />
 *
 * @since 1.0
 * @author Reuben Paul Wafula
 * @version Version 1.0
 */
@SuppressWarnings({"ClassWithoutLogger", "FinalClass"})
public final class TestOutcomeProcessorDaemon{

    private static Props props = new Props();

    /**
     * Logger for this application.
     */
    private static Logging log = new Logging();
    /**
     * Loads system properties.
     */

    private static final MySQL3 mysql3 = new MySQL3();
    /**
     * The main processRequests class.
     */
    private static OutcomeProcessor betpalace;
    /**
     * Instance of the MySQL connection pool.
     */

    private transient static Timer timer;
    private transient static LogWorking logWorking;

    private static QueueConnection queueConnection = new QueueConnection();
    private transient static Publisher publisher = new Publisher();

    /**
     * Private constructor.
     */
    private TestOutcomeProcessorDaemon() {
    }

    /**
     * Test init().
     */
    public static void init() {

        Logging.info("Init starting mysql db connections");

        Logging.info("Attempting to create RequisitionDaemon");
        System.err.println("Before Fishines init");
        // betRadar = new BetRadar(mysql, log, props);
        betpalace = new OutcomeProcessor();
        timer = new Timer();
        logWorking = new LogWorking();
        System.err.println("Fishines init");
    }

    /**
     * Main method.
     *
     * @param args command line arguments
     */
    @SuppressWarnings({"SleepWhileInLoop", "UseOfSystemOutOrSystemErr"})
    public static void main(final String[] args) {
        System.err.println("Looping +");
        init();
        //while (true) {
        long tStart = System.currentTimeMillis();
        try {
            Logging.info("Calling init from main args in loop");
            try {
                if (!logWorking.hasRunStarted()) {
                    timer.scheduleAtFixedRate(logWorking, 0, 5000);
                }
            } catch (Exception e) {
                Logging.error("Error Logging Timer", e);
            }
            betpalace.processRequests();
        } catch (Exception ex) {
            System.err.println(ex.getMessage());
        }
        long tEnd = System.currentTimeMillis();
        long tDelta = tEnd - tStart;
        double elapsedSeconds = tDelta / 1000.0;
        //}
    }
}
