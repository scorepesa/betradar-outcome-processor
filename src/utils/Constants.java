package utils;

/**
 * Constants used in the daemon.
 *
 * @author Reuben Paul Wafula
 */
@SuppressWarnings({"ClassMayBeInterface", "FinalClass"})
public final class Constants {

    /**
     * Private constructor.
     */
    private Constants() {
    }

    /**
     * Ping failed constant.
     */
    public static final int PING_FAILED = 101;
    /**
     * Ping success constant.
     */
    public static final int PING_SUCCESS = 100;
    /**
     * Daemon running state.
     */
    public static final int DAEMON_RUNNING = 1005;
    /**
     * Daemon interrupted state.
     */
    public static final int DAEMON_INTERRUPTED = 1006;
    /**
     * Daemon resuming state.
     */
    public static final int DAEMON_RESUMING = 1007;
    /**
     * The failed queries file.
     */
    public static final String FAILED_QUERIES_FILE = "FAILED_QUERIES.TXT";

 
    public static final String SAFARICOM = "safaricom";
    public static final String AIRTEL = "airtel";
    public static final String EQUITEL = "eqiutel";
    public static final String ORANGE = "orange";

    public static enum OutcomeType {
        NORMALOUTCOME, OUTRIGHTOUTCOME, VIRTUALOUTCOME
    };
}
