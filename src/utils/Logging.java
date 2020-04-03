package utils;

import java.io.IOException;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.RollingFileAppender;

/**
 * Initializes the log files.
 *
 * @author Reuben Paul Wafula
 */
@SuppressWarnings("FinalClass")
public final class Logging {

    /**
     * Info log.
     */
    private static Logger infoLog;
    /**
     * Error log.
     */
    private static Logger errorLog;
    /**
     * Fatal log.
     */
    private static Logger fatalLog;
    /**
     * Loaded system properties.
     */
    private static Logger timerLog;

    /**
     * Constructor.
     *
     * @param properties passed in loaded system properties
     */
    public Logging() {        
        initializeLoggers();
    }

    /**
     * Initialize the log managers.
     */
    @SuppressWarnings({"CallToThreadDumpStack", "UseOfSystemOutOrSystemErr"})
    private void initializeLoggers() {
        infoLog = Logger.getLogger("InfoLog");
        errorLog = Logger.getLogger("ErrorLog");
        fatalLog = Logger.getLogger("FatalLog");
        timerLog = Logger.getLogger("TimerLog");

        PatternLayout layout = new PatternLayout();
        layout.setConversionPattern("%d{yyyy MMM dd HH:mm:ss,SSS}: %p : %m%n");

        try {
            RollingFileAppender rfaInfoLog = new RollingFileAppender(layout,
                    Props.getInfoLogFile(), true);
            rfaInfoLog.setMaxFileSize("1000MB");
            rfaInfoLog.setMaxBackupIndex(10);

            RollingFileAppender rfaErrorLog = new RollingFileAppender(layout,
                    Props.getErrorLogFile(), true);
            rfaErrorLog.setMaxFileSize("1000MB");
            rfaErrorLog.setMaxBackupIndex(10);

            RollingFileAppender rfaFatalLog = new RollingFileAppender(layout,
                    Props.getFatalLogFile(), true);
            rfaFatalLog.setMaxFileSize("1000MB");
            rfaFatalLog.setMaxBackupIndex(10);
            
            RollingFileAppender rfaTimerLog = new RollingFileAppender(layout,
                    Props.getTimerLogFile(), true);
            rfaTimerLog.setMaxFileSize("1000MB");
            rfaTimerLog.setMaxBackupIndex(10);

            infoLog.addAppender(rfaInfoLog);
            errorLog.addAppender(rfaErrorLog);
            fatalLog.addAppender(rfaFatalLog);
            timerLog.addAppender(rfaTimerLog);
        } catch (IOException ex) {
            System.err.println("Failed to initialize loggers... EXITING: "
                    + ex.getMessage());
            ex.printStackTrace();
            System.exit(1);
        }

        infoLog.setLevel(Level.toLevel(Props.getInfoLogLevel()));
        errorLog.setLevel(Level.toLevel(Props.getErrorLogLevel()));
        fatalLog.setLevel(Level.toLevel(Props.getFatalLogLevel()));
        timerLog.setLevel(Level.toLevel(Props.getTimerLogLevel()));

        info("Initialized Loggers...");
    }

    /**
     * Log info messages.
     *
     * @param message the message content
     */
    public static void info(final String message) {
        infoLog.info(Thread.currentThread().getName() + ": " + message);
    }
    
    /**
     * Log info messages.
     *
     * @param message the message content
     */
    public static void timer(final String message) {
        timerLog.info(Thread.currentThread().getName() + ": " + message);
    }

    /**
     * Log debug messages.
     *
     * @param message the message content
     */
    public static void debug(final String message) {
        infoLog.debug(Thread.currentThread().getName() + ": " + message);
    }    

    /**
     * Log error messages.
     *
     * @param message the message content
     */
    public static void error(final String message) {
        errorLog.error(Thread.currentThread().getName() + ": " + message);
    }

    public static void error(final String message, Throwable t) {
        errorLog.error(Thread.currentThread().getName() + ": " + message, t);
    }

    /**
     * Log fatal error messages.
     *
     * @param message the message content
     */
    public static void fatal(final String message) {
        fatalLog.fatal(Thread.currentThread().getName() + ": " + message);
    }

    public static void fatal(final String message, Throwable t) {
        fatalLog.fatal(Thread.currentThread().getName() + ": " + message, t);
    }
}
