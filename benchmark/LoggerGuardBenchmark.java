/**
 * LoggerGuardBenchmark.java — Standalone benchmark demonstrating the impact of
 * guarding java.util.logging entering/exiting calls with isLoggable(FINER).
 *
 * No dependencies required. Compile and run with any JDK 8+:
 *   javac LoggerGuardBenchmark.java && java LoggerGuardBenchmark
 *
 * Background:
 *   Every ResultSet getter (getInt, getString, getBoolean, etc.) calls
 *   loggerExternal.entering() and loggerExternal.exiting() with varargs.
 *
 *   The varargs signature `entering(String, String, Object...)` forces the
 *   JVM to:
 *     1. Autobox any primitive arguments (int → Integer, boolean → Boolean)
 *     2. Allocate a new Object[] array for the varargs
 *     3. Pass these into entering(), which internally checks isLoggable(FINER)
 *        and returns immediately when logging is off — wasting the allocation.
 *
 *   In production, logging is almost always OFF. For a query reading 1M rows
 *   with 10 columns, that's 20M getter calls × 2 (entering + exiting) =
 *   40M unnecessary autoboxing + Object[] allocations per query.
 *
 *   The fix: wrap every entering/exiting call with:
 *     if (loggerExternal.isLoggable(Level.FINER)) { ... }
 *
 *   isLoggable() is a simple volatile read — effectively free.
 */
import java.util.logging.Level;
import java.util.logging.Logger;

public class LoggerGuardBenchmark {

    static final int WARMUP_ITERS  = 8;
    static final int MEASURE_ITERS = 15;
    static final int OPS_PER_ITER  = 10_000_000;

    // Simulate the driver's logger configuration
    static final Logger logger = Logger.getLogger("com.microsoft.sqlserver.jdbc");
    static final String CLASS_NAME  = "SQLServerResultSet";
    static final String METHOD_NAME = "getInt";

    static {
        // Production default: logging OFF
        logger.setLevel(Level.OFF);
        // Remove console handlers to avoid noise
        for (var h : logger.getHandlers()) logger.removeHandler(h);
    }

    public static void main(String[] args) {
        System.out.println("Logger Guard Benchmark — mssql-jdbc ResultSet getter overhead");
        System.out.println("=============================================================\n");
        System.out.printf("Config: %d warmup iters, %d measurement iters, %,d ops/iter%n",
                WARMUP_ITERS, MEASURE_ITERS, OPS_PER_ITER);
        System.out.printf("Logger level: %s (typical production setting)%n%n", logger.getLevel());

        // ── Scenario 1: Simple entering/exiting (single int arg) ───────
        System.out.println("Scenario 1: entering(class, method, int) — single primitive arg");
        System.out.println("  This is the getInt(columnIndex) pattern.\n");

        runScenario("Single-arg entering/exiting",
            LoggerGuardBenchmark::beforeSingleArg,
            LoggerGuardBenchmark::afterSingleArg);

        // ── Scenario 2: Multiple args (column index + value) ───────────
        System.out.println("\nScenario 2: entering(class, method, int) + exiting(class, method, Object)");
        System.out.println("  Full getter pattern: entering with column index, exiting with result.\n");

        runScenario("Full getter entering+exiting",
            LoggerGuardBenchmark::beforeFullGetter,
            LoggerGuardBenchmark::afterFullGetter);

        // ── Scenario 3: Realistic mixed — simulates reading rows ───────
        System.out.println("\nScenario 3: Simulated row read — 10 getters per row");
        System.out.println("  Represents reading a row with 10 columns of mixed types.\n");

        int rowOps = OPS_PER_ITER / 10; // each 'op' is a full row (10 getters)
        runScenario("10-column row read",
            () -> beforeRowRead(rowOps),
            () -> afterRowRead(rowOps));

        // ── Impact estimate ────────────────────────────────────────────
        System.out.println("\n═══════════════════════════════════════════════════════");
        System.out.println("Real-world impact estimate:");
        System.out.println("  Query: SELECT 10 columns FROM table (1,000,000 rows)");
        System.out.println("  Getter calls: 10M (one per column per row)");
        System.out.println("  entering + exiting calls: 20M");
        System.out.println("  Eliminated allocations: 20M autoboxed Integers + 20M Object[]");
        System.out.println("  Young-gen GC pressure reduction: significant");
        System.out.println("═══════════════════════════════════════════════════════");
    }

    // ── Scenario implementations ───────────────────────────────────────

    // BEFORE: Unconditional entering — pays autoboxing + Object[] on every call
    static long beforeSingleArg() {
        long sink = 0;
        for (int i = 0; i < OPS_PER_ITER; i++) {
            logger.entering(CLASS_NAME, METHOD_NAME, i); // autobox int→Integer + Object[]
            sink += i;
        }
        return sink;
    }

    // AFTER: Guarded — isLoggable is a cheap volatile read, skips everything
    static long afterSingleArg() {
        long sink = 0;
        for (int i = 0; i < OPS_PER_ITER; i++) {
            if (logger.isLoggable(Level.FINER)) {
                logger.entering(CLASS_NAME, METHOD_NAME, i);
            }
            sink += i;
        }
        return sink;
    }

    // BEFORE: Full getter pattern — entering with index, exiting with result
    static long beforeFullGetter() {
        long sink = 0;
        for (int i = 0; i < OPS_PER_ITER; i++) {
            logger.entering(CLASS_NAME, METHOD_NAME, i);       // autobox + Object[]
            Integer result = Integer.valueOf(i * 42);           // simulate getValue()
            logger.exiting(CLASS_NAME, METHOD_NAME, result);    // another Object[]
            sink += result;
        }
        return sink;
    }

    // AFTER: Guarded full getter
    static long afterFullGetter() {
        long sink = 0;
        for (int i = 0; i < OPS_PER_ITER; i++) {
            if (logger.isLoggable(Level.FINER)) {
                logger.entering(CLASS_NAME, METHOD_NAME, i);
            }
            Integer result = Integer.valueOf(i * 42);
            if (logger.isLoggable(Level.FINER)) {
                logger.exiting(CLASS_NAME, METHOD_NAME, result);
            }
            sink += result;
        }
        return sink;
    }

    // BEFORE: Simulated row with 10 getters
    static long beforeRowRead(int rows) {
        long sink = 0;
        String[] methods = {"getInt","getString","getLong","getBoolean","getDouble",
                            "getBigDecimal","getDate","getTimestamp","getFloat","getShort"};
        for (int row = 0; row < rows; row++) {
            for (int col = 0; col < 10; col++) {
                logger.entering(CLASS_NAME, methods[col], col + 1);
                int val = row * 10 + col;
                logger.exiting(CLASS_NAME, methods[col], val);
                sink += val;
            }
        }
        return sink;
    }

    // AFTER: Guarded row read
    static long afterRowRead(int rows) {
        long sink = 0;
        String[] methods = {"getInt","getString","getLong","getBoolean","getDouble",
                            "getBigDecimal","getDate","getTimestamp","getFloat","getShort"};
        for (int row = 0; row < rows; row++) {
            for (int col = 0; col < 10; col++) {
                if (logger.isLoggable(Level.FINER)) {
                    logger.entering(CLASS_NAME, methods[col], col + 1);
                }
                int val = row * 10 + col;
                if (logger.isLoggable(Level.FINER)) {
                    logger.exiting(CLASS_NAME, methods[col], val);
                }
                sink += val;
            }
        }
        return sink;
    }

    // ── Runner ─────────────────────────────────────────────────────────

    @FunctionalInterface
    interface BenchFn { long run(); }

    static void runScenario(String name, BenchFn before, BenchFn after) {
        // Warmup
        for (int w = 0; w < WARMUP_ITERS; w++) {
            before.run();
            after.run();
        }

        // Measure BEFORE
        long totalBefore = 0;
        for (int i = 0; i < MEASURE_ITERS; i++) {
            long t0 = System.nanoTime();
            before.run();
            totalBefore += System.nanoTime() - t0;
        }
        double nsPerOpBefore = (double) totalBefore / (MEASURE_ITERS * OPS_PER_ITER);

        // Measure AFTER
        long totalAfter = 0;
        for (int i = 0; i < MEASURE_ITERS; i++) {
            long t0 = System.nanoTime();
            after.run();
            totalAfter += System.nanoTime() - t0;
        }
        double nsPerOpAfter = (double) totalAfter / (MEASURE_ITERS * OPS_PER_ITER);

        double speedup = nsPerOpBefore / nsPerOpAfter;
        double msBefore = totalBefore / (double) MEASURE_ITERS / 1e6;
        double msAfter  = totalAfter / (double) MEASURE_ITERS / 1e6;

        System.out.printf("  ┌─────────────┬──────────────┬──────────────┬──────────┐%n");
        System.out.printf("  │             │  ns/op       │  ms/iter     │ speedup  │%n");
        System.out.printf("  ├─────────────┼──────────────┼──────────────┼──────────┤%n");
        System.out.printf("  │ Before      │ %10.2f   │ %10.1f   │          │%n", nsPerOpBefore, msBefore);
        System.out.printf("  │ After       │ %10.2f   │ %10.1f   │ %5.1fx    │%n", nsPerOpAfter, msAfter, speedup);
        System.out.printf("  └─────────────┴──────────────┴──────────────┴──────────┘%n");
    }
}
