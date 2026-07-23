/*
 * ============================================================================
 *  REFERENCE ONLY -- NOT PART OF THE BUILD
 * ============================================================================
 *  I placed this file under docs/ so Maven never compiles it. It documents the
 *  standalone benchmark harness I used to produce the TOKEN_ACQUISITION numbers
 *  I report in the "fixed semaphore pool for the SP token gate" change (see
 *  SQLServerMSAL4JUtils#getSqlFedAuthTokenPrincipal).
 *
 *  This is the MULTI service-principal workload. Where the single-SP harness
 *  (TokenAcquisitionTest) shows worst-case same-credential contention, this one
 *  drives many DISTINCT service principals concurrently -- which is exactly the
 *  scenario the pooled semaphore relieves. On a single SP the pool behaves like
 *  the global gate (all callers hash to one slot), so I measure it here with N
 *  principals round-robined across the worker threads.
 *
 *  It will NOT compile inside this repo: it depends on a small, local,
 *  git-ignored `Creds` helper that you supply with your own Azure SQL server
 *  FQDN, database name and a LIST of service-principal client-id/secret pairs:
 *
 *      final class Creds {
 *          static final String SERVER_FQDN   = "<your-server>.database.windows.net";
 *          static final String DATABASE_NAME = "<your-db>";
 *          // one row per service principal: { clientId, clientSecret }
 *          static final String[][] SPS = {
 *              { "<client-id-1>", "<client-secret-1>" },
 *              { "<client-id-2>", "<client-secret-2>" },
 *              // ... as many distinct principals as you want to spread load over
 *          };
 *      }
 *
 *  I embed no credentials here -- they live only in your local Creds class. I
 *  kept this file purely so you can see exactly how I produced the measurements.
 *
 * ----------------------------------------------------------------------------
 *  This measures the cross-principal contention on the token gate: driver-org
 *  routes every SP token acquisition through ONE global Semaphore(1), so unrelated
 *  principals serialise against each other. pooled-sem maps each principal (via
 *  its hashedSecret) onto one of SEM_POOL_SIZE slots, so distinct principals
 *  contend only on a hash collision (~1/SEM_POOL_SIZE). See the PR description for
 *  the driver-org vs pooled-sem TOKEN_ACQUISITION results produced by this harness.
 *
 * ----------------------------------------------------------------------------
 *  Retry configuration (I built both the baseline and the pooled-sem jars this
 *  way, so my comparison stays apples-to-apples): I disable retries so a
 *  transient failure surfaces immediately instead of being silently retried and
 *  inflating latency.
 *
 *    1. connectRetryCount=0 -- I set this via the connection string below (no
 *       code change required).
 *    2. INTERMITTENT_TLS_MAX_RETRY=0 -- the driver's internal, no-wait
 *       TLS/prelogin handshake retry (normally 5). This is a hard-coded
 *       `private final` constant in SQLServerConnection and CANNOT be set from
 *       the connection string; to reproduce my numbers, build the driver with
 *       that constant patched to 0 for BOTH the baseline and the change.
 * ============================================================================
 */

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;

/**
 * MultiSpTokenAcquisitionTest - multi-service-principal connection-throughput benchmark.
 *
 * <p>K worker threads repeatedly open a brand-new JDBC connection and run {@code SELECT 1},
 * but each worker is pinned to a different service principal from {@code Creds.SPS}
 * (round-robin: worker w uses {@code SPS[w % SPS.length]}). Because the workers authenticate
 * with many DISTINCT credentials, this exercises the cross-principal contention on the token
 * gate -- the scenario the semaphore pool is designed to relieve:
 * <ul>
 *   <li><b>driver-org</b>: all principals share one global {@code Semaphore(1)}, so unrelated
 *       principals serialise against each other on every token acquisition.</li>
 *   <li><b>pooled-sem</b>: each principal maps onto one of {@code SEM_POOL_SIZE} slots, so
 *       distinct principals contend only when their hashed secrets collide on a slot.</li>
 * </ul>
 *
 * <p>The harness reports throughput (connections/sec), latency (avg/max), failures and
 * driver-measured TOKEN_ACQUISITION timings.
 *
 * <p>Compile this file plus the reader-supplied {@code Creds} helper against the driver
 * jar, then run it twice with the same command but a different driver jar on the
 * classpath (before vs. after this change). The {@code driver-org} / {@code pooled-sem}
 * argument is just a label printed in the output.
 * <pre>
 *   // baseline: the current driver jar
 *   java -cp "mssql-jdbc-&lt;version&gt;.jar;." MultiSpTokenAcquisitionTest driver-org &lt;K&gt; &lt;T&gt; [warm|cold]
 *
 *   // this change: the driver jar built from this branch
 *   java -cp "mssql-jdbc-&lt;version&gt;.jar;." MultiSpTokenAcquisitionTest pooled-sem &lt;K&gt; &lt;T&gt; [warm|cold]
 * </pre>
 * Defaults: K=100, T=60, mode=warm. For a meaningful multi-SP measurement, supply several
 * distinct principals in {@code Creds.SPS} (ideally K &gt;= SPS.length so every principal is used).
 * <ul>
 *   <li><b>warm</b>: run a throwaway warm-up first so the MSAL token / TLS / DB caches
 *       are primed and every measured call is a cache hit.</li>
 *   <li><b>cold</b>: skip the warm-up so threads race against a cold cache (stampede).</li>
 * </ul>
 */
public final class MultiSpTokenAcquisitionTest {

    public static void main(String[] args) throws Exception {
        // Parse CLI args: [driverLabel] [K threads] [T seconds] [warm|cold].
        String driverLabel = (args.length > 0) ? args[0] : "unknown";
        int K = (args.length > 1) ? Integer.parseInt(args[1]) : 100;
        int T = (args.length > 2) ? Integer.parseInt(args[2]) : 60;
        String mode = (args.length > 3) ? args[3].toLowerCase() : "warm";
        boolean warmup = !mode.startsWith("cold");

        // Load the JDBC driver so DriverManager can resolve the sqlserver:// URL.
        System.out.println("[step] Loading JDBC driver class...");
        Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        System.out.println("[step] JDBC driver loaded successfully");

        // Register a callback that captures the driver's internal TOKEN_ACQUISITION
        // timings (time spent acquiring the Azure AD access token per connection).
        TokenAcquisitionCollector tokenStats = new TokenAcquisitionCollector();
        com.microsoft.sqlserver.jdbc.SQLServerDriver.registerPerformanceLogCallback(tokenStats);
        System.out.println("[step] Registered TOKEN_ACQUISITION performance callback");

        // Pre-build one JDBC URL per distinct service principal; worker w uses urls[w % urls.length].
        final int spCount = Creds.SPS.length;
        final String[] urls = new String[spCount];
        for (int i = 0; i < spCount; i++) {
            urls[i] = jdbcUrl(Creds.SPS[i][0], Creds.SPS[i][1]);
        }
        System.out.println("[step] Built JDBC URLs for " + spCount + " distinct service principal(s)");

        System.out.println("============================================================");
        System.out.println(" MultiSpTokenAcquisitionTest  driver=" + driverLabel
                + "  K=" + K + "  T=" + T + "s  mode=" + (warmup ? "warm" : "cold")
                + "  SPs=" + spCount + " (round-robin)");
        System.out.println("============================================================");

        if (warmup) {
            // Warm-up phase: drive full load for WARMUP_SEC to prime the MSAL token,
            // TLS sessions and DB caches for every principal. Metrics are discarded.
            System.out.println("[warmup] Running " + WARMUP_SEC + "s full-load warm-up (K=" + K
                    + ", SPs=" + spCount + ") to prime MSAL token / TLS / DB caches...");
            long warmStart = System.currentTimeMillis();
            runLoad(urls, K, WARMUP_SEC, false);
            System.out.println("[warmup] Warm-up load complete in "
                    + (System.currentTimeMillis() - warmStart) + " ms");
            // Brief rest so the warm-up burst fully drains before measuring.
            System.out.println("[rest] Resting " + REST_SEC + "s before measured run...");
            Thread.sleep(REST_SEC * 1000L);
            System.out.println("[rest] Rest complete.");
        } else {
            System.out.println("[warmup] SKIPPED -- cache starts cold (stampede)");
        }

        // Discard warm-up token stats and settle briefly so the measured window
        // starts from a clean baseline.
        tokenStats.reset();
        Thread.sleep(1000);
        System.out.println("[quiesce] 1 s settled");

        // Measured phase: run the real load and collect metrics.
        System.out.println("[step] Starting measured run: K=" + K + " threads for " + T + "s");
        tokenStats.markWindowStart();
        Metrics m = runLoad(urls, K, T, true);

        // Derive the headline numbers from the measured window.
        long wall = m.wall;
        System.out.println("[step] All workers finished. Wall time: " + wall + " ms");

        long okN = m.ok;
        long failN = m.fail;
        long totalN = okN + failN;
        double cps = totalN * 1000.0 / wall;                         // connections/sec
        long avgMs = okN > 0 ? (m.latSumMicros / okN / 1000L) : 0;   // mean latency of ok calls

        System.out.println();
        System.out.println("Summary:");
        System.out.println("  driver          : " + driverLabel);
        System.out.println("  K (threads)     : " + K);
        System.out.println("  T (sec target)  : " + T);
        System.out.println("  mode            : " + (warmup ? "warm" : "cold"));
        System.out.println("  SPs             : " + spCount + " (round-robin)");
        System.out.println("  wall            : " + wall + " ms");
        System.out.println("  calls ok        : " + okN);
        System.out.println("  calls fail      : " + failN);
        System.out.printf ("  throughput      : %.1f cps%n", cps);
        System.out.println("  avg latency     : " + avgMs + " ms / call");
        System.out.println("  max latency     : " + (m.latMaxMicros / 1000L) + " ms");
        System.out.println();
        tokenStats.printSummary();
        System.out.println();
        System.out.println("[done]");
    }

    /**
     * Performance-log callback that aggregates the driver's {@code TOKEN_ACQUISITION}
     * durations (time spent inside {@code onFedAuthInfo()} acquiring the Azure AD
     * access token for each connection). Durations are reported in nanoseconds and
     * accumulated lock-free via {@link LongAdder}/{@link AtomicLong} so the callback
     * adds negligible overhead under high concurrency.
     */
    private static final class TokenAcquisitionCollector
            implements com.microsoft.sqlserver.jdbc.PerformanceLogCallback {

        private final LongAdder count = new LongAdder();
        private final LongAdder sumNanos = new LongAdder();
        private final AtomicLong maxNanos = new AtomicLong(0);
        private final LongAdder failures = new LongAdder();

        // Details of the single slowest token acquisition seen so far. Guarded by
        // `this` only on the rare path where a new maximum is observed, so the hot
        // path (a normal, non-max sample) stays lock-free.
        private volatile long maxConnectionId = -1;
        private volatile long maxAtEpochMs = -1;
        // Wall-clock origin so the max can be reported as "seconds into the run".
        private volatile long windowStartEpochMs = System.currentTimeMillis();

        @Override
        public boolean useNanoseconds() {
            return true;
        }

        @Override
        public void publish(com.microsoft.sqlserver.jdbc.PerformanceActivity activity, int connectionId,
                long duration, Exception exception) {
            // Only token-acquisition events are of interest here.
            if (activity != com.microsoft.sqlserver.jdbc.PerformanceActivity.TOKEN_ACQUISITION) {
                return;
            }
            count.increment();
            sumNanos.add(duration);
            long prevMax = maxNanos.accumulateAndGet(duration, Math::max);
            // If this sample set a new maximum, capture which connection produced it
            // and when. accumulateAndGet returns the NEW max, so compare to duration.
            if (prevMax == duration) {
                synchronized (this) {
                    // Re-check under lock in case a larger sample raced in.
                    if (duration >= maxNanos.get()) {
                        maxConnectionId = connectionId;
                        maxAtEpochMs = System.currentTimeMillis();
                    }
                }
            }
            if (exception != null) {
                failures.increment();
            }
        }

        @Override
        public void publish(com.microsoft.sqlserver.jdbc.PerformanceActivity activity, int connectionId,
                int statementId, long duration, Exception exception) {
            // Statement-level activities are not relevant for token acquisition; ignore.
        }

        /** Clears all counters so warm-up measurements don't leak into the run. */
        void reset() {
            count.reset();
            sumNanos.reset();
            maxNanos.set(0);
            failures.reset();
            maxConnectionId = -1;
            maxAtEpochMs = -1;
            windowStartEpochMs = System.currentTimeMillis();
        }

        /**
         * Marks the true start of the measured window so "seconds into the window"
         * for the max is measured from the first measured call, not from reset().
         */
        void markWindowStart() {
            windowStartEpochMs = System.currentTimeMillis();
        }

        /** Prints aggregate token-acquisition count, failures, and avg/max/total times. */
        void printSummary() {
            long n = count.sum();
            System.out.println("Token acquisition (TOKEN_ACQUISITION):");
            System.out.println("  acquisitions    : " + n);
            System.out.println("  failures        : " + failures.sum());
            if (n > 0) {
                long avgNs = sumNanos.sum() / n;
                System.out.printf("  avg time        : %.3f ms (%d ns)%n", avgNs / 1_000_000.0, avgNs);
                System.out.printf("  max time        : %.3f ms (%d ns)%n",
                        maxNanos.get() / 1_000_000.0, maxNanos.get());
                // Identify exactly which connection produced the max and when in the run.
                if (maxConnectionId >= 0) {
                    double atSec = (maxAtEpochMs - windowStartEpochMs) / 1000.0;
                    System.out.printf("  max occurred on : ConnectionID=%d at %.3fs into the measured window%n",
                            maxConnectionId, atSec);
                }
                System.out.printf("  total time      : %.3f ms%n", sumNanos.sum() / 1_000_000.0);
            } else {
                System.out.println("  (no token acquisitions recorded -- tokens likely served from MSAL cache)");
            }
        }
    }

    private static final int WARMUP_SEC = 30;   // duration of the discarded warm-up phase
    private static final int REST_SEC = 20;     // idle gap between warm-up and measured run

    /**
     * Core load generator: starts K worker threads that each open a fresh connection,
     * run {@code SELECT 1}, close it, and repeat in a tight loop until durationSec
     * elapses. Worker w is pinned to {@code urls[w % urls.length]} so load spreads
     * across the distinct service principals. All threads are released simultaneously
     * via a {@link CountDownLatch}.
     *
     * <p>When {@code measure=false} this is a warm-up (caches are primed, no metrics
     * returned). When {@code measure=true} the returned {@link Metrics} describe the window.
     *
     * @return aggregate metrics (ok/fail counts, wall time, latency sum/max)
     */
    private static Metrics runLoad(String[] urls, int K, int durationSec, boolean measure) throws InterruptedException {
        // Lock-free aggregate counters updated by every worker.
        LongAdder ok = new LongAdder();
        LongAdder fail = new LongAdder();
        LongAdder latSumMicros = new LongAdder();
        AtomicLong latMaxMicros = new AtomicLong(0);
        AtomicBoolean firstHitLogged = new AtomicBoolean(false);   // log only the first ok
        AtomicBoolean firstFailLogged = new AtomicBoolean(false);  // log only the first error

        ExecutorService ex = Executors.newFixedThreadPool(K);
        CountDownLatch start = new CountDownLatch(1);    // gate so all workers start together
        final long windowStart = System.currentTimeMillis();
        long deadline = windowStart + durationSec * 1000L;

        for (int w = 0; w < K; w++) {
            final int workerId = w;
            // Pin this worker to one service principal so distinct principals run concurrently.
            final String url = urls[w % urls.length];
            ex.submit(() -> {
                // Wait at the gate until main releases all workers at once.
                try { start.await(); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); return; }
                while (System.currentTimeMillis() < deadline) {
                    long t0 = System.nanoTime();
                    // open -> query -> close, timed end to end (try-with-resources auto-closes).
                    try (Connection c = DriverManager.getConnection(url);
                         Statement st = c.createStatement();
                         ResultSet rs = st.executeQuery("SELECT 1")) {
                        if (measure && firstHitLogged.compareAndSet(false, true)) {
                            System.out.println("[first-conn] First measured connection established by worker-" + workerId
                                    + " in " + ((System.nanoTime() - t0) / 1_000_000L) + " ms");
                        }
                        rs.next();
                        long usec = (System.nanoTime() - t0) / 1000L;
                        ok.increment();
                        latSumMicros.add(usec);
                        latMaxMicros.accumulateAndGet(usec, Math::max);
                    } catch (Exception e) {
                        // Failed connection (e.g. gateway reset / timeout) is still timed and recorded.
                        fail.increment();
                        if (measure && firstFailLogged.compareAndSet(false, true)) {
                            System.out.println("[error] First failure by worker-" + workerId + ": " + e.getMessage());
                        }
                    }
                }
            });
        }

        // Release all workers, then wait for the pool to drain after the deadline.
        long t0 = System.currentTimeMillis();
        start.countDown();
        ex.shutdown();
        boolean terminated = ex.awaitTermination(durationSec + 60L, TimeUnit.SECONDS);
        if (!terminated) {
            System.out.println("[warn] Timeout -- some workers did not finish; forcing shutdown");
            ex.shutdownNow();
        }
        long wall = System.currentTimeMillis() - t0;

        Metrics m = new Metrics();
        m.ok = ok.sum();
        m.fail = fail.sum();
        m.wall = wall;
        m.latSumMicros = latSumMicros.sum();
        m.latMaxMicros = latMaxMicros.get();
        return m;
    }

    /** Aggregate metrics for one measured window (counts, wall time, latency sum/max in micros). */
    private static final class Metrics {
        long ok, fail, wall, latSumMicros, latMaxMicros;
    }

    /** Builds the ActiveDirectoryServicePrincipal JDBC URL for the given client id/secret. */
    private static String jdbcUrl(String clientId, String secret) {
        return String.format(
                "jdbc:sqlserver://%s:1433;database=%s;"
                        + "encrypt=true;trustServerCertificate=false;"
                        + "hostNameInCertificate=*.database.windows.net;"
                        + "loginTimeout=3000;"
                        + "connectionTimeout=3000;"
                        // I disable connection-level retry here (see the "Retry configuration" note in
                        // the file header; I also patch INTERMITTENT_TLS_MAX_RETRY=0 in the driver build).
                        + "connectRetryCount=0;"
                        + "authentication=ActiveDirectoryServicePrincipal;"
                        + "AADSecurePrincipalId=%s;"
                        + "AADSecurePrincipalSecret=%s",
                Creds.SERVER_FQDN, Creds.DATABASE_NAME, clientId, secret);
    }
}
