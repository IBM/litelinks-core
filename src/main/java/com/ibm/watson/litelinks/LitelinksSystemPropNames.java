/*
 * Copyright 2021 IBM Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy
 * of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package com.ibm.watson.litelinks;

import com.ibm.watson.litelinks.server.ReleaseAfterResponse;

/**
 * Optional System Properties (JVM arguments) used by litelinks
 * <p>
 * NOTE:
 * SSL-related system property names (litelinks.ssl.* and com.ibm.watson.litelinks.ssl.*)
 * are defined in the {@link SSLHelper} class.
 */
public /*final*/ class LitelinksSystemPropNames {

    protected /*private*/ LitelinksSystemPropNames() {}

    // server-side

    /**
     * Size of the server-side threadpool which is used to execute the service
     * methods. Setting this to a finite value will cap the number of concurrent
     * API invocations that can be processed, others will be queued.
     * <p>
     * Value must be either an integer &gt;= 1, or of the format Fc where F is a
     * possibly fractional multiplier of the number of cores. E.g. "1.5c",
     * or the string "unlimited". "-1" can also be used as an alternative to
     * "unlimited".
     * <p>
     * The default value is "unlimited"
     */
    public static final String NUM_PROCESS_THREADS = "litelinks.numprocthreads";

    /**
     * This is the time in seconds in which each incoming request message read must
     * be completed, starting from the first data received.
     * It is important to prevent clients consuming worker threads by hanging mid-write.
     * <p>
     * The default value is "20"
     */
    public static final String SERVER_READ_TIMEOUT = "litelinks.server_read_timeout_secs";

    /**
     * Time in milliseconds allowed for the service to shutdown. This includes time to
     * quiesce in-flight requests, shutdown the service impl, and perform normal
     * deregistration. If not complete within this time, brute-force deregistration
     * will be done (e.g. disconnecting from zookeeper), followed by an immediate exit.
     * <p>
     * Different values may be specified for shutdowns triggered by "normal" means
     * (triggered by receiving "stop" on stdin or anchor-file deletion) versus those
     * triggered by a SIGTERM signal.
     * <p>
     * The format is "timeout_ms" OR "normal_timout_ms,signal_timeout_ms". The default
     * value is "30000,4000" (30 secs and 4 secs).
     * <p>
     */
    public static final String SERVER_SHUTDOWN_TIMEOUT = "litelinks.shutdown_timeout_ms";

    /**
     * Controls the type of host address used for service registration by default,
     * when none is specified via the WATSON_SERVICE_ADDRESS env var.
     * <p>
     * Local IP address is used by default, unless this is set to "localhostname",
     * in which case the local hostname is used.
     */
    public static final String DEFAULT_PUBLISH_ADDR = "litelinks.defaultaddress";

    /**
     * Alternative to the LL_PRIVATE_ENDPOINT env var. Should be of the form "host",
     * or "host:port", where host is a hostname or ip address. If port is omitted
     * the public port will be used.
     * <p>
     * Note this option should be considered BETA functionality and may change in future
     * <p>
     * Default is to not publish a private endpoint.
     */
    public static final String PRIVATE_ENDPOINT = "litelinks.private_endpoint";

    /**
     * By default service instance clusters are not required to have the same
     * connection configuration (protocol, SSL mode, etc), and clients work fine
     * when communicating with such heterogeneous clusters.
     * In some cases though it may be useful to enforce consistency. When applied
     * to a (server-side) service instance, this setting will prevent it from
     * joining a cluster unless its connection properties match (specifically
     * with the oldest registered service instance in the existing cluster).
     * <p>
     * Default is "false", set to "true" to disable.
     */
    public static final String FORCE_HOMOG_CONN_PROPS = "litelinks.force_homog_conn_props";

    /**
     * Specifies whether server threads should be cancelled if the waiting client
     * closes it's connection (for example in the case of client-side timeout). If
     * the service method is already running, it will be interrupted.
     * <p>
     * Default is "false", set to "true" to enable.
     */
    public static final String CANCEL_ON_CLIENT_CLOSE = "litelinks.cancel_on_client_close";

    /**
     * Sets the minimum time that a request must have been running before its stacktrace
     * is logged when subsequently cancelled/interrupted, in milliseconds. Applies only if
     * the {@link #CANCEL_ON_CLIENT_CLOSE} sysprop is set to true. Its purpose is to avoid
     * misbehaving clients flooding server logs with stacktraces.
     * <p>
     * Default is "4000" (4 seconds)
     */
    public static final String MIN_TIME_BEFORE_INTERRUPT_LOG = "litelinks.min_time_for_cancel_log_ms";

    /**
     * Specifies the default service registry to use for <i>registering</i> service
     * instances. Must be of the form "type:conn-string" where the format/meaning of
     * conn-string depends on the type.
     */
    public static final String SERVER_REGISTRY = "litelinks.server_registry";

    // client-side

    /**
     * The resources associated with clients of a particular service within the same
     * JVM are shared (conn pools etc). Because some applications may "build" clients
     * on-the-fly as needed, it may be inefficient to close the resources immediately
     * even if it's the "last" client of that service to be closed - another client
     * might be built in the new future which could re-use the already established
     * connections. Litelinks will delay the closing of these resources based on how
     * the total number of other services currently active, unless this property is
     * set to false.
     * <p>
     * Default is "true", set to "false" to disable.
     */
    public static final String DELAY_CLIENT_CLOSING = "litelinks.delay_client_close";

    /**
     * Controls whether "strong" unavailability checking is performed. This can be
     * needed to eliminate race conditions where sequential consistency is required
     * between different clients of a service, specifically in the case of the service
     * becoming available. Without this set, one client might see a newly registered
     * service as unavailable *after* another has already seen it as available.
     * <p>
     * Default is "true", set to "false" to disable.
     */
    public static final String VERIFY_UNAVAIL = "litelinks.verify_unavailabilty";

    /**
     * Comma-separated list of "types" of thread context to propagate to services
     * "out of band". Currently supported types are "log_mdc". The prior "context"
     * type is now always sent if present and so setting it via this system property
     * is no longer required.
     * See {@link ThreadContext} class for more details.
     * <p>
     * Default is none.
     */
    public static final String CLIENT_THREADCONTEXTS = "litelinks.threadcontexts";

    /**
     * This controls how long service instances are put into FAILED state after throwing
     * a designated "instance failure" exception (see {@link MethodInfo}). <b>NOTE</b> this
     * relates to internal resiliency logic and may not be available in future versions.
     * <p>
     * Default is "90".
     */
    public static final String INSTANCE_FAIL_DELAY_SECS = "litelinks.failed_instance_duration";

    /**
     * Specifies that when a service instance publishes an separate "private" endpoint,
     * clients should use this to communicate with it rather than the primary one, unless
     * the instance's private domain id differs from the client's.
     * <p>
     * Note this option should be considered BETA functionality and may change in future
     * <p>
     * Default is to use private endpoints only if we're also configured as a server to
     * publish a private endpoint.
     */
    public static final String USE_PRIVATE_ENDPOINTS = "litelinks.use_private";

    /**
     * Optional |-delimited list of one or more service registry target strings to
     * be used by <b>clients</b>, of the form "type:config-string", where the
     * format/meaning of config-string is type-dependent.
     * <p>
     * Individual built clients may override this with specific settings.
     */
    public static final String CLIENT_REGISTRIES = "litelinks.client_registries";

    /**
     * If true, RPCs are routed to failing instances when there are no active instances.
     * <p>
     * Default is <b>true</b>. Must set to false to disable this behaviour.
     */
    public static final String USE_FAILING_INSTANCES = "litelinks.use_failing_instances";

    /**
     * If true, all callbacks from async rpc method invocations will be made from the
     * same single thread (and therefore strictly ordered/serialized).
     * <p>
     * Default is <b>false</b>.
     */
    public static final String SERIAL_CALLBACKS = "litelinks.serialize_async_callbacks";


    // both

    /**
     * How many threads to use in the netty worker event loop group shared by
     * all clients and servers.
     * <p>
     * Default is min(8, number-of-cores).
     */
    public static final String WORKER_ELG_SIZE = "litelinks.elg_thread_count";

    /**
     * (Linux-Only) Control whether native epoll transport is used by netty. Has
     * no effect on unsupported platforms.
     * <p>
     * Default is "true" on supported platforms, set to "false" to disable.
     */
    public static final String USE_EPOLL_TRANSPORT = "litelinks.transport.use_epoll";

    /**
     * Specifies a unique "domain id" corresponding to the private network that clients
     * and/or service instances in this process belong to. Used in conjunction with the
     * private endpoint settings. Clients belonging to a particular domain will only
     * use the private endpoint of service instances belonging to the same domain
     * (or those belonging to no domain, for backwards compatibility).
     * <p>
     * Can alternatively be specified by appending to the end of the configured private
     * endpoint string, delimited with a semicolon, i.e. "host:port;domain-id"
     */
    public static final String PRIVATE_DOMAIN_ID = "litelinks.private_domain_id";

    /**
     * Tell litelinks to use the OpenSSL TLS implementation <i>if possible</i> (by
     * setting to "false"). If not set to "false", java's default JSSE impl will be
     * used (default value is "true").
     */
    public static final String USE_JDK_TLS = "litelinks.ssl.use_jdk";

    /**
     * <b>Warning: This is an experimental function and not intended for general use</b>
     * <p>
     * If set to "true", all <code>ByteBuffer</code>s returned from client methods
     * which aren't called from a server thread must be released at some point from
     * the same thread by calling {@link ReleaseAfterResponse#releaseAll()}.
     * <code>ByteBuffer</code>s passed to server threads or returned from client
     * invocations in server threads must not be used after the server method
     * returns.
     */
    public static final String POOLED_BYTEBUFFERS = "litelinks.produce_pooled_bytebufs";
}
