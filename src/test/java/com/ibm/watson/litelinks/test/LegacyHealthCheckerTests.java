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

package com.ibm.watson.litelinks.test;

import com.google.common.util.concurrent.Service;
import com.ibm.watson.litelinks.server.LitelinksService;
import com.ibm.watson.litelinks.utilities.CheckInstanceHealth;
import org.apache.curator.test.TestingServer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.Assert.assertTrue;

/**
 * Unit tests that target the {@link CheckInstanceHealth} command
 * line utility.
 *
 * @deprecated
 */
@Deprecated
public class LegacyHealthCheckerTests {
    /**
     * The testing Zookeeper server that we use for unit tests
     */
    private static TestingServer localZk;

    /**
     * The connection string to use for unit tests
     */
    public static String ZK;

    /**
     * The Java class that we'll launch as a command line
     */
    public static Class<?> commandClass = CheckInstanceHealth.class;

    /**
     * Logger we use for debugging
     */
    private static final Logger logger = LoggerFactory.getLogger(LegacyHealthCheckerTests.class);

    @BeforeClass
    public static synchronized void setUpBeforeClass() throws Exception {
        if (ZK == null || "".equals(ZK.trim())) {
            logger.info("Starting Testing Zookeeper service");
            localZk = new TestingServer();
            ZK = localZk.getConnectString();
        }
        logger.info("Using Zookeeper connection string: " + ZK);
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        logger.info("Stopping Testing Zookeeper service");
        if (localZk != null) {
            localZk.stop();
        }
    }

    @Test
    public void testInvalidOptions() throws Exception {
        expectError("The -n option is required", "-i", "unknown", "-z", "unknown");
        expectError("The -i option is required", "-n", "unknown", "-z", "unknown");
        expectError("The timeout value must be positive", "-t", "0");
    }

    @Test
    public void testServiceNotFound() throws Exception {
        expectError("not found for service name", "-n", "unknown", "-i", "unknown", "-z", ZK);
    }

    @Test
    public void testAgainstRunningService() throws Exception {
        logger.info("Creating and starting dummy Litelinks service");
        Service svc = LitelinksService
                .createService(new LitelinksService.ServiceDeploymentConfig(SimpleThriftServiceImpl.class)
                        .setZkConnString(ZK).setServiceName("jphtestservice").setInstanceId("instanceone"))
                .startAsync();

        logger.info("Waiting for dummy Litelinks service to be running");
        svc.awaitRunning();

        ExitInfo exitInfo = runHealthCommand("-n", "jphtestservice", "-i", "instanceone", "-z", ZK);
        assertTrue("The health checker for a running service and instance, with -z option, did not succeed",
                exitInfo.rc == 0);

        Map<String, String> env = new TreeMap<>();
        env.put("ZOOKEEPER", ZK);
        exitInfo = runHealthCommand(env, null, "-n", "jphtestservice", "-i", "instanceone");
        assertTrue("The health checker for a running service and instance, with ZOOKEEPER env var, did not succeed",
                exitInfo.rc == 0);

        expectError("not found for service name", "-n", "jphtestservice", "-i", "unknowninstance", "-z", ZK);

        logger.info("Stopping dummy Litelinks service");
        svc.stopAsync().awaitTerminated();
    }

    /**
     * Run a health checking command that is expected to fail and
     * and verify that we received the expected error message.
     *
     * @param message The message or fragment that we should
     *                see written to stderr.
     * @param args    The arguments of the health checking command.
     * @throws Exception Any sort of failure.
     */
    void expectError(String message, String... args) throws Exception {
        ExitInfo exitInfo = runHealthCommand(args);
        assertTrue("The health checker didn't fail as expected", exitInfo.rc != 0);
        int index = exitInfo.stderr.indexOf(message);
        assertTrue("The error message \"" + message + "\" wasn't found in the stderr outout", index != -1);
    }

    /**
     * Run a health checking command and return the exit status and
     * and error messages that were written to stderr.
     *
     * @param args The command-line arguments of the health
     *             checking command.
     * @return Encapsulates the exit status and error
     * messages written to stderr.
     * @throws Exception Any sort of failure.
     */
    ExitInfo runHealthCommand(String... args) throws Exception {
        return runHealthCommand(null, null, args);
    }

    /**
     * Run a health checking command and return the exit status and
     * and error messages that were written to stderr.
     *
     * @param env     Environment variables to add to the
     *                environment of the command, or <code>null</code>
     *                if no additions should be made.
     * @param jvmArgs Any JVM command-line args to add to the
     *                command line before the name of the Java
     *                class to run.
     * @param args    The command-line arguments of the health
     *                checking command.
     * @return Encapsulates the exit status and error
     * messages written to stderr.
     * @throws Exception Any sort of failure.
     */
    ExitInfo runHealthCommand(Map<String, String> env, String[] jvmArgs, String... args) throws Exception {
        List<String> argv = new ArrayList<>();

        StringBuilder wholeCmd = new StringBuilder("CheckInstanceHealth ");
        for (String arg : args) {
            wholeCmd.append(arg);
            wholeCmd.append(' ');
        }
        logger.info("Executing command: " + wholeCmd);

        // Create the basic command, using our Java executable and class path
        argv.add(System.getProperty("java.home") + "/bin/java");
        argv.add("-cp");
        argv.add(System.getProperty("java.class.path"));

        // Add any specified JVM arguments
        if (jvmArgs != null) {
            argv.addAll(Arrays.asList(jvmArgs));
        }

        // Add the class name to execute
        argv.add(commandClass.getName());

        // Add the command-line arguments
        if (args != null) {
            argv.addAll(Arrays.asList(args));
        }

        // Define the process we're going to launch
        ProcessBuilder processBuilder = new ProcessBuilder(argv);

        File devNull = new File("/dev/null");
        if (devNull.exists()) {
            // Reads from stdin should get immediate EOF
            processBuilder.redirectInput(devNull);
            // We don't care about output to stdout
            processBuilder.redirectOutput(devNull);
        }

        // Always remove any inherited ZOOKEEPER environment variable
        processBuilder.environment().remove("ZOOKEEPER");

        // Add or override any specified environment variables
        if (env != null) {
            processBuilder.environment().putAll(env);
        }

        logger.debug("ZOOKEEPER env var: " + processBuilder.environment().get("ZOOKEEPER"));

        // Launch the process
        Process process = processBuilder.start();

        // Read whatever the child process writes to stderr
        StringBuilder stderr = new StringBuilder();
        Reader stderrReader = new InputStreamReader(process.getErrorStream());
        char[] buffer = new char[1024];
        while (true) {
            int count = stderrReader.read(buffer);
            if (count == -1) {
                break;     // EOF
            }
            stderr.append(buffer, 0, count);
        }

        // We got EOF on the pipe to the child's stderr, so the child is ending.
        // Wait for it to complete process termination.
        return new ExitInfo(process.waitFor(), stderr.toString());
    }

    /**
     * A simple "struct"-like class to encapsulate the exit status and any
     * error messages written to stderr.
     */
    static class ExitInfo {
        int rc;
        String stderr;

        public ExitInfo(int rc, String stderr) {
            logger.info("rc==" + rc + ", stderr: " + stderr);
            this.rc = rc;
            this.stderr = stderr;
        }
    }
}
