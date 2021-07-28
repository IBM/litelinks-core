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
package com.ibm.watson.litelinks.utilities;

import com.ibm.watson.litelinks.LitelinksEnvVariableNames;
import com.ibm.watson.litelinks.LitelinksSystemPropNames;
import com.ibm.watson.litelinks.client.LitelinksServiceClient;
import com.ibm.watson.litelinks.client.LitelinksServiceClient.ServiceInstanceInfo;
import com.ibm.watson.litelinks.client.ThriftClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * A utility to perform health checking on a particular instance of a
 * Litelinks service.
 * <p>
 * This class is designed to be usable as a command-line health checking
 * job for the Mesos cluster management software.
 */
public class CheckInstanceHealth {

    /** Logger we use for debugging */
    private static final Logger logger = LoggerFactory.getLogger(CheckInstanceHealth.class);

    /** The service name whose health we're checking. */
    String serviceName;

    /** The instance id of the particular instance of the service whose health we're checking. */
    String instanceId;

    /** The Zookeeper connection string we're using to do service discovery. */
    String zookeeperConnectionStr;

    /** The timeout to use for the connection check, in seconds, preset to the default. */
    long timeoutSeconds = 10;

    /** Whether to only fail if the service is registered but not responding */
    boolean livenessCheck = false;

    /** Create a health checker for an instance of a particular service. */
    private CheckInstanceHealth(String[] args) {
        if (!parseCommandLine(args)) {
            giveUsage();
        }
    }

    /**
     * The main entry point
     *
     * @param args The command-line arguments
     */
    public static void main(String[] args) {
        try {
            new CheckInstanceHealth(args).checkHealth();
        } catch (Throwable t) {
            // just in case, force exit
            t.printStackTrace();
            logger.error("Health check failed:", t);
            System.exit(1);
        }
        System.exit(0);     // Exit even if non-daemon threads still exist
    }

    /**
     * Parse the command-line arguments, setting instance fields.
     *
     * @param args The command-line arguments.
     * @return <code>true</code> if the arguments are parsed
     * successfully, otherwise <code>false</code>.
     */
    boolean parseCommandLine(String[] args) {
        if (args.length % 2 != 0) {
            return false;  // Must have an even number of args
        }

        for (int i = 0; i < args.length; i += 2) {
            String o = args[i], v = args[i + 1];
            switch (o) {
            case "-n":
                serviceName = v;
                break;

            case "-i":
                instanceId = v;
                break;

            case "-t":
                try {
                    timeoutSeconds = Long.parseLong(v);
                } catch (NumberFormatException e) {
                    System.err.println("The timeout value \"" + v + "\" isn't an integer.");
                    return false;
                }
                break;

            case "-z":
                zookeeperConnectionStr = v;
                break;

            case "-l":
                livenessCheck = Boolean.parseBoolean(v);
                break;

            default:
                System.err.println("Unknown option \"" + o + "\" specified.");
                return false;
            }
        }

        // Give all necessary error messages before exiting due to an error

        if (serviceName == null) {
            System.err.println("The -n option is required, to specify the Litelinks service name.");
        }

        if (instanceId == null) {
            System.err.println("The -i option is required, to specify the Litelinks instance id.");
        }

        if (timeoutSeconds <= 0) {
            System.err.println("The timeout value must be positive.");
        }

        return instanceId != null && serviceName != null && timeoutSeconds > 0;
    }

    /**
     * Print command usage information and exit.
     */
    static void giveUsage() {
        System.out.println("\nUsage: -n serviceName -i instanceId [-t timeout] [-z zookeeperConn]\n\n" +
                           "Where:\n" +
                           " -n serviceName\n" +
                           "    The Litelinks service name to check the health of.\n" +
                           "    This argument is required.\n\n" +
                           " -i instanceId\n" +
                           "    The specific Litelinks instance of the given service name to check.\n" +
                           "    This argument is required.\n\n" +
                           " -t seconds\n" +
                           "    The timeout, in seconds, to use for the connection test.  If not specified,\n" +
                           "    the default is 10 seconds.\n\n" +
                           " -z instanceId\n" +
                           "    The Zookeeper connection string to use for service discovery.\n" +
                           "    If this option is not specified, one of the equivalent env vars must be set.\n" +
                           " -l true\n" +
                           "    If specified, the healthcheck won't fail if the instance is not registered,\n" +
                           "    it will only fail if the instance is registered *and* does not respond.\n"
        );
        System.exit(1);
    }

    /**
     * Check the health of the specified service and instance id of that service.
     * <p>
     * If the instance is healthy, this method will return.  Otherwise, it will
     * print an error message and exit with a non-zero status code.
     */
    void checkHealth() {
        // if the server registry sysprop or env var is set, ensure the client registry
        // sysprop is set to the same thing (since we need to find our own instance)
        String regString = System.getProperty(LitelinksSystemPropNames.SERVER_REGISTRY);
        if (regString == null) {
            regString = System.getenv(LitelinksEnvVariableNames.SERVER_REGISTRY);
        }
        if (regString != null) {
            System.setProperty(LitelinksSystemPropNames.CLIENT_REGISTRIES, regString);
        }

        // Create a generic Litelinks client for the service name
        logger.debug("Building a generic Litelinks client for service name \"" + serviceName + '"');
        ThriftClientBuilder<LitelinksServiceClient> builder = ThriftClientBuilder.newBuilder().
                withServiceName(serviceName);
        if (zookeeperConnectionStr != null) {
            builder.withZookeeper(zookeeperConnectionStr);
        }
        try (LitelinksServiceClient client = builder.build()) {

            // Find the information for the specific instance whose health we want to check
            List<ServiceInstanceInfo> serviceInstances = client.getServiceInstanceInfo();
            ServiceInstanceInfo infoForInstance = null;
            for (ServiceInstanceInfo instanceInfo : serviceInstances) {
                if (instanceInfo.getInstanceId().equals(instanceId)) {
                    infoForInstance = instanceInfo;
                    break;
                }
            }

            // Did we find the info object for the instance we're looking for?
            if (infoForInstance == null) {
                logger.error("Instance id \"" + instanceId + "\" not found for service name \"" + serviceName + "\".");
                System.err.println(
                        "Instance id \"" + instanceId + "\" not found for service name \"" + serviceName + "\".");
                // For debugging this:
                logger.info("Service \"" + serviceName + "\" has " + serviceInstances.size() + " instances:");
                for (ServiceInstanceInfo instanceInfo : serviceInstances) {
                    logger.info("Service has instance id \"" + instanceInfo.getInstanceId() + '"');
                }
                // although not "live", we're not necessarily "dead" either (might be starting up)
                System.exit(livenessCheck? 0 : 1);
            }

            // Perform the actual health check of this particular instance
            try {
                logger.info("Checking the health of service name \"" + serviceName + "\", instance id \"" +
                            instanceId + "\", running version \"" + infoForInstance.getVersion() +
                            "\", isActive==" + infoForInstance.isActive());
                infoForInstance.testConnection(timeoutSeconds * 1000);
                logger.info("Health check is successful");

            } catch (Exception e) {
                logger.error("Health check failed:", e);
                System.err.println("Health check failed: " + e);
                System.exit(1);
            }
        }
    }
}
