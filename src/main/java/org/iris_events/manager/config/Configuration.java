package org.iris_events.manager.config;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;

@ConfigMapping(prefix = "iris")
public interface Configuration {

    Retry retry();

    interface Retry {
        /**
         * Number of retries for amqp messages
         */

        @WithDefault("3")
        int maxRetries();

        /**
         * Initial retry backoff interval in milliseconds
         */
        @WithDefault("5000")
        long initialInterval();

        /**
         * Retry backoff multiplier
         */
        @WithDefault("1.5")
        double retryFactor();
    }
}
