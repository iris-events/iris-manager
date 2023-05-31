package org.iris_events.manager;

import org.iris_events.manager.retry.RetryHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.iris_events.manager.infrastructure.InfrastructureDeclarator;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;

@ApplicationScoped
public class IrisManagerLifecycleBean {

    private static final Logger log = LoggerFactory.getLogger(IrisManagerLifecycleBean.class);

    @Inject
    InfrastructureDeclarator infrastructureDeclarator;

    @Inject
    RetryHandler retryHandler;

    void onStart(@Observes StartupEvent ev) {
        log.info("Iris Manager is starting...");
        infrastructureDeclarator.declareBackboneInfrastructure();
        retryHandler.initialize();
    }
}
