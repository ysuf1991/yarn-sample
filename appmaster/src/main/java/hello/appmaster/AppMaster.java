package hello.appmaster;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Records;
import org.springframework.stereotype.Component;

@Component
public class AppMaster {
    private static final Log log = LogFactory.getLog(AppMaster.class);
    public void sub() throws Exception {
        // Initialize clients to ResourceManager and NodeManagers
        Configuration conf = new YarnConfiguration();

        AMRMClient<AMRMClient.ContainerRequest> rmClient = AMRMClient.createAMRMClient();
        rmClient.init(conf);
        rmClient.start();

        NMClient nmClient = NMClient.createNMClient();
        nmClient.init(conf);
        nmClient.start();

        // Register with ResourceManager
        log.info("registerApplicationMaster 0");
        rmClient.registerApplicationMaster("", 0, "");
        log.info("registerApplicationMaster 1");

        // Priority for worker containers - priorities are intra-application
        Priority priority = Records.newRecord(Priority.class);
        priority.setPriority(0);

        // Resource requirements for worker containers
        Resource capability = Records.newRecord(Resource.class);
        capability.setMemory(128);
        capability.setVirtualCores(1);

        // Make container requests to ResourceManager
//        for (int i = 0; i < 1; ++i) {
//            AMRMClient.ContainerRequest containerAsk = new AMRMClient.ContainerRequest(capability, null, null, priority);
//            log.info("Making res-req " + i);
//            rmClient.addContainerRequest(containerAsk);
//        }

        // Obtain allocated containers, launch and check for responses
//        int responseId = 0;
//        int completedContainers = 0;
//        while (completedContainers < n) {
//            AllocateResponse response = rmClient.allocate(responseId++);
//            for (Container container : response.getAllocatedContainers()) {
//                // Launch container by create ContainerLaunchContext
//                ContainerLaunchContext ctx =
//                    Records.newRecord(ContainerLaunchContext.class);
//                ctx.setCommands(
//                    Collections.singletonList(
//                        command +
//                        " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout" +
//                        " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr"
//                    ));
//                log.info("Launching container " + container.getId());
//                nmClient.startContainer(container, ctx);
//            }
//            for (ContainerStatus status : response.getCompletedContainersStatuses()) {
//                ++completedContainers;
//                log.info("Completed container " + status.getContainerId());
//            }
//            Thread.sleep(100);
//        }

        // Un-register with ResourceManager
        rmClient.unregisterApplicationMaster(
            FinalApplicationStatus.SUCCEEDED, "", "");
        Thread.sleep(1000);
    }
}
