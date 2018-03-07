package hello.appmaster;

import static org.apache.hadoop.yarn.api.records.FinalApplicationStatus.KILLED;
import static org.apache.hadoop.yarn.api.records.FinalApplicationStatus.SUCCEEDED;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.time.Instant;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Component
public class AppMaster {

    private static final Log log = LogFactory.getLog(AppMaster.class);

    private final AMRMClient amrmClient;

    private final ContainerLauncher containerLauncher;

    private final Map<Long, List<Container>> allocatedContainers = new HashMap<>();

    public AppMaster(AMRMClient amrmClient, ContainerLauncher containerLauncher) {
        this.amrmClient = amrmClient;
        this.containerLauncher = containerLauncher;
    }

    public List<Container> submit(int count, int memory, int vCores) {
        List<Container> allocated = containerLauncher.allocate(count, memory, vCores);
        allocatedContainers.put(Instant.now().getEpochSecond(), allocated);
        return allocated;
    }

    public void release() {
        try {
            List<ContainerId> ids = amrmClient.allocate(0)
                .getCompletedContainersStatuses()
                .stream().map(ContainerStatus::getContainerId)
                .collect(Collectors.toList());

            boolean succses = allocatedContainers.values()
                .stream()
                .flatMap(Collection::stream)
                .allMatch(c -> ids.contains(c.getId()));
            amrmClient.stop();
            amrmClient.unregisterApplicationMaster(succses ? SUCCEEDED : KILLED, "", "");

        } catch (YarnException | IOException e) {
            log.info("Failed when shutdown");
        } finally {
            amrmClient.stop();
        }
    }
}
