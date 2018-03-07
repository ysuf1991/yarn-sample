package hello.appmaster;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Records;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClientException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Component
public class ContainerLauncher {

    private static final Log log = LogFactory.getLog(ContainerLauncher.class);

    private final String command = "$JAVA_HOME/bin/java ";

    private final ResourceLocalize resourceLocalize;
    private final AMRMClient amrmClient;
    private final NMClient nmClient;

    public ContainerLauncher(ResourceLocalize resourceLocalize, AMRMClient amrmClient, NMClient nmClient) {
        this.resourceLocalize = resourceLocalize;
        this.amrmClient = amrmClient;
        this.nmClient = nmClient;
    }

    public List<Container> allocate(int count, int memory, int vCores) {
        List<AMRMClient.ContainerRequest> containerRequest = createContainerRequest(count, memory, vCores);
        return startContainers(containerRequest);
    }

    public void allocate(int count) {
        allocate(1, 128, 1);
    }

    private List<AMRMClient.ContainerRequest> createContainerRequest(int count, int memory, int vCores) {
        Priority priority = Records.newRecord(Priority.class);
        priority.setPriority(0);

        Resource capability = Records.newRecord(Resource.class);
        capability.setMemory(memory);
        capability.setVirtualCores(vCores);

        return IntStream.range(0, count)
            .mapToObj(ignored -> new AMRMClient.ContainerRequest(capability, null, null, priority))
            .collect(Collectors.toList());
    }

    private List<Container> startContainers(List<AMRMClient.ContainerRequest> containerRequest){
        int responseId = 0;
        int completedContainers = 0;
        List<Container> containers = new ArrayList<>();
        HashMap<String, String> env = new HashMap<>();
        resourceLocalize.setupEnv(env);
        containerRequest.forEach(amrmClient::addContainerRequest);
        try {
            while (completedContainers < containerRequest.size()) {
                AllocateResponse response = amrmClient.allocate(responseId++);
                for (Container container : response.getAllocatedContainers()) {
                    createContainerLaunchContext(env, container);
                    nmClient.startContainer(container, createContainerLaunchContext(env, container));
                    containers.add(container);
                    completedContainers++;
                }
            }
            return containers;
        } catch (YarnException | IOException e) {
            log.info("cannot allocate containers");
            throw new RestClientException("cannot allocate containers");
        } finally {
            containerRequest.forEach(amrmClient::removeContainerRequest);
            containers.forEach(c -> amrmClient.releaseAssignedContainer(c.getId()));
        }
    }

    private ContainerLaunchContext createContainerLaunchContext(HashMap<String, String> env, Container container) {
        ContainerLaunchContext ctx = Records.newRecord(ContainerLaunchContext.class);
        ctx.setCommands(Collections.singletonList(
            command + " -jar container-1.0.0.jar " +
            " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout" +
            " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr"
            ));
        log.info("Launching container " + container.getId());
        ctx.setLocalResources(Collections.singletonMap("container-1.0.0.jar", resourceLocalize.getContainerJar()));
        ctx.setEnvironment(env);
        return ctx;
    }
}
