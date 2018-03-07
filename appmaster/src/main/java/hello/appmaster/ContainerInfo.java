package hello.appmaster;

import org.apache.hadoop.yarn.api.records.Container;

public class ContainerInfo {

    private long id;
    private long memory;
    private long vCores;

    public ContainerInfo(Container container) {
        id = container.getId().getContainerId();
        memory = container.getResource().getMemory();
        vCores = container.getResource().getVirtualCores();
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public long getMemory() {
        return memory;
    }

    public void setMemory(long memory) {
        this.memory = memory;
    }

    public long getvCores() {
        return vCores;
    }

    public void setvCores(long vCores) {
        this.vCores = vCores;
    }
}
