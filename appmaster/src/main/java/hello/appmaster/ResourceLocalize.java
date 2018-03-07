package hello.appmaster;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClientException;

import java.io.File;
import java.net.URI;
import java.util.Map;

@Component
public class ResourceLocalize {

    private final static String HDFS = "hdfs://sandbox-hdp.hortonworks.com:8020";
    private final static String LOCAL_DIR = "/app/gs-yarn-basic/";
    private final static String JAR = "container-1.0.0.jar";

    private final YarnConfiguration configuration;

    private final LocalResource containerJar;

    public ResourceLocalize(YarnConfiguration configuration) {
        this.configuration = configuration;
        containerJar = setupLocalResource();
    }

    public LocalResource getContainerJar() {
        return containerJar;
    }

    private LocalResource setupLocalResource() {
        LocalResource resource = Records.newRecord(LocalResource.class);
        try {
            FileSystem fileSystem = FileSystem.get(new URI(HDFS), configuration);

            FileStatus jarStat = fileSystem.getFileStatus(new Path(LOCAL_DIR + JAR));
            resource.setResource(ConverterUtils.getYarnUrlFromPath(new Path(new URI(HDFS + LOCAL_DIR + JAR))));
            resource.setSize(jarStat.getLen());
            resource.setTimestamp(jarStat.getModificationTime());
            resource.setType(LocalResourceType.FILE);
            resource.setVisibility(LocalResourceVisibility.PUBLIC);
        } catch (Exception ex) {
            throw new RestClientException("Cannot load local resources", ex);
        }

        return resource;
    }

    public void setupEnv(Map<String, String> env) {
        String[] classPaths = configuration.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH,
                                              YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH);
        for (String c : classPaths) {
            Apps.addToEnvironment(env, ApplicationConstants.Environment.CLASSPATH.name(), c.trim());
        }
        Apps.addToEnvironment(env, ApplicationConstants.Environment.CLASSPATH.name(),
                              ApplicationConstants.Environment.PWD.$() + File.separator + "*");

    }
}
