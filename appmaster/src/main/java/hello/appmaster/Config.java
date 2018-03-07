package hello.appmaster;

import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.apache.hadoop.yarn.client.api.AMRMClient;

import java.io.IOException;

@Configuration
public class Config {

    @Autowired
    private org.apache.hadoop.conf.Configuration conf;

    @Bean
    public AMRMClient amrmClient(org.apache.hadoop.conf.Configuration conf) throws IOException, YarnException {
        AMRMClient<AMRMClient.ContainerRequest> rmClient = AMRMClient.createAMRMClient();
        rmClient.init(conf);
        rmClient.start();
        rmClient.registerApplicationMaster("", 7012, "");
        return rmClient;
    }

    @Bean
    public NMClient nmClient(org.apache.hadoop.conf.Configuration conf) {
        NMClient nmClient = NMClient.createNMClient();
        nmClient.init(conf);
        nmClient.start();
        return nmClient;
    }

    @Bean
    public static org.apache.hadoop.conf.Configuration yarnConfiguration() {
        return new YarnConfiguration();
    }

}
