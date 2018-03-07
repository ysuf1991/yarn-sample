package hello.appmaster;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.yarn.boot.YarnAppmasterAutoConfiguration;
import org.springframework.yarn.boot.YarnClientAutoConfiguration;
import org.springframework.yarn.boot.YarnContainerAutoConfiguration;

@SpringBootApplication(exclude = {YarnAppmasterAutoConfiguration.class,
                                  YarnClientAutoConfiguration.class,
                                  YarnContainerAutoConfiguration.class})
public class AppmasterApplication {

	public static void main(String[] args) throws Exception {
            SpringApplication.run(AppmasterApplication.class, args);
	}
}
