package hello.container;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.hadoop.fs.FsShell;
import org.springframework.yarn.annotation.OnContainerStart;
import org.springframework.yarn.annotation.YarnComponent;

import java.net.URI;
import java.time.LocalDateTime;

@YarnComponent
public class HelloPojo {

	private static final Log log = LogFactory.getLog(HelloPojo.class);

	@Autowired
	private Configuration configuration;

	@OnContainerStart
	public void publicVoidNoArgsMethod() throws Exception {
		log.info("Hello from HelloPojo");
		log.info("About to list from hdfs root content");

		String ss = "";
		FsShell shell = new FsShell(configuration);
		for (FileStatus s : shell.ls(false, "/")) {
			log.info(s);
			ss += s.toString();
		}
		shell.close();
		FileSystem fileSystem = FileSystem.get(new URI("hdfs://sandbox-hdp.hortonworks.com:8020/"), configuration);
		FSDataOutputStream outputStream=fileSystem.create(new Path("/app/gs-yarn-basic/" + System.currentTimeMillis()));
		//Cassical output stream usage
		outputStream.writeBytes(ss);
		outputStream.close();
	}

}
