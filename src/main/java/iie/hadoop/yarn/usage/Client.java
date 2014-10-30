package iie.hadoop.yarn.usage;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Records;

import com.google.common.collect.Lists;

public class Client {
	public static void main(String[] args) throws IOException, YarnException {
		Configuration conf = new Configuration();
		YarnConfiguration yarnConf = new YarnConfiguration(conf);
		InetSocketAddress rmAddress = NetUtils.createSocketAddr(yarnConf.get(
				YarnConfiguration.RM_ADDRESS,
				YarnConfiguration.DEFAULT_RM_ADDRESS));
		Configuration appsManagerServerConf = new Configuration(conf);
		ApplicationClientProtocol applicationsManager = ((ApplicationClientProtocol) RPC
				.getProxy(
						ApplicationClientProtocol.class,
						RPC.getProtocolVersion(ApplicationClientProtocol.class),
						rmAddress, appsManagerServerConf));

		GetNewApplicationRequest request = Records
				.newRecord(GetNewApplicationRequest.class);
		GetNewApplicationResponse newApp = applicationsManager
				.getNewApplication(request);
		ApplicationId applicationId = newApp.getApplicationId();

		SubmitApplicationRequest submitApplication = Records
				.newRecord(SubmitApplicationRequest.class);
		ApplicationSubmissionContext context = Records
				.newRecord(ApplicationSubmissionContext.class);
		context.setApplicationId(applicationId);
		context.setQueue("default");
		context.setApplicationName("test");
		
		ContainerLaunchContext amContext = Records.newRecord(ContainerLaunchContext.class);
		String command = "";
		amContext.setCommands(Lists.newArrayList(command));
		submitApplication.setApplicationSubmissionContext(context);
		applicationsManager.submitApplication(submitApplication);
	}
}
