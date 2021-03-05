import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;

import java.io.IOException;
import java.util.EnumSet;
import java.util.List;

public class taskState {
    public static void main(String[] args) {
        System.out.println(">>>begin");
        String res = taskState.getAppId("monitot_DB");
        System.out.println(res);
        System.out.println(">>>end");
    }
    /**
     * 获取任务的applicationId
     * @return String
     * @param jobName
     * @return
     */

    public static String getAppId(String jobName) {
        YarnClient client = YarnClient.createYarnClient();
        Configuration conf = new Configuration();
        client.init(conf);
        client.start();
        EnumSet<YarnApplicationState> appStates = EnumSet.noneOf(YarnApplicationState.class);
        if (appStates.isEmpty()) {
            appStates.add(YarnApplicationState.RUNNING);
            appStates.add(YarnApplicationState.ACCEPTED);
            appStates.add(YarnApplicationState.SUBMITTED);
        }
        List<ApplicationReport> appsReport = null;
        try {
            //返回EnumSet<YarnApplicationState>中个人任务状态的所有任务
            appsReport = client.getApplications(appStates);
        } catch (YarnException | IOException e) {
            e.printStackTrace();
        }
        assert appsReport != null;
        for (ApplicationReport appReport : appsReport) {
            //获取任务名
            String jn = appReport.getName();
            String applicationType = appReport.getApplicationType();
            if (jn.equals(jobName)) {
                try {
                    client.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                return appReport.getApplicationId().toString();
            }
        }
        try {
            client.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
}
