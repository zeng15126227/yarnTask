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
        YarnApplicationState res = taskState.getState("application_1611052690019_6010298");
        System.out.println(res.toString());
        System.out.println(">>>end");
    }

    public static volatile YarnClient client;
    public static final String APPLICATION_ID_PREFIX="application_";
    /**
     * 获取yarn-client对象
     * @return
     */
    public static synchronized YarnClient getClientInstance(){
        if(client==null){
            client = YarnClient.createYarnClient();
            Configuration conf = new Configuration();
            client.init(conf);
            client.start();
        }
        return client;
    }
    /**
     * 获取任务的applicationId
     * @return String
     * @param jobName
     * @return
     */
    public static String getAppId(String jobName) {
        YarnClient client = getClientInstance();
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

    /**
     * applicationId生成
     * @param appIdStr
     * @return
     */
    public static ApplicationId stringToAppId(String appIdStr) {
        if (!appIdStr.startsWith(APPLICATION_ID_PREFIX)) {
            throw new IllegalArgumentException("Invalid ApplicationId prefix: "
                    + appIdStr + ". The valid ApplicationId should start with prefix "
                    + APPLICATION_ID_PREFIX);
        }
        try {
            int pos1 = APPLICATION_ID_PREFIX.length() - 1;
            int pos2 = appIdStr.indexOf('_', pos1 + 1);
            if (pos2 < 0) {
                throw new IllegalArgumentException("Invalid ApplicationId: "
                        + appIdStr);
            }
            long rmId = Long.parseLong(appIdStr.substring(pos1 + 1, pos2));
            int appId = Integer.parseInt(appIdStr.substring(pos2 + 1));
            ApplicationId applicationId = ApplicationId.newInstance(rmId, appId);
            return applicationId;
        } catch (NumberFormatException n) {
            throw new IllegalArgumentException("Invalid ApplicationId: "
                    + appIdStr, n);
        }
    }

    /**
     * 根据任务的applicationId去获取任务的状态
     * @return YarnApplicationState
     * @param appId
     * @return
     */
    public static YarnApplicationState getState(String appId) {
        YarnClient client = getClientInstance();
        ApplicationId applicationId = stringToAppId(appId);
        YarnApplicationState yarnApplicationState = null;
        try {
            ApplicationReport applicationReport = client.getApplicationReport(applicationId);
            yarnApplicationState = applicationReport.getYarnApplicationState();
        } catch (YarnException | IOException e) {
            e.printStackTrace();
        }
        try {
            client.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return yarnApplicationState;
    }
}
