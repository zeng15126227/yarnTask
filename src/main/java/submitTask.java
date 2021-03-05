import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;

import java.util.HashMap;

public class submitTask {
    public static void main(String[] args) throws Exception {
        /*HashMap<String, String> envParams = new HashMap<>();
        envParams.put("YARN_CONF_DIR", "/opt/beh/core/hadoop/etc/hadoop");
        envParams.put("HADOOP_CONF_DIR", "/opt/beh/core/hadoop/etc/hadoop");
        envParams.put("SPARK_HOME", "/opt/beh/core/spark");
        envParams.put("SPARK_PRINT_LAUNCH_COMMAND", "1");*/
        System.out.println(">>>begin>>>");
        SparkAppHandle spark = new SparkLauncher()
                .setAppResource("/data2/ubd_zh/unicom/zxz/MergeFile-1.0-SNAPSHOT.jar")
                .setMainClass("min3_region")
                .setMaster("yarn")
                .setDeployMode("client")
                .setConf(SparkLauncher.DRIVER_MEMORY,"8g")
                .setConf(SparkLauncher.EXECUTOR_CORES,"8")
                .setConf(SparkLauncher.EXECUTOR_MEMORY,"32g")
                .addAppArgs("20210221")
                .setConf("spark.yarn.queue","ia_dzh_dev.dev")
                .startApplication();

        // application执行失败重试机制
        // 最大重试次数
        boolean  failedflag = false;
        int maxRetrytimes = 3;
        int currentRetrytimes = 0;
        while (spark.getState() != SparkAppHandle.State.FINISHED) {
            currentRetrytimes ++;
            // 每6s查看application的状态（UNKNOWN、SUBMITTED、RUNNING、FINISHED、FAILED、KILLED、 LOST）
            Thread.sleep(6000L);
            System.out.println("applicationId is: " + spark.getAppId());
            System.out.println("current state: " + spark.getState());
            if (spark.getState() == SparkAppHandle.State.FAILED && currentRetrytimes > maxRetrytimes){
                System.out.println(String.format("tried launching application for %s times but failed, exit.", maxRetrytimes));
                failedflag = true;
                break;
            }
        }
        System.out.println(">>>end>>>");
    }
}
