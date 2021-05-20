import org.apache.hadoop.conf.Configuration;
import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;

import java.io.InputStream;
import java.util.HashMap;

public class submitTask {
    public static void main(String[] args) throws Exception {

       /* HashMap<String, String> envParams = new HashMap<>();
        envParams.put("SPARK_PRINT_LAUNCH_COMMAND", "1");*/
        System.out.println(">>>begin>>>");

        SparkAppHandle spark = new SparkLauncher()
                .setAppResource(args[0])
                .setMainClass(args[1])
                .setMaster("yarn")
                .setDeployMode(args[2])
                .setConf(SparkLauncher.DRIVER_MEMORY, "1g")
                .setConf(SparkLauncher.EXECUTOR_CORES, "1")
                .setConf(SparkLauncher.EXECUTOR_MEMORY, "1g")
                .setConf("spark.yarn.queue", "ia_dzh_dev.dev")
                .setVerbose(true)
                .startApplication(new SparkAppHandle.Listener() {
                    @Override
                    public void stateChanged(SparkAppHandle handle) {
                        System.out.println("******state change******");
                    }

                    @Override
                    public void infoChanged(SparkAppHandle handle) {
                        System.out.println("******info change******");
                    }
                });

        while(!"FINISHED".equalsIgnoreCase(spark.getState().toString()) && !"FAILED".equalsIgnoreCase(spark.getState().toString())){
            System.out.println("id "+spark.getAppId());
            System.out.println("state "+spark.getAppId());
            Thread.sleep(10000);
        }
        System.out.println(">>>end>>>");
    }
}
