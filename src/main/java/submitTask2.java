import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.spark.SparkConf;
import org.apache.spark.deploy.yarn.Client;
import org.apache.spark.deploy.yarn.ClientArguments;


import java.io.*;
import java.util.*;

public class submitTask2 {
    public static void main(String[] args) {
        YarnSubmitConditions conditions = new YarnSubmitConditions();
        conditions.setAppName("min3_DBScan");
        conditions.setMaster("yarn");
        conditions.setDeployMode(args[6]);
        conditions.setDriverMemory(args[2]);
        conditions.setExecutorMemory(args[3]);
        conditions.setExecutorCores(args[4]);
        conditions.setNumExecutors(args[5]);
        conditions.setApplicationJar(args[0]);
        conditions.setMainClass(args[1]);
        conditions.setOtherArgs(Arrays.asList("1000"));

        String appId = submitSpark(conditions);

        System.out.println("application id is " + appId);
        System.out.println("Complete ....");
    }

    public static String submitSpark(YarnSubmitConditions conditions) {
        System.out.println("初始化spark on yarn参数");
        System.out.println("初始化spark on yarn客户端");

        List<String> args = Lists.newArrayList(//
                "--jar", conditions.getApplicationJar(),//
                "--class", conditions.getMainClass()//
        );
        if (conditions.getOtherArgs() != null && conditions.getOtherArgs().size() > 0) {
            for (String s : conditions.getOtherArgs()) {
                args.add("--arg");
                args.add(org.apache.commons.lang.StringUtils.join(new String[] { s }, ","));
            }
        }

        SparkConf sparkConf = new SparkConf();

        sparkConf.setMaster(conditions.getMaster());
        sparkConf.set("spark.submit.deployMode", conditions.getDeployMode());
        sparkConf.setAppName(conditions.getAppName());

        sparkConf.set("spark.driver.memory", conditions.getDriverMemory());
        sparkConf.set("spark.executor.memory", conditions.getExecutorMemory());
        sparkConf.set("spark.executor.cores", conditions.getExecutorCores());
        sparkConf.set("spark.executor.instance", conditions.getNumExecutors());
        sparkConf.set("spark.yarn.queue", "ia_dzh_dev.dev");
        //sparkConf.set("spark.yarn.jars", "hdfs://lf319-m3-002:8020/user/ubd_data/ubd_zh/zxz/jars/*.jar");
        sparkConf.set("spark.yarn.jars", "hdfs://lf-172-whx3:8020/user/spark/jars/*.jar");

        // 指定使用yarn框架
        sparkConf.set("mapreduce.framework.name", "yarn");
        // spark2.2
        // 初始化 yarn的配置
        Configuration cf = new Configuration();
        InputStream a = null;
        InputStream b = null;
        InputStream c = null;
        a = submitTask2.class.getResourceAsStream("core-site-test.xml");
        b = submitTask2.class.getClassLoader().getResourceAsStream("hdfs-site-test.xml");
        c = submitTask2.class.getClassLoader().getResourceAsStream("yarn-site-test.xml");

        cf.addResource(a);
        cf.addResource(b);
        cf.addResource(c);
        //System.setProperty("HADOOP_USER_NAME", "ubd_zh");
        System.setProperty("HADOOP_USER_NAME", "hdfs");
        cf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        ClientArguments cArgs = new ClientArguments(args.toArray(new String[0]));
        Client client = new Client(cArgs, cf, sparkConf);

        System.out.println("提交任务，任务名称：" + conditions.getAppName());

        try {
            ApplicationId appId = client.submitApplication();
            return appId.toString();
        } catch (Exception e) {
            System.out.println("提交spark任务失败\n"+e);
            return null;
        } finally {
            if (client != null) {
                client.stop();
            }
        }
    }

    private static List<String> getSparkJars(String dir) {
        List<String> items = new ArrayList<String>();
        File file = new File(dir);
        for (File item : file.listFiles()) {
            items.add(item.getPath());
        }
        return items;
    }
}
