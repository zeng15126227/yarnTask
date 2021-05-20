import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.logaggregation.AggregatedLogFormat;
import org.apache.hadoop.yarn.logaggregation.LogAggregationUtils;
import org.apache.hadoop.yarn.util.ConverterUtils;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.PrintStream;

public class GetYarnLog {
    public static void main(String[] args) {
        try {
            run(args[0]);
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }
    }

    public static int run(String appIdStr) throws Throwable{


        Configuration conf = new YarnConfiguration();
        conf.addResource(new Path("core-site.xml"));
        conf.addResource(new Path("yarn-site.xml"));
        conf.addResource(new Path("hdfs-site.xml"));
        if(appIdStr == null || appIdStr.equals(""))
        {
            System.out.println("appId is null!");
            return -1;
        }
        PrintStream out=new PrintStream(appIdStr);
        ApplicationId appId = null;
        appId = ConverterUtils.toApplicationId(appIdStr);

        Path remoteRootLogDir = new Path(conf.get("yarn.nodemanager.remote-app-log-dir", "/tmp/logs"));

        String user =  UserGroupInformation.getCurrentUser().getShortUserName();;
        String logDirSuffix = LogAggregationUtils.getRemoteNodeLogDirSuffix(conf);

        Path remoteAppLogDir = LogAggregationUtils.getRemoteAppLogDir(remoteRootLogDir, appId, user, logDirSuffix);
        RemoteIterator<FileStatus> nodeFiles;
        try
        {
            Path qualifiedLogDir = FileContext.getFileContext(conf).makeQualified(remoteAppLogDir);
            nodeFiles = FileContext.getFileContext(qualifiedLogDir.toUri(), conf).listStatus(remoteAppLogDir);
        }
        catch (FileNotFoundException fnf)
        {
            System.out.println("日志不存在");
            return -1;
        }

        boolean foundAnyLogs = false;
        while (nodeFiles.hasNext())
        {
            FileStatus thisNodeFile = (FileStatus)nodeFiles.next();
            if (!thisNodeFile.getPath().getName().endsWith(".tmp"))
            {
                System.out.println("NodeFileName = "+thisNodeFile.getPath().getName());
                AggregatedLogFormat.LogReader reader = new AggregatedLogFormat.LogReader(conf, thisNodeFile.getPath());
                try
                {
                    AggregatedLogFormat.LogKey key = new AggregatedLogFormat.LogKey();
                    DataInputStream valueStream = reader.next(key);
                    for (;;)
                    {
                        if (valueStream != null)
                        {
                            String containerString = "\n\nContainer: " + key + " on " + thisNodeFile.getPath().getName();

                            out.println(containerString);
                            out.println(StringUtils.repeat("=", containerString.length()));
                            try
                            {
                                for (;;)
                                {
                                    AggregatedLogFormat.LogReader.readAContainerLogsForALogType(valueStream, out, thisNodeFile.getModificationTime());

                                    foundAnyLogs = true;
                                }

                            }
                            catch (EOFException eof)
                            {
                                key = new AggregatedLogFormat.LogKey();
                                valueStream = reader.next(key);

                            }

                        }else{
                            break;
                        }
                    }
                }
                finally
                {
                    reader.close();
                }
            }
        }
        if (!foundAnyLogs)
        {
            System.out.println("空的日志目录");
            return -1;
        }
        return 0;
    }
}
