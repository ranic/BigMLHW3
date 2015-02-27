import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

/**
 * Created by vijay on 2/27/15.
 */
public class run {

    public static void main(String[] args) throws Exception {
        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);
        int numReducers = Integer.valueOf(args[2]);

        JobConf conf = new JobConf(NB_train_hadoop.class);
        conf.setJobName("Naive Bayes");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);

        conf.setMapperClass(NB_train_hadoop.TokenizerMapper.class);
        conf.setCombinerClass(NB_train_hadoop.IntSumReducer.class);
        conf.setReducerClass(NB_train_hadoop.IntSumReducer.class);

        FileInputFormat.setInputPaths(conf, inputPath);
        FileOutputFormat.setOutputPath(conf, outputPath);

        conf.setNumReduceTasks(numReducers);

        JobClient.runJob(conf);
    }
}
