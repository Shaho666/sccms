package com.sccms.orm.test;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

@SuppressWarnings("unused")
public class ProductAnalyzer {

	public enum Counters {
		ROWS, COLS, VALID, ERROR
	}

	static class ProductMapper extends TableMapper<Text, IntWritable> {
		private IntWritable ONE = new IntWritable(1);

		@SuppressWarnings("deprecation")
		@Override
		public void map(ImmutableBytesWritable row, Result columns, Context context) throws IOException {
			// increment the row counter here
			context.getCounter(Counters.ROWS).increment(1);
			String value = null;
			try {
				for (KeyValue kv : columns.list()) {
					context.getCounter(Counters.COLS).increment(1);
					// get the value
					value = Bytes.toStringBinary(kv.getValue());
					// emit the key/value out
					context.write(new Text(value), ONE);
					context.getCounter(Counters.VALID).increment(1);
				}
			} catch (Exception e) {
				e.printStackTrace();
				System.err.println("Row: " + Bytes.toStringBinary(row.get()) + ", value = " + value);
				context.getCounter(Counters.ERROR).increment(1);
			}

		}

	}

	static class ProductReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int count = 0;
			// increment count for each of the values in the Iterable passed in.
			for (IntWritable one : values)
				count++;
			// emit the Key and total count out of the Reducer
			context.write(key, new IntWritable(count));
		}
	}

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// create String table
		String table = args[0];
		// create String output
		String output = args[1];
		// create the scan object with the attribute
		Scan scan = new Scan();
		// to scan to just grab the cf:pdk column
		scan.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("pdk"));

		System.setProperty("hadoop.home.dir", "E:\\hadoop\\hadoop-common-2.7.4-bin");
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "192.168.16.129");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		//conf.set("hbase.nameserver.address", "nn1,nn2");
		//conf.set("mapreduce.framework.name", "yarn");

		Job job = new Job(conf, "Analyze product key");
		// set the class of the job that contains the mapper and reducer
		job.setJarByClass(ProductAnalyzer.class);
		// Use the TableMapReduceUtil.initTableMapperJob() method to set up your job.
		TableMapReduceUtil.initTableMapperJob(table, scan, ProductMapper.class, Text.class, IntWritable.class, job);
		// set up the reducer class and its output types
		job.setReducerClass(ProductReducer.class);
		// set the output format
		FileOutputFormat.setOutputPath(job, new Path(output));
		job.setOutputKeyClass(Text.class);
		// set the number of reduce task
		job.setOutputValueClass(IntWritable.class);
		// submit the job
		job.setNumReduceTasks(1);
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}
