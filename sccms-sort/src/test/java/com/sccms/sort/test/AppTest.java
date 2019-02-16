package com.sccms.sort.test;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.junit.Test;

import com.sccms.mapper.BookPriceMapper;
import com.sccms.reducer.BookPriceReducer;

import scala.Tuple2;

public class AppTest {

	@Test
	public void testSortBook() throws Exception {

		System.setProperty("hadoop.home.dir", "E:\\hadoop\\hadoop-common-2.7.4-bin");
		Configuration configuration = HBaseConfiguration.create();
		configuration.set("hbase.zookeeper.quorum", "192.168.16.129");
		configuration.set("hbase.zookeeper.property.clientPort", "2181");

		Job job = Job.getInstance(configuration);
		job.setJarByClass(AppTest.class);

		Scan scan = new Scan();
		scan.addFamily("bookName-price".getBytes());

		TableMapReduceUtil.initTableMapperJob("BookPrice", // 数据库表名
				scan, // Scan实例，控制列族及属性的选择
				BookPriceMapper.class, // mapper类
				FloatWritable.class, // mapper输出键
				Text.class, // mapper输出值
				job);
		TableMapReduceUtil.initTableReducerJob("BookPriceSorted", // 设置输出表
				BookPriceReducer.class, // 设置TableReducer类
				job);

		job.setNumReduceTasks(1);
		job.waitForCompletion(true);

	}

	public static void main(String[] args) throws Exception {

		System.setProperty("hadoop.home.dir", "E:\\hadoop\\hadoop-common-2.7.4-bin");
		// 1.先创建conf对象进行配置，主要是设置名称，为了设置运行模式
		SparkConf conf = new SparkConf();
		conf.setAppName("JavaWordCount");
		conf.setMaster("spark://192.168.16.129:7077");
		// 2.创建context对象
		JavaSparkContext jsc = new JavaSparkContext(conf);
		JavaRDD<String> lines = jsc.textFile("word.txt");
		// 3.进行切分数据 --flatMapFunction是具体实现类
		JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
			private static final long serialVersionUID = 1L;

			// Iterable是所有集合的超级父接口
			@Override
			public Iterable<String> call(String s) throws Exception {
				List<String> splited = Arrays.asList(s.split(" "));
				return splited;
			}
		});
		// 4.将数据生成元组
		// 第一个泛型是输入的数据类型，后两个参数是输出参数元组的数据
		JavaPairRDD<String, Integer> tuples = words.mapToPair(new PairFunction<String, String, Integer>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Integer> call(String s) throws Exception {
				return new Tuple2<String, Integer>(s, 1);
			}
		});
		// 5.聚合
		JavaPairRDD<String, Integer> sumed = tuples.reduceByKey(new Function2<Integer, Integer, Integer>() {
			private static final long serialVersionUID = 1L;

			/**
			 *
			 * @param v1 相同key对应的value
			 * @param v2 相同key对应的value
			 * @return
			 * @throws Exception
			 */
			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}
		});
		// 因为Java API 没有提供sortedBy 算子，此时需要将元组中的数据进行位置调换，排完序再换回来
		// 第一次交换是为了排序
		JavaPairRDD<Integer, String> swaped = sumed
				.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<Integer, String> call(Tuple2<String, Integer> tup) throws Exception {
						return tup.swap();
					}
				});
		// 排序 J
		JavaPairRDD<Integer, String> sorted = swaped.sortByKey(false);
		// 第二次交换是为了最终结果 <单词，数量>
		JavaPairRDD<String, Integer> res = sorted
				.mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String, Integer> call(Tuple2<Integer, String> tup) throws Exception {
						return tup.swap();
					}
				});

		System.out.println(res.collect());
		// res.saveAsTextFile("out4");
		//jsc.stop();
		jsc.close();

	}

}
