package com.sccms.reducer;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;

public class BookPriceReducer extends TableReducer<FloatWritable, Text, ImmutableBytesWritable> {

	private String columnPrice;

	private enum Counters {
		ROWS, COLS, VALID, ERROR
	}

	@Override
	public void reduce(FloatWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		context.getCounter(Counters.VALID).increment(1);
		for (Text val : values) {
			byte[] rowkey = val.getBytes();
			columnPrice = key.toString();
			Put put = new Put(rowkey);// 以书名为键值，创建Put对象
			put.addColumn(Bytes.toBytes("bookName-price"), Bytes.toBytes("price"),
					Bytes.toBytes(context.getCounter(Counters.VALID).getValue()));
			// 以sort为簇列，price为列名，以columnprice为价格
			context.write(new ImmutableBytesWritable(Bytes.toBytes(columnPrice)), put);

		}
	}

}
