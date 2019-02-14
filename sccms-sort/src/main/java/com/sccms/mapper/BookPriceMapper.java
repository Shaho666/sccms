package com.sccms.mapper;

import java.io.IOException;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;

public class BookPriceMapper extends TableMapper<FloatWritable, Text> {

	@Override
	public void map(ImmutableBytesWritable row, Result result, Context context)
			throws InterruptedException, IOException {
		
		Text bookName = null;
		FloatWritable price = new FloatWritable();
		byte[] byteName = null;
		byte[] bytePrice = null;

		for (Cell cell : result.rawCells()) {
			byteName = CellUtil.cloneRow(cell);
			bytePrice = CellUtil.cloneValue(cell);
			bookName = new Text(Bytes.toString(byteName));
			price = new FloatWritable(Bytes.toFloat(bytePrice));
			context.write(price, bookName);
			System.out.println("书名：" + bookName + " 价格：" + price);

		}

	}

}
