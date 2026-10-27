package com.retail.mapreduce;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * StoreSalesReducer
 * Input Key: Store Location (Text)
 * Input Values: Iterable of sales amounts (DoubleWritable)
 * Output Key: Store Location (Text)
 * Output Value: Total Sales Amount (DoubleWritable)
 */
public class StoreSalesReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    private DoubleWritable totalSales = new DoubleWritable();

    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context)
            throws IOException, InterruptedException {
        double sum = 0.0;
        for (DoubleWritable val : values) {
            sum += val.get();
        }
        totalSales.set(Math.round(sum * 100.0) / 100.0);
        context.write(key, totalSales);
    }
}
