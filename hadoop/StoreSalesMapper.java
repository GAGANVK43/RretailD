package com.retail.mapreduce;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * StoreSalesMapper
 * Input Key: Byte offset (LongWritable)
 * Input Value: Raw CSV transaction line (Text)
 * Output Key: Store Location (Text)
 * Output Value: Sale Amount (DoubleWritable)
 */
public class StoreSalesMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

    private Text storeKey = new Text();
    private DoubleWritable salesValue = new DoubleWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString().trim();
        
        // Skip empty lines or header row
        if (line.isEmpty() || line.toLowerCase().startsWith("date") || line.toLowerCase().startsWith("transaction_id")) {
            return;
        }

        String delimiter = line.contains(",") ? "," : "\t";
        String[] tokens = line.split(delimiter);

        try {
            // Expected columns: date, time, store, category, sales_amount, payment_method
            if (tokens.length >= 5) {
                String store = tokens[2].trim();
                double amount = Double.parseDouble(tokens[4].trim());

                storeKey.set(store);
                salesValue.set(amount);
                context.write(storeKey, salesValue);
            } else if (tokens.length == 2) {
                // store, sales_amount
                String store = tokens[0].trim();
                double amount = Double.parseDouble(tokens[1].trim());

                storeKey.set(store);
                salesValue.set(amount);
                context.write(storeKey, salesValue);
            }
        } catch (NumberFormatException e) {
            // Skip records with invalid number formatting
        }
    }
}
