import java.io.IOException;
import java.time.Instant;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;


public class GroupBy {
    private static final String INPUT_PATH = "input-groupBy/";
    private static final String OUTPUT_PATH = "output/groupBy-";
    private static final Logger LOG = Logger.getLogger(GroupBy.class.getName());

    static {
        System.setProperty("java.util.logging.SimpleFormatter.format", "%5$s%n%6$s");

        try {
            FileHandler fh = new FileHandler("out.log");
            fh.setFormatter(new SimpleFormatter());
            LOG.addHandler(fh);
        } catch (SecurityException | IOException e) {
            System.exit(1);
        }
    }

    // ==================================================================
    // EXERCICE 2: Profit par Customer-ID
    // ==================================================================
    public static class ProfitByCustomerMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        private Text customerId = new Text();
        private DoubleWritable profit = new DoubleWritable();

        @Override
        public void map(LongWritable key, Text value, Context context) 
                throws IOException, InterruptedException {
            String line = value.toString();
            
            if (line.startsWith("Row ID") || line.trim().isEmpty()) {
                return;
            }
            
            String[] columns = line.split(",");
            
            if (columns.length >= 21) {
                try {
                    // Customer ID à l'index 6, Profit à l'index 20
                    String customer = columns[6].trim();
                    double profitValue = Double.parseDouble(columns[20].trim());
                    
                    customerId.set(customer);
                    profit.set(profitValue);
                    context.write(customerId, profit);
                } catch (NumberFormatException e) {
                    System.err.println("Erreur de parsing pour la ligne: " + line);
                }
            }
        }
    }

    public static class ProfitByCustomerReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        private DoubleWritable totalProfit = new DoubleWritable();

        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {
            double sum = 0.0;
            
            for (DoubleWritable val : values) {
                sum += val.get();
            }
            
            totalProfit.set(sum);
            context.write(key, totalProfit);
        }
    }

    // ==================================================================
    // EXERCICE 3: Ventes par Date et State
    // ==================================================================
    public static class SalesByDateStateMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        private Text outputKey = new Text();
        private DoubleWritable sales = new DoubleWritable();
        private CSVParser parser = new CSVParserBuilder().withSeparator(',').build();

        @Override
        public void map(LongWritable key, Text value, Context context) 
                throws IOException, InterruptedException {
            String line = value.toString();

            // Ignorer l'entête ou les lignes vides
            if (line.startsWith("Row ID") || line.trim().isEmpty()) {
                return;
            }

            try {
                String[] columns = parser.parseLine(line); // Utilisation de OpenCSV

                if (columns.length > 17) {
                    String orderDate = columns[2].trim();
                    String state = columns[11].trim();
                    double salesValue = Double.parseDouble(columns[17].trim());

                    String compositeKey = orderDate + "|" + state;
                    outputKey.set(compositeKey);
                    sales.set(salesValue);
                    context.write(outputKey, sales);
                }
            } catch (Exception e) {
                System.err.println("Erreur de parsing pour la ligne: " + line);
            }
        }
    }

    public static class SalesByDateStateReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        private DoubleWritable totalSales = new DoubleWritable();

        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {
            double sum = 0.0;
            
            for (DoubleWritable val : values) {
                sum += val.get();
            }
            
            totalSales.set(sum);
            context.write(key, totalSales);
        }
    }

    // ==================================================================
    // EXERCICE 3: Ventes par Date et Category
    // ==================================================================
    public static class SalesByDateCategoryMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        private Text outputKey = new Text();
        private DoubleWritable sales = new DoubleWritable();

        @Override
        public void map(LongWritable key, Text value, Context context) 
                throws IOException, InterruptedException {
            String line = value.toString();
            
            if (line.startsWith("Row ID") || line.trim().isEmpty()) {
                return;
            }
            
            String[] columns = line.split(",");
            
            if (columns.length >= 16) {
                try {
                    // Order Date à l'index 2, Category à l'index 14, Sales à l'index 17
                    String orderDate = columns[2].trim();
                    String category = columns[14].trim();
                    double salesValue = Double.parseDouble(columns[17].trim());
                    
                    String compositeKey = orderDate + "|" + category;
                    outputKey.set(compositeKey);
                    sales.set(salesValue);
                    context.write(outputKey, sales);
                } catch (NumberFormatException e) {
                    System.err.println("Erreur de parsing pour la ligne: " + line);
                }
            }
        }
    }

    public static class SalesByDateCategoryReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        private DoubleWritable totalSales = new DoubleWritable();

        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {
            double sum = 0.0;
            
            for (DoubleWritable val : values) {
                sum += val.get();
            }
            
            totalSales.set(sum);
            context.write(key, totalSales);
        }
    }

    // ==================================================================
    // EXERCICE 3: Statistiques par commande
    // ==================================================================
    public static class OrderStatsMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text orderId = new Text();
        private Text productInfo = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) 
                throws IOException, InterruptedException {
            String line = value.toString();
            
            if (line.startsWith("Row ID") || line.trim().isEmpty()) {
                return;
            }
            
            String[] columns = line.split(",");
            
            if (columns.length >= 19) {
                try {
                    // Order ID à l'index 1, Product ID à l'index 13, Quantity à l'index 18
                    String orderID = columns[1].trim();
                    String productID = columns[13].trim();
                    String quantity = columns[18].trim();
                    
                    orderId.set(orderID);
                    productInfo.set(productID + ":" + quantity);
                    
                    context.write(orderId, productInfo);
                } catch (NumberFormatException e) {
                    System.err.println("Erreur de parsing pour la ligne: " + line);
                }
            }
        }
    }

    public static class OrderStatsReducer extends Reducer<Text, Text, Text, Text> {
        private Text result = new Text();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            Set<String> distinctProducts = new HashSet<>();
            double totalQuantity = 0.0;

            for (Text val : values) {
                String[] parts = val.toString().split(":");
                if (parts.length == 2) {
                    distinctProducts.add(parts[0]); // Product ID
                    totalQuantity += Double.parseDouble(parts[1]); // Quantity
                }
            }

            String output = "Produits distincts: " + distinctProducts.size() + ", Quantité totale: " + totalQuantity;
            result.set(output);
            context.write(key, result);
        }
    }

    
  
    public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = new Job(conf, "GroupBy");

		job.setOutputKeyClass(Text.class);
		//job.setOutputValueClass(Text.class);

		// DECOMMENTER CETTE PARTIE POUR L'EXERCICE 2 
		//job.setMapperClass(ProfitByCustomerMapper.class);
		//job.setReducerClass(ProfitByCustomerReducer.class);
		//job.setOutputValueClass(DoubleWritable.class); 

		
		// DECOMMENTER CETTE PARTIE POUR LA QUESTION 2 DE L'EXO 3
	    //job.setMapperClass(SalesByDateStateMapper.class);
	    //job.setReducerClass(SalesByDateStateReducer.class);
		//job.setOutputValueClass(DoubleWritable.class); 

		// DECOMMENTER CETTE PARTIE POUR LA QUESTION 2 DE L'EXO 3
		//job.setMapperClass(SalesByDateCategoryMapper.class);
	   // job.setReducerClass(SalesByDateCategoryReducer.class);
		//job.setOutputValueClass(DoubleWritable.class); 

		
		// QUESTION 3 EXO 3 , COMMENTEZ CETTE PARTIE POUR EXECUTEZ UNE AUTRE PARTIE DE L'EXERCICE
		job.setMapperClass(OrderStatsMapper.class);
	    job.setReducerClass(OrderStatsReducer.class);
	    job.setOutputValueClass(Text.class); 

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
		FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH + Instant.now().getEpochSecond()));

		job.waitForCompletion(true);
	}
}