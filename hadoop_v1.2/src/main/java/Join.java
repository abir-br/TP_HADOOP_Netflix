import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Join {
    // Chemins d'entrée/sortie
    private static final String INPUT_PATH = "TP4_Hadoop-MapReduce/hadoop_v1.2/input-join/";
    private static final String OUTPUT_PATH = "TP4_Hadoop-MapReduce/hadoop_v1.2/output/join-";

    // Séparateur pour distinguer les sources de données dans le Reducer
    private static final String SEPARATOR = "#";
    private static final String CUSTOMER_TAG = "C"; // Tag pour les clients
    private static final String ORDER_TAG = "O";    // Tag pour les commandes

    // ==================================================================
    // MAPPER : Émettre (CustomerID, Tag + Donnée)
    // ==================================================================
    public static class JoinMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text outputKey = new Text();
        private Text outputValue = new Text();

        // Définition du séparateur Pipe pour les fichiers .tbl
        private static final String FILE_DELIMITER = "\\|";

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();

            // Ignorer les lignes d'entête (si elles existent)
            if (line.startsWith("Customer ID") || line.startsWith("Order ID")) {
                return;
            }

            // Correction: Utiliser le séparateur Pipe '|'
            String[] fields = line.split(FILE_DELIMITER);

            // Retirer les espaces blancs en fin de ligne souvent présents dans les fichiers .tbl
            if (fields.length > 0 && fields[fields.length - 1].trim().isEmpty()) {
                fields = java.util.Arrays.copyOf(fields, fields.length - 1);
            }

            // Logique de distinction des fichiers (basée sur le nombre de champs TPC-H standard)

            // Fichier CLIENT (CUSTOMERS): 8 champs (ID: 0, Name: 1)
            if (fields.length == 8) {
                // Indices basés sur CUSTOMER.tbl (C_CUSTKEY à 0, C_NAME à 1)
                String customerId = fields[0].trim();
                String customerName = fields[1].trim();

                outputKey.set(customerId);
                outputValue.set(CUSTOMER_TAG + SEPARATOR + customerName);
                context.write(outputKey, outputValue);
            }

            // Fichier COMMANDE (ORDERS): 9 champs (Cust ID: 1, Comment: 8)
            else if (fields.length == 9) {
                // Indices basés sur ORDERS.tbl (O_CUSTKEY à 1, O_COMMENT à 8)
                String customerId = fields[1].trim();
                String comment = fields[8].trim();

                outputKey.set(customerId);
                outputValue.set(ORDER_TAG + SEPARATOR + comment);
                context.write(outputKey, outputValue);
            }

            // Si le nombre de champs ne correspond à aucun des deux fichiers, la ligne est ignorée.
        }
    }

    // ==================================================================
    // REDUCER (Implémentation du Nested Loop Join)
    // ==================================================================
    public static class JoinReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            // Listes temporaires pour stocker les noms de clients et les commentaires de commandes
            List<String> customerNames = new ArrayList<>();
            List<String> orderComments = new ArrayList<>();

            // 1. Remplir les tableaux temporaires
            for (Text val : values) {
                String value = val.toString();

                // Le tag est la première partie après le séparateur
                if (value.startsWith(CUSTOMER_TAG + SEPARATOR)) {
                    // C#Name -> Name
                    customerNames.add(value.substring(2));
                } else if (value.startsWith(ORDER_TAG + SEPARATOR)) {
                    // O#Comment -> Comment
                    orderComments.add(value.substring(2));
                }
            }

            // 2. Effectuer la jointure (Boucles imbriquées / Nested Loop Join)

            // Si le client ET la commande existent pour cette clé (Inner Join)
            if (!customerNames.isEmpty() && !orderComments.isEmpty()) {

                // Boucle sur les Noms de Clients
                for (String name : customerNames) {
                    // Boucle sur les Commentaires de Commandes
                    for (String comment : orderComments) {

                        // Restituer les couples (CUSTOMERS.name, ORDERS.comment)
                        Text outputKey = new Text(name);
                        Text outputValue = new Text(comment);

                        // Écrire le résultat de la jointure
                        context.write(outputKey, outputValue);
                    }
                }
            }
        }
    }

    // ==================================================================
    // MAIN
    // ==================================================================
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = new Job(conf, "JoinData");

        job.setJarByClass(Join.class);
        job.setMapperClass(JoinMapper.class);
        job.setReducerClass(JoinReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // Définition des chemins d'entrée et de sortie (utilise les chemins corrigés)
        FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH + Instant.now().getEpochSecond()));

        job.waitForCompletion(true);
    }
}