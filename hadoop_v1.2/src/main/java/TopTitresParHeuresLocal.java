import java.io.IOException;
import java.time.Instant;
import java.util.*;

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

public class TopTitresParHeuresLocal {
    // Chemins d'entrée/sortie
    private static final String INPUT_PATH = "TP4_Hadoop-MapReduce/hadoop_v1.2/input-PROJET-NETFLIX/";
    private static final String OUTPUT_PATH = "TP4_Hadoop-MapReduce/hadoop_v1.2/output/top-titres-";

    // Séparateur pour distinguer les sources de données
    private static final String SEPARATOR = "#";
    private static final String VISIONNAGE_TAG = "V"; // Tag pour les visionnages
    private static final String TITRE_TAG = "T";      // Tag pour les titres

    // ==================================================================
    // MAPPER : Émettre (id_titre, Tag + Donnée)
    // ==================================================================
    public static class NetflixMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text outputKey = new Text();
        private Text outputValue = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();

            // Ignorer les lignes d'entête
            if (line.startsWith("id_")) {
                return;
            }

            // Séparateur CSV
            String[] fields = line.split(",");

            // Fichier fait_visionnage.csv : 10 colonnes
            // id_visionnage,id_membre,id_titre,id_appareil,id_localisation,id_date_debut,id_heure_debut,duree_visionnage_secondes,est_visionnage_complet,cout_visionnage
            if (fields.length == 10) {
                try {
                    String idTitre = fields[2].trim();           // colonne 2 = id_titre
                    String dureeSecondes = fields[7].trim();     // colonne 7 = duree_visionnage_secondes

                    outputKey.set(idTitre);
                    outputValue.set(VISIONNAGE_TAG + SEPARATOR + dureeSecondes);
                    context.write(outputKey, outputValue);
                } catch (Exception e) {
                    // Ignorer les lignes mal formées
                }
            }

            // Fichier dim_titre.csv : 11 colonnes (avec header)
            // id_titre,nom_titre,type_content,genre_principal,genres_secondaires,duree_minutes,est_production_netflix,pays_origine,langue_originale,date_ajout_catalogue,age_minimum
            else if (fields.length == 11) {
                try {
                    String idTitre = fields[0].trim();  // colonne 0 = id_titre
                    String nomTitre = fields[1].trim(); // colonne 1 = nom_titre

                    outputKey.set(idTitre);
                    outputValue.set(TITRE_TAG + SEPARATOR + nomTitre);
                    context.write(outputKey, outputValue);
                } catch (Exception e) {
                    // Ignorer les lignes mal formées
                }
            }

            // Fichiers dim_membre.csv et dim_localisation.csv sont ignorés
            // car non nécessaires pour cette requête
        }
    }

    // ==================================================================
    // REDUCER : Calculer le total d'heures par titre et classer
    // ==================================================================
    public static class NetflixReducer extends Reducer<Text, Text, Text, Text> {

        // Classe interne pour stocker les résultats
        static class TitreResultat implements Comparable<TitreResultat> {
            String idTitre;
            String nomTitre;
            double heuresTotal;

            TitreResultat(String id, String nom, double heures) {
                this.idTitre = id;
                this.nomTitre = nom;
                this.heuresTotal = heures;
            }

            @Override
            public int compareTo(TitreResultat other) {
                // Tri décroissant par heures
                return Double.compare(other.heuresTotal, this.heuresTotal);
            }
        }

        // PriorityQueue pour garder les meilleurs résultats
        private PriorityQueue<TitreResultat> topTitres = new PriorityQueue<>();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            // Listes temporaires pour stocker les données
            String nomTitre = null;
            double totalSecondes = 0.0;

            // 1. Remplir les tableaux temporaires
            for (Text val : values) {
                String value = val.toString();
                String[] parts = value.split(SEPARATOR);

                if (parts.length >= 2) {
                    if (parts[0].equals(VISIONNAGE_TAG)) {
                        // V#duree_secondes
                        try {
                            double secondes = Double.parseDouble(parts[1]);
                            totalSecondes += secondes;
                        } catch (NumberFormatException e) {
                            // Ignorer les valeurs non numériques
                        }
                    } else if (parts[0].equals(TITRE_TAG)) {
                        // T#nom_titre
                        nomTitre = parts[1];
                    }
                }
            }

            // 2. Si on a les deux informations (titre et visionnage)
            if (nomTitre != null && totalSecondes > 0) {
                // Convertir secondes en heures
                double totalHeures = totalSecondes / 3600.0;

                // Ajouter au classement
                topTitres.add(new TitreResultat(key.toString(), nomTitre, totalHeures));

                // Garder seulement les 100 meilleurs (pour la mémoire)
                if (topTitres.size() > 100) {
                    topTitres.poll();
                }
            }
        }

        @Override
        protected void cleanup(Context context)
                throws IOException, InterruptedException {

            // 3. Écrire les résultats triés
            List<TitreResultat> resultats = new ArrayList<>(topTitres);
            Collections.sort(resultats);

            // Écrire l'en-tête
            context.write(new Text("Rang"), new Text("ID_Titre\tNom_Titre\tHeures_Total"));
            context.write(new Text("----"), new Text("--------\t---------\t------------"));

            // Écrire chaque résultat avec son rang
            int rang = 1;
            for (TitreResultat tr : resultats) {
                String valeur = tr.idTitre + "\t" + tr.nomTitre + "\t" +
                        String.format("%.2f", tr.heuresTotal) + " heures";
                context.write(new Text(String.valueOf(rang++)), new Text(valeur));
            }

            // Résumé final
            context.write(new Text(""), new Text(""));
            context.write(new Text("Total:"), new Text(resultats.size() + " titres classés"));
        }
    }

    // ==================================================================
    // MAIN
    // ==================================================================
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = new Job(conf, "TopTitresParHeuresLocal");

        job.setJarByClass(TopTitresParHeuresLocal.class);
        job.setMapperClass(NetflixMapper.class);
        job.setReducerClass(NetflixReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // Définition des chemins d'entrée et de sortie
        FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH + Instant.now().getEpochSecond()));

        // Un seul reducer pour avoir un classement global unique
        job.setNumReduceTasks(1);

        System.out.println("Démarrage du job MapReduce Netflix...");
        System.out.println("Input: " + INPUT_PATH);
        System.out.println("Output: " + OUTPUT_PATH + Instant.now().getEpochSecond());

        job.waitForCompletion(true);
    }
}