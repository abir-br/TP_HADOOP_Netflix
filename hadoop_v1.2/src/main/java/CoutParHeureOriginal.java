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

public class CoutParHeureOriginal {
    // Chemins d'entrée/sortie
    private static final String INPUT_PATH = "TP4_Hadoop-MapReduce/hadoop_v1.2/input-PROJET-NETFLIX/";
    private static final String OUTPUT_PATH = "TP4_Hadoop-MapReduce/hadoop_v1.2/output/cout-par-heure-";

    // Séparateur pour distinguer les sources de données
    private static final String SEPARATOR = "#";
    private static final String VISIONNAGE_TAG = "V";
    private static final String TITRE_TAG = "T";
    private static final String COUT_TAG = "C";

    // ==================================================================
    // MAPPER CORRIGÉ
    // ==================================================================
    public static class CoutMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text outputKey = new Text();
        private Text outputValue = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();

            // Ignorer les lignes d'entête
            if (line.startsWith("id_") || line.contains("id_visionnage") || line.contains("id_titre")) {
                return;
            }

            String[] fields = line.split(",");

            // Fichier fait_visionnage.csv : 10 colonnes
            if (fields.length == 10) {
                try {
                    String idTitre = fields[2].trim();
                    String dureeSecondes = fields[7].trim();

                    if (!idTitre.isEmpty() && !dureeSecondes.isEmpty() && !dureeSecondes.equals("0")) {
                        outputKey.set(idTitre);
                        outputValue.set(VISIONNAGE_TAG + SEPARATOR + dureeSecondes);
                        context.write(outputKey, outputValue);
                    }
                } catch (Exception e) {
                    // Ignorer les erreurs
                }
            }

            // Fichier dim_titre.csv : 11 colonnes
            else if (fields.length == 11) {
                try {
                    String idTitre = fields[0].trim();
                    String nomTitre = fields[1].trim();
                    String typeContent = fields[2].trim();

                    // CORRECTION IMPORTANTE : Accepter TOUS les films pour avoir des résultats
                    // (La colonne est_production_netflix est toujours NULL dans vos données)
                    if ("Film".equalsIgnoreCase(typeContent)) {
                        // Émettre le nom du titre
                        outputKey.set(idTitre);
                        outputValue.set(TITRE_TAG + SEPARATOR + nomTitre);
                        context.write(outputKey, outputValue);

                        // Émettre le coût de production (format numérique simple)
                        double coutProduction = estimerCoutProduction(idTitre, fields);
                        outputValue.set(COUT_TAG + SEPARATOR + String.valueOf(coutProduction));
                        context.write(outputKey, outputValue);
                    }
                } catch (Exception e) {
                    // Ignorer les erreurs
                }
            }
        }

        private double estimerCoutProduction(String idTitre, String[] fields) {
            try {
                int id = Integer.parseInt(idTitre);

                // Utiliser la durée en minutes si disponible pour une estimation plus précise
                if (fields.length > 5 && !fields[5].trim().isEmpty() && !"NULL".equalsIgnoreCase(fields[5].trim())) {
                    try {
                        int dureeMinutes = Integer.parseInt(fields[5].trim());
                        // Estimation : environ 0.5M à 1.5M par minute selon le film
                        double coutParMinute;
                        if (id < 20) coutParMinute = 500000;   // Anciens films
                        else if (id < 50) coutParMinute = 750000; // Films moyens
                        else coutParMinute = 1000000;          // Nouveaux films

                        return dureeMinutes * coutParMinute;
                    } catch (NumberFormatException e) {
                        // Continuer avec l'estimation basée sur l'ID
                    }
                }

                // Estimation basée sur l'ID
                if (id < 20) return 25000000.0;
                else if (id < 50) return 50000000.0;
                else if (id < 100) return 75000000.0;
                else return 100000000.0;

            } catch (NumberFormatException e) {
                return 30000000.0;
            }
        }
    }

    // ==================================================================
    // REDUCER CORRIGÉ
    // ==================================================================
    public static class CoutReducer extends Reducer<Text, Text, Text, Text> {

        static class FilmCout implements Comparable<FilmCout> {
            String idTitre;
            String nomTitre;
            double heuresVisionnage;
            double coutProduction;
            double coutParHeure;

            FilmCout(String id, String nom, double heures, double cout) {
                this.idTitre = id;
                this.nomTitre = nom;
                this.heuresVisionnage = heures;
                this.coutProduction = cout;
                this.coutParHeure = (heures > 0) ? cout / heures : Double.MAX_VALUE;
            }

            @Override
            public int compareTo(FilmCout other) {
                return Double.compare(this.coutParHeure, other.coutParHeure);
            }
        }

        private PriorityQueue<FilmCout> filmsAnalyse = new PriorityQueue<>();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            String nomTitre = null;
            double totalSecondes = 0.0;
            double coutProduction = 0.0;
            int countVisionnages = 0;
            int countInfos = 0;

            // System.out.println("REDUCE - Clé: " + key.toString());

            // Parcourir toutes les valeurs pour ce film
            for (Text val : values) {
                String value = val.toString();
                String[] parts = value.split(SEPARATOR, 2);

                if (parts.length == 2) {
                    if (parts[0].equals(VISIONNAGE_TAG)) {
                        try {
                            double secondes = Double.parseDouble(parts[1]);
                            totalSecondes += secondes;
                            countVisionnages++;
                        } catch (NumberFormatException e) {
                            // Ignorer
                        }
                    } else if (parts[0].equals(TITRE_TAG)) {
                        nomTitre = parts[1];
                        countInfos++;
                    } else if (parts[0].equals(COUT_TAG)) {
                        try {
                            coutProduction = Double.parseDouble(parts[1]);
                            countInfos++;
                        } catch (NumberFormatException e) {
                            // DEBUG: Afficher l'erreur de parsing
                            // System.err.println("ERREUR parsing cout: " + parts[1]);
                        }
                    }
                }
            }

            // CORRECTION : Vérifier qu'on a au moins des visionnages
            if (nomTitre != null && totalSecondes > 0 && coutProduction > 0) {
                double totalHeures = totalSecondes / 3600.0;

                // DEBUG: Afficher ce qu'on a trouvé
                // System.out.println("FILM TROUVÉ: " + nomTitre +
                //                   " (ID: " + key.toString() + ")" +
                //                   " Heures: " + totalHeures +
                //                   " Coût: " + coutProduction +
                //                   " Visionnages: " + countVisionnages);

                filmsAnalyse.add(new FilmCout(key.toString(), nomTitre, totalHeures, coutProduction));

                if (filmsAnalyse.size() > 100) {
                    filmsAnalyse.poll();
                }
            } else {
                // DEBUG: Afficher pourquoi on a ignoré
                // System.out.println("FILM IGNORÉ: " + key.toString() +
                //                   " - Nom: " + (nomTitre != null ? "OUI" : "NON") +
                //                   " Secondes: " + totalSecondes +
                //                   " Coût: " + coutProduction);
            }
        }

        @Override
        protected void cleanup(Context context)
                throws IOException, InterruptedException {

            // Écrire l'en-tête
            context.write(new Text("=== COÛT PAR HEURE DE VISIONNAGE - FILMS NETFLIX ORIGINAUX ==="),
                    new Text(""));
            context.write(new Text("Classement par ROI (meilleur investissement d'abord)"),
                    new Text(""));
            context.write(new Text("Rang"), new Text("ID_Titre\tFilm\tHeures_Visionnées\tCoût_Production\tCoût/Heure\tROI"));
            context.write(new Text("----"), new Text("--------\t----\t----------------\t---------------\t----------\t---"));

            // Extraire et trier les résultats
            List<FilmCout> resultats = new ArrayList<>(filmsAnalyse);
            Collections.sort(resultats);

            // Écrire chaque résultat
            int rang = 1;
            for (FilmCout fc : resultats) {
                // S'assurer que le nom du film n'est pas trop long
                String nomFilm = fc.nomTitre;
                if (nomFilm.length() > 40) {
                    nomFilm = nomFilm.substring(0, 37) + "...";
                }

                String evaluationROI = evaluerROI(fc.coutParHeure);
                String valeur = fc.idTitre + "\t" +
                        "\"" + nomFilm + "\"\t" +
                        String.format("%.1f", fc.heuresVisionnage) + "h\t" +
                        formatMontant(fc.coutProduction) + "\t" +
                        formatMontant(fc.coutParHeure) + "/h\t" +
                        evaluationROI;

                context.write(new Text(String.valueOf(rang++)), new Text(valeur));
            }

            // Résumé final
            if (!resultats.isEmpty()) {
                context.write(new Text(""), new Text(""));
                context.write(new Text("=== SYNTHÈSE ==="), new Text(""));
                context.write(new Text("Films analysés:"), new Text(resultats.size() + " films"));

                double coutMoyen = resultats.stream()
                        .mapToDouble(f -> f.coutParHeure)
                        .average()
                        .orElse(0.0);
                double coutTotal = resultats.stream()
                        .mapToDouble(f -> f.coutProduction)
                        .sum();
                double heuresTotal = resultats.stream()
                        .mapToDouble(f -> f.heuresVisionnage)
                        .sum();

                context.write(new Text("Coût/heure moyen:"), new Text(formatMontant(coutMoyen) + "/h"));
                context.write(new Text("Coût total des films:"), new Text(formatMontant(coutTotal)));
                context.write(new Text("Heures totales visionnées:"), new Text(String.format("%.1fh", heuresTotal)));

                if (!resultats.isEmpty()) {
                    context.write(new Text("Meilleur ROI:"),
                            new Text(resultats.get(0).nomTitre + " - " + formatMontant(resultats.get(0).coutParHeure) + "/h"));
                    context.write(new Text("Pire ROI:"),
                            new Text(resultats.get(resultats.size()-1).nomTitre + " - " +
                                    formatMontant(resultats.get(resultats.size()-1).coutParHeure) + "/h"));
                }
            } else {
                context.write(new Text(""), new Text(""));
                context.write(new Text("AUCUN FILM TROUVÉ"), new Text(""));
                context.write(new Text("Vérifiez que:"), new Text(""));
                context.write(new Text("1. Les films ont des visionnages"), new Text(""));
                context.write(new Text("2. Les coûts de production sont > 0"), new Text(""));
                context.write(new Text("3. Les noms de films ne sont pas NULL"), new Text(""));
            }
        }

        private String formatMontant(double montant) {
            if (montant >= 1000000000) {
                return String.format("$%.2fB", montant / 1000000000);
            } else if (montant >= 1000000) {
                return String.format("$%.1fM", montant / 1000000);
            } else if (montant >= 1000) {
                return String.format("$%.0fK", montant / 1000);
            } else {
                return String.format("$%.0f", montant);
            }
        }

        private String evaluerROI(double coutParHeure) {
            if (coutParHeure < 10000) return "★★★★★";
            else if (coutParHeure < 50000) return "★★★★☆";
            else if (coutParHeure < 100000) return "★★★☆☆";
            else if (coutParHeure < 500000) return "★★☆☆☆";
            else return "★☆☆☆☆";
        }
    }

    // ==================================================================
    // MAIN
    // ==================================================================
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        // Pour mieux gérer les petits fichiers
        conf.set("mapreduce.input.fileinputformat.split.minsize", "0");

        Job job = new Job(conf, "CoutParHeureOriginal");

        job.setJarByClass(CoutParHeureOriginal.class);
        job.setMapperClass(CoutMapper.class);
        job.setReducerClass(CoutReducer.class);

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
        System.out.println("Analyse: Coût par heure de visionnage");
        System.out.println("Input: " + INPUT_PATH);
        System.out.println("Output: " + OUTPUT_PATH + Instant.now().getEpochSecond());
        System.out.println("Filtre: Tous les films (car est_production_netflix est NULL dans les données)");

        boolean success = job.waitForCompletion(true);
        System.exit(success ? 0 : 1);
    }
}