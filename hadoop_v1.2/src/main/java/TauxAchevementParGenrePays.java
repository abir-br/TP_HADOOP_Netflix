import java.io.IOException;
import java.text.DecimalFormat;
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

public class TauxAchevementParGenrePays {
    private static final String INPUT_PATH = "TP4_Hadoop-MapReduce/hadoop_v1.2/input-PROJET-NETFLIX/";
    private static final String OUTPUT_PATH = "TP4_Hadoop-MapReduce/hadoop_v1.2/output/taux-achevement-";
    private static final String SEPARATOR = "#";
    private static final String VISIONNAGE_TAG = "V";
    private static final String TITRE_TAG = "T";
    private static final String MEMBRE_TAG = "M";

    // ==================================================================
   //  ERRURS D'AFFICHAGE A CORRIGER
    // ==================================================================
    public static class TauxAchevementMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text outputKey = new Text();
        private Text outputValue = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();

            if (line.startsWith("id_")) {
                return;
            }

            String[] fields = line.split(",");

            // fait_visionnage.csv - 10 colonnes
            if (fields.length == 10) {
                try {
                    String idTitre = fields[2].trim();           // colonne 2
                    String idMembre = fields[1].trim();          // colonne 1
                    String estComplet = fields[8].trim();        // colonne 8

                    String cleComposite = idTitre + SEPARATOR + idMembre;

                    outputKey.set(cleComposite);
                    outputValue.set(VISIONNAGE_TAG + SEPARATOR + estComplet);
                    context.write(outputKey, outputValue);

                } catch (Exception e) {
                    // ignorer
                }
            }

            // dim_titre.csv - 11 colonnes
            else if (fields.length == 11) {
                try {
                    String idTitre = fields[0].trim();        // colonne 0
                    String genrePrincipal = fields[3].trim(); // colonne 3

                    if (!"NULL".equalsIgnoreCase(genrePrincipal) && !genrePrincipal.isEmpty()) {
                        outputKey.set(idTitre);
                        outputValue.set(TITRE_TAG + SEPARATOR + genrePrincipal);
                        context.write(outputKey, outputValue);
                    }

                } catch (Exception e) {
                    // ignorer
                }
            }

            // dim_membre.csv - CORRECTION IMPORTANTE : 10 colonnes, pas 6 !
            // Structure: id_membre,pays_principal,age,tranche_age,genre,date_inscription,anciennete_mois,type_abonnement_actuel,appareil_principal,statut_abonnement
            else if (fields.length == 10) {
                try {
                    String idMembre = fields[0].trim();      // colonne 0 = id_membre
                    String paysMembre = fields[1].trim();    // colonne 1 = pays_principal

                    // Nettoyer le pays
                    if (!"NULL".equalsIgnoreCase(paysMembre) && !paysMembre.isEmpty()) {
                        outputKey.set(idMembre);
                        outputValue.set(MEMBRE_TAG + SEPARATOR + paysMembre);
                        context.write(outputKey, outputValue);

                        // DEBUG
                        System.out.println("DEBUG MAP - Membre: id=" + idMembre + ", pays=" + paysMembre);
                    }

                } catch (Exception e) {
                    System.err.println("ERREUR membre: " + e.getMessage() + " - ligne: " + line);
                }
            }
        }
    }

    // ==================================================================
    // REDUCER
    // ==================================================================
    public static class TauxAchevementReducer extends Reducer<Text, Text, Text, Text> {
        private Text outputKey = new Text();
        private Text outputValue = new Text();

        private java.util.HashMap genres = new java.util.HashMap();    // id_titre -> genre
        private java.util.HashMap pays = new java.util.HashMap();      // id_membre -> pays
        private java.util.ArrayList visionnages = new java.util.ArrayList();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String cle = key.toString();

            if (cle.indexOf(SEPARATOR) != -1) {
                // Clé composite : id_titre#id_membre
                String[] parts = cle.split(SEPARATOR);
                if (parts.length == 2) {
                    String idTitre = parts[0];
                    String idMembre = parts[1];

                    java.util.Iterator<Text> iter = values.iterator();
                    while (iter.hasNext()) {
                        Text val = iter.next();
                        String value = val.toString();
                        String[] valParts = value.split(SEPARATOR, 2);

                        if (valParts.length == 2 && valParts[0].equals(VISIONNAGE_TAG)) {
                            String estComplet = valParts[1];
                            // Stocker comme tableau d'objets
                            Object[] visionnage = new Object[3];
                            visionnage[0] = idTitre;
                            visionnage[1] = idMembre;
                            visionnage[2] = estComplet;
                            visionnages.add(visionnage);
                            break;
                        }
                    }
                }
            } else {
                // Clé simple
                java.util.Iterator<Text> iter = values.iterator();
                while (iter.hasNext()) {
                    Text val = iter.next();
                    String value = val.toString();
                    String[] parts = value.split(SEPARATOR, 2);

                    if (parts.length == 2) {
                        if (parts[0].equals(TITRE_TAG)) {
                            genres.put(cle, parts[1]);
                            System.out.println("DEBUG REDUCE - Genre: cle=" + cle + ", genre=" + parts[1]);
                        } else if (parts[0].equals(MEMBRE_TAG)) {
                            pays.put(cle, parts[1]);
                            System.out.println("DEBUG REDUCE - Pays: cle=" + cle + ", pays=" + parts[1]);
                        }
                    }
                }
            }
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            // DEBUG: Afficher ce qu'on a collecté
            System.out.println("DEBUG CLEANUP - Genres: " + genres.size());
            System.out.println("DEBUG CLEANUP - Pays: " + pays.size());
            System.out.println("DEBUG CLEANUP - Visionnages: " + visionnages.size());

            // 1. Compter les visionnages par (genre, pays)
            java.util.HashMap stats = new java.util.HashMap();  // cleGenrePays -> int[2] {complets, total}

            java.util.Iterator visionIter = visionnages.iterator();
            int compteurAssocies = 0;
            while (visionIter.hasNext()) {
                Object[] v = (Object[]) visionIter.next();
                String idTitre = (String) v[0];
                String idMembre = (String) v[1];
                String estComplet = (String) v[2];

                String genre = (String) genres.get(idTitre);
                String paysMembre = (String) pays.get(idMembre);

                if (genre != null && paysMembre != null) {
                    compteurAssocies++;
                    String cleGenrePays = genre + SEPARATOR + paysMembre;

                    int[] compteurs = (int[]) stats.get(cleGenrePays);
                    if (compteurs == null) {
                        compteurs = new int[2]; // [complets, total]
                        stats.put(cleGenrePays, compteurs);
                    }

                    compteurs[1]++; // total
                    if ("1".equals(estComplet)) {
                        compteurs[0]++; // complets
                    }
                }
            }

            System.out.println("DEBUG CLEANUP - Visionnages associés: " + compteurAssocies);

            // 2. Écrire l'en-tête
            context.write(new Text("=== TAUX D'ACHÈVEMENT PAR GENRE ET PAYS ==="), new Text(""));
            context.write(new Text("Genre#Pays"), new Text("Taux\t(Complets/Total)"));
            context.write(new Text("----------"), new Text("----\t---------------"));

            // 3. Trier et écrire les résultats
            java.util.List entrees = new java.util.ArrayList(stats.entrySet());

            // Trier par taux décroissant
            java.util.Collections.sort(entrees, new java.util.Comparator() {
                public int compare(Object o1, Object o2) {
                    java.util.Map.Entry e1 = (java.util.Map.Entry) o1;
                    java.util.Map.Entry e2 = (java.util.Map.Entry) o2;

                    int[] c1 = (int[]) e1.getValue();
                    int[] c2 = (int[]) e2.getValue();

                    double taux1 = c1[1] > 0 ? (double) c1[0] / c1[1] : 0;
                    double taux2 = c2[1] > 0 ? (double) c2[0] / c2[1] : 0;

                    if (taux1 > taux2) return -1;
                    if (taux1 < taux2) return 1;
                    return 0;
                }
            });

            // Écrire chaque ligne
            java.util.Iterator entreesIter = entrees.iterator();
            while (entreesIter.hasNext()) {
                java.util.Map.Entry entry = (java.util.Map.Entry) entreesIter.next();
                String cleGenrePays = (String) entry.getKey();
                int[] compteurs = (int[]) entry.getValue();

                double taux = 0;
                if (compteurs[1] > 0) {
                    taux = (double) compteurs[0] / compteurs[1] * 100;
                }

                DecimalFormat df = new DecimalFormat("0.0");
                String valeur = df.format(taux) + "%\t(" + compteurs[0] + "/" + compteurs[1] + ")";

                context.write(new Text(cleGenrePays), new Text(valeur));
            }

            // 4. Résumé
            context.write(new Text(""), new Text(""));
            context.write(new Text("=== SYNTHÈSE ==="), new Text(""));
            context.write(new Text("Genres analysés:"), new Text(String.valueOf(genres.size())));
            context.write(new Text("Pays analysés:"), new Text(String.valueOf(pays.size())));
            context.write(new Text("Visionnages traités:"), new Text(String.valueOf(visionnages.size())));
            context.write(new Text("Visionnages associés (genre+pays):"), new Text(String.valueOf(compteurAssocies)));
            context.write(new Text("Combinaisons genre/pays:"), new Text(String.valueOf(stats.size())));
        }
    }

    // ==================================================================
    // MAIN
    // ==================================================================
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapreduce.input.fileinputformat.split.minsize", "0");

        Job job = new Job(conf, "TauxAchevementParGenrePays");

        job.setJarByClass(TauxAchevementParGenrePays.class);
        job.setMapperClass(TauxAchevementMapper.class);
        job.setReducerClass(TauxAchevementReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH + System.currentTimeMillis()));

        job.setNumReduceTasks(1);

        System.out.println("Démarrage du job MapReduce - Taux d'achèvement par genre et pays");
        System.out.println("DEBUG: dim_membre.csv a 10 colonnes, pays_principal est la colonne 1");

        boolean success = job.waitForCompletion(true);
        System.exit(success ? 0 : 1);
    }
}