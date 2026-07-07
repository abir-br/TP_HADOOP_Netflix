# TP Hadoop — MapReduce avec Java

Projet de travaux pratiques sur **Apache Hadoop / MapReduce**, réalisé en Java avec **Maven**.
Le projet couvre les fondamentaux de MapReduce (WordCount, tri, top-K, jointure, group by) ainsi qu'une étude de cas complète sur un dataset **Netflix** (visionnages, membres, titres, coûts).

---

## Structure du projet

```
TP_HADOOP-main/
└── hadoop_v1.2/
    ├── pom.xml                        
    ├── out.log                         # Log d'exécution des jobs
    ├── src/main/java/
    │   ├── WordCount.java               # Exercice 1 : comptage de mots basique
    │   ├── TopkWordCount.java           # Extension : conservation des k mots les plus fréquents
    │   ├── TriAvecComparaison.java      # Tri des mots par fréquence décroissante (comparateur inversé)
    │   ├── GroupBy.java                 # Agrégations sur le dataset Superstore (profits, ventes...)
    │   ├── Join.java                    # Jointure MapReduce (clients ⋈ commandes, format TPC-H)
    │   ├── TopTitresParHeuresLocal.java # Projet Netflix : titres les plus regardés
    │   ├── CoutParHeureOriginal.java    # Projet Netflix : coût de visionnage par heure
    │   └── TauxAchevementParGenrePays.java # Projet Netflix : taux d'achèvement par genre et pays
    ├── src/main/resources/
    │   └── logback.xml                  # Configuration du logging
    ├── input-wordCount/
    │   └── constitutionFrancaise.txt    # Corpus pour WordCount / TopK / Tri
    ├── input-groupBy/
    │   └── superstore.csv                # Dataset ventes (Superstore)
    ├── input-join/
    │   ├── customers.tbl                 # Clients (format TPC-H, séparateur `|`)
    │   └── orders.tbl                    # Commandes (format TPC-H, séparateur `|`)
    ├── input-TopK/
    │   └── hadoop.txt                    # Texte d'exemple pour le TopK
    ├── input-PROJET-NETFLIX/
    │   ├── dim_membre.csv                # Dimension membres (pays, âge, abonnement...)
    │   ├── dim_titre.csv                 # Dimension titres (genre, durée, catalogue...)
    │   ├── dim_localisation.csv          # Dimension localisation (pays, région, devise...)
    │   └── fait_visionnage.csv           # Table de faits : visionnages (coût, durée, complétude...)
    ├── output/                           # Résultats des jobs (un dossier horodaté par exécution)
    │   ├── wordCount-*/
    │   ├── groupBy-*/
    │   ├── join-*/
    │   ├── top-titres-*/
    │   ├── cout-par-heure-*/
    │   └── taux-achevement-*/
    └── target/                           # Classes compilées (généré par Maven)
```

---

## Description des exercices

### Exercice 1 — WordCount (`WordCount.java`)
Comptage classique du nombre d'occurrences de chaque mot dans un texte (`constitutionFrancaise.txt`), avec `Mapper`/`Reducer` internes et logging vers `out.log`.

### Exercice 2 — Top-K WordCount (`TopkWordCount.java`)
Variante de WordCount qui ne conserve que les **k mots les plus fréquents** (`k=10` par défaut, paramétrable en argument du job), à l'aide d'une `TreeMap` côté Reducer.

### Exercice 3 — Tri par comparateur (`TriAvecComparaison.java`)
Utilise un `WritableComparator` personnalisé (`InverseComparator` / `TextInverseComparator`) pour trier les résultats de `input-groupBy/` par ordre décroissant lors de la phase de shuffle.

### Exercice 4 — Group By (`GroupBy.java`)
Sur le dataset **Superstore** (`superstore.csv`), plusieurs agrégations sont implémentées (activées une à la fois en commentant/décommentant le job) :

| Mapper/Reducer                  | Calcul                                              |
|----------------------------------|------------------------------------------------------|
| `ProfitByCustomerMapper/Reducer` | Profit total par client                              |
| `SalesByDateStateMapper/Reducer` | Ventes agrégées par date et par état                 |
| `SalesByDateCategoryMapper/Reducer` | Ventes agrégées par date et par catégorie de produit |
| `OrderStatsMapper/Reducer`       | Statistiques par commande (actif par défaut dans `main`) |

### Exercice 5 — Jointure (`Join.java`)
Jointure MapReduce de type **reduce-side join** entre `customers.tbl` et `orders.tbl` (format TPC-H, séparateur `|`), les enregistrements étant tagués (`C` pour client, `O` pour commande) puis rapprochés par `CustomerID` dans le Reducer.

---

## Projet fil rouge — Analyse Netflix (`input-PROJET-NETFLIX/`)

Trois jobs MapReduce indépendants exploitent le modèle en étoile Netflix (`fait_visionnage` + dimensions `membre`/`titre`/`localisation`) :

| Job                                  | Objectif                                                                 |
|----------------------------------------|-----------------------------------------------------------------------------|
| `TopTitresParHeuresLocal.java`         | Classement des titres les plus regardés (jointure fait ⋈ dim_titre)         |
| `CoutParHeureOriginal.java`            | Coût total de visionnage par heure de la journée                            |
| `TauxAchevementParGenrePays.java`      | Taux d'achèvement des visionnages, croisé par genre de contenu et pays du membre |

Ces jobs utilisent un **map-side/reduce-side join multi-sources** : chaque Mapper tague ses enregistrements (`V` = visionnage, `T` = titre, `M` = membre) avant de les envoyer au Reducer, qui reconstitue les lignes complètes par clé commune (`id_titre` ou `id_membre`).

---

## Installation

Prérequis : **JDK 11**, **Maven**, **Hadoop 1.2.1** (dépendance `hadoop-core`, exécution en mode local sans cluster).

```bash
cd hadoop_v1.2

# Compiler le projet et récupérer les dépendances
mvn clean package
```

## Exécution

Chaque classe possède sa propre méthode `main` et peut être lancée directement (mode local, sans cluster Hadoop) :

```bash
# WordCount
java -cp target/classes:$(mvn dependency:build-classpath -Dmdep.outputFile=/dev/stdout -q) WordCount

# Top-K WordCount (k=5 par exemple)
java -cp target/classes:... TopkWordCount 5

# Tri avec comparateur inversé
java -cp target/classes:... TriAvecComparaison

# Group By (Superstore)
java -cp target/classes:... GroupBy

# Jointure clients/commandes
java -cp target/classes:... Join

# Projet Netflix
java -cp target/classes:... TopTitresParHeuresLocal
java -cp target/classes:... CoutParHeureOriginal
java -cp target/classes:... TauxAchevementParGenrePays
```

