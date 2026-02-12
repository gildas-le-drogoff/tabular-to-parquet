# 🦀 Tabular to parquet

## 📘 Description

`tabular_to_parquet` est un outil en ligne de commande écrit en **Rust** permettant de convertir des fichiers
tabulaires (CSV, TSV ou formats similaires) en **Apache Parquet**, avec une **détection automatique du délimiteur** et
une **inférence du schéma Arrow**.

Conçu pour traiter des fichiers volumineux de manière **déterministe**, il produit des fichiers
Parquet directement exploitables par les moteurs analytiques modernes tels que DuckDB, Polars, Spark, Pandas ou PyArrow.

Il infère le schéma sans nécessiter de configuration manuelle.

![Démo du projet](docs/tabular-to-parquet.demo.gif)

## ⚙️ Fonctionnalités

- **Détection automatique du délimiteur** : `,`, `;`, `\t`, `|`, `:`, espace
- **Inférence automatique des types de colonnes** à partir d’un échantillon (max. 10 000 lignes)
- **Support complet des types Arrow** :
    - Booléens
    - Entiers signés et non signés
    - Flottants
    - Dates (`Date32`)
    - Timestamps (`Second`, `Millisecond`, `Microsecond`, `Nanosecond`)
    - Heures (`Time64(Microsecond)`)
    - Texte (`Utf8`, `LargeUtf8`)
    - Binaire (`Binary`, `LargeBinary`)
- **Conversion robuste** :
    - Valeurs invalides converties en `null`
    - Adaptation automatique de la nullabilité si nécessaire
- **Traitement par blocs** (50 000 lignes)
- **Écriture Parquet compressée** avec **ZSTD**
- **Support de l’entrée standard (`stdin`)** via `-`

## 🧩 Dépendances principales

| Crate        | Rôle                                                        |
|--------------|-------------------------------------------------------------|
| `anyhow`     | Gestion unifiée et contextuelle des erreurs                 |
| `csv`        | Lecture et parsing des fichiers tabulaires (CSV, TSV, etc.) |
| `arrow`      | Structures de données colonne et schémas Apache Arrow       |
| `parquet`    | Écriture du format Apache Parquet                           |
| `chrono`     | Parsing et manipulation des dates, heures et timestamps     |
| `rayon`      | Parallélisation CPU (inférence, traitements auxiliaires)    |
| `clap`       | Parsing des arguments de ligne de commande                  |
| `log`        | API de journalisation structurée                            |
| `indicatif`  | Barres de progression et indicateurs de traitement          |
| `owo-colors` | Colorisation de la sortie terminal                          |

> Les versions de `arrow` et `parquet` doivent être identiques.

## 🏗️ Installation

### Prérequis

- Rust stable (édition 2021)
- Cargo

### Compilation

```bash
cargo build --release  # --target x86_64-unknown-linux-musl (pour compatibilité)
```

Le binaire généré se trouve dans :

```text
./target/release/tabular_to_parquet
```

## Utilisation

### Syntaxe

```bash
tabular_to_parquet <fichier | ->
```

### Option disponible

```bash
tabular_to_parquet --inferer-schema-complet fichier.(csv|tsv)
```

* `--inferer-schema-complet`
  Analyse l’ensemble du fichier pour l’inférence du schéma au lieu d’un échantillon.
  Cette option augmente le temps d’analyse.

### Exemples

Conversion d’un fichier CSV/TSV :

```bash
tabular_to_parquet donnees.csv
```

Produit le fichier `donnees.parquet` dans le même répertoire.

Conversion d’un fichier TSV dans un sous-répertoire :

```bash
tabular_to_parquet ./data/mesures.tsv
```

Produit le fichier `./data/mesures.parquet`.

Lecture depuis l’entrée standard :

```bash
cat donnees.csv | tabular_to_parquet -
```

Produit le fichier... `stdin.parquet`

## Inférence du schéma

- Analyse d’un échantillon des **10 000 premières lignes**
- Reconnaissance des booléens, entiers, flottants
- Détection des dates et timestamps (formats multiples, UNIX)
- Bascule vers texte en cas d’ambiguïté

Toutes les colonnes sont traitées comme **nullables**.

## 💾 Performances

Débit typique : ~10⁴ lignes/s (≈ 10–50 µs/ligne).

Le traitement est effectué par blocs de **50 000 lignes**, avec une écriture séquentielle dans le fichier Parquet.

Les performances dépendent fortement :

- du disque (SSD vs HDD)
- de la complexité du schéma
- du taux d’erreurs de parsing

Le programme est conçu pour être **stable et prévisible**.

## 🧪 Vérification du fichier Parquet

### DuckDB

```sql
SELECT *
  FROM read_parquet('jeu_test_types_complet.parquet') LIMIT 5;
DESCRIBE SELECT * FROM 'jeu_test_types_complet.parquet';
```

### Python (PyArrow / Pandas)

```python
import pyarrow.parquet as pq
table = pq.read_table("jeu_test_types_complet.parquet")
print(table.schema)
print(table.to_pandas().head())
```

### Polars

```python
import polars as pl
df = pl.read_parquet("jeu_test_types_complet.parquet")
print(df.head())
```

## ⚠️ Limitations

- Pas de paramètres CLI avancés (`--output`, `--delimiter`, etc.)
- Pas de streaming pur
- Encodage supposé UTF-8
- Formats datetime exotiques non reconnus → texte


