// src/schema.rs

use crate::utils::*;
use anyhow::Result;
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime};
use csv::ReaderBuilder;
use std::fs::File;
use std::path::Path;

const MAX_LIGNES_INFERENCE: u64 = 1000;

fn est_date_texte(valeur: &str) -> bool {
    let texte = valeur.trim();
    NaiveDate::parse_from_str(texte, "%Y-%m-%d").is_ok()
        || NaiveDate::parse_from_str(texte, "%d/%m/%Y").is_ok()
        || NaiveDate::parse_from_str(texte, "%m/%d/%Y").is_ok()
}

fn precision_fractionnelle_datetime(texte: &str) -> Option<usize> {
    let valeur = texte.trim();
    let position_point = valeur.find('.')?;
    let apres_point = &valeur[position_point + 1..];
    let mut compteur = 0usize;
    for caractere in apres_point.chars() {
        if caractere.is_ascii_digit() {
            compteur += 1;
        } else {
            break;
        }
    }
    if compteur == 0 {
        None
    } else {
        Some(compteur)
    }
}

fn unite_timestamp_depuis_precision(precision: usize) -> TimeUnit {
    if precision >= 9 {
        TimeUnit::Nanosecond
    } else if precision >= 6 {
        TimeUnit::Microsecond
    } else if precision >= 3 {
        TimeUnit::Millisecond
    } else {
        TimeUnit::Second
    }
}

fn detecter_unite_datetime_texte(valeur: &str) -> Option<TimeUnit> {
    let texte = valeur.trim();
    if DateTime::parse_from_rfc3339(texte).is_ok() {
        let precision = precision_fractionnelle_datetime(texte).unwrap_or(3);
        return Some(unite_timestamp_depuis_precision(precision));
    }

    let formats_timezone = [
        "%Y-%m-%d %H:%M:%S%:z",
        "%Y-%m-%d %H:%M:%S%.f%:z",
        "%Y-%m-%dT%H:%M:%S%:z",
        "%Y-%m-%dT%H:%M:%S%.f%:z",
        "%Y-%m-%d %H:%M:%S%z",
        "%Y-%m-%d %H:%M:%S%.f%z",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S%.f%z",
    ];

    for format in formats_timezone.iter() {
        if DateTime::<FixedOffset>::parse_from_str(texte, format).is_ok() {
            let precision = precision_fractionnelle_datetime(texte).unwrap_or(3);
            return Some(unite_timestamp_depuis_precision(precision));
        }
    }

    let formats_naifs = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M:%S%.3f",
        "%Y-%m-%d %H:%M:%S%.6f",
        "%Y-%m-%d %H:%M:%S%.9f",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S%.3f",
        "%Y-%m-%dT%H:%M:%S%.6f",
        "%Y-%m-%dT%H:%M:%S%.9f",
        "%d/%m/%Y %H:%M:%S",
        "%Y/%m/%d %H:%M:%S",
    ];

    for format in formats_naifs.iter() {
        if NaiveDateTime::parse_from_str(texte, format).is_ok() {
            let precision = precision_fractionnelle_datetime(texte).unwrap_or(3);
            return Some(unite_timestamp_depuis_precision(precision));
        }
    }

    None
}

fn detecter_unite_epoch(valeur: &str) -> Option<TimeUnit> {
    let texte = valeur.trim();
    if texte.is_empty() {
        return None;
    }
    if texte
        .bytes()
        .any(|b| !(b.is_ascii_digit() || b == b'+' || b == b'-'))
    {
        return None;
    }

    let entier = texte.parse::<i128>().ok()?;
    let valeur_absolue = if entier < 0 { -entier } else { entier };

    if valeur_absolue < 100_000_000_000i128 {
        Some(TimeUnit::Second)
    } else if valeur_absolue < 100_000_000_000_000i128 {
        Some(TimeUnit::Millisecond)
    } else if valeur_absolue < 100_000_000_000_000_000i128 {
        Some(TimeUnit::Microsecond)
    } else {
        Some(TimeUnit::Nanosecond)
    }
}

fn detecter_unite_timestamp(valeur: &str) -> Option<TimeUnit> {
    detecter_unite_datetime_texte(valeur).or_else(|| detecter_unite_epoch(valeur))
}

#[derive(Clone)]
struct StatistiquesColonne {
    valeurs_non_nulles: u64,
    nb_booleen_ok: u64,
    nb_date_ok: u64,
    nb_ts_s_ok: u64,
    nb_ts_ms_ok: u64,
    nb_ts_us_ok: u64,
    nb_ts_ns_ok: u64,
    nb_f64_ok: u64,
    nb_i128_ok: u64,
    nb_negatifs: u64,
    syntaxe_flottante_vue: bool,
    min_i128: i128,
    max_i128: i128,
    longueur_max: usize,
}

impl StatistiquesColonne {
    fn nouvelle() -> Self {
        Self {
            valeurs_non_nulles: 0,
            nb_booleen_ok: 0,
            nb_date_ok: 0,
            nb_ts_s_ok: 0,
            nb_ts_ms_ok: 0,
            nb_ts_us_ok: 0,
            nb_ts_ns_ok: 0,
            nb_f64_ok: 0,
            nb_i128_ok: 0,
            nb_negatifs: 0,
            syntaxe_flottante_vue: false,
            min_i128: i128::MAX,
            max_i128: i128::MIN,
            longueur_max: 0,
        }
    }

    fn observer_valeur(&mut self, valeur: &str) {
        if est_null_texte(valeur) {
            return;
        }

        self.valeurs_non_nulles += 1;
        let texte = valeur.trim();
        self.longueur_max = self.longueur_max.max(texte.len());

        if parse_bool(texte).is_some() {
            self.nb_booleen_ok += 1;
        }

        if est_date_texte(texte) {
            self.nb_date_ok += 1;
        }

        if texte.len() >= 8 && texte.bytes().any(|b| b == b'-' || b == b':' || b == b'T') {
            if let Some(unite) = detecter_unite_timestamp(texte) {
                match unite {
                    TimeUnit::Second => self.nb_ts_s_ok += 1,
                    TimeUnit::Millisecond => self.nb_ts_ms_ok += 1,
                    TimeUnit::Microsecond => self.nb_ts_us_ok += 1,
                    TimeUnit::Nanosecond => self.nb_ts_ns_ok += 1,
                }
            }
        }

        if texte.parse::<f64>().is_ok() {
            self.nb_f64_ok += 1;
            if texte.contains('.') || texte.contains('e') || texte.contains('E') {
                self.syntaxe_flottante_vue = true;
            }
        }

        if let Ok(entier) = texte.parse::<i128>() {
            self.nb_i128_ok += 1;
            if entier < 0 {
                self.nb_negatifs += 1;
            }
            self.min_i128 = self.min_i128.min(entier);
            self.max_i128 = self.max_i128.max(entier);
        }
    }

    fn ratio(compteur: u64, total: u64) -> f64 {
        if total == 0 {
            0.0
        } else {
            compteur as f64 / total as f64
        }
    }

    fn choisir_unite_timestamp(&self) -> TimeUnit {
        let mut meilleur = (TimeUnit::Millisecond, self.nb_ts_ms_ok);
        let candidats = [
            (TimeUnit::Second, self.nb_ts_s_ok),
            (TimeUnit::Millisecond, self.nb_ts_ms_ok),
            (TimeUnit::Microsecond, self.nb_ts_us_ok),
            (TimeUnit::Nanosecond, self.nb_ts_ns_ok),
        ];
        for (unite, compte) in candidats {
            if compte > meilleur.1 {
                meilleur = (unite, compte);
            }
        }
        meilleur.0
    }

    fn choisir_type_large(&self) -> DataType {
        if self.valeurs_non_nulles == 0 {
            return DataType::LargeUtf8;
        }

        let nb_ts_total = self.nb_ts_s_ok + self.nb_ts_ms_ok + self.nb_ts_us_ok + self.nb_ts_ns_ok;

        let ratio_bool = Self::ratio(self.nb_booleen_ok, self.valeurs_non_nulles);
        let ratio_date = Self::ratio(self.nb_date_ok, self.valeurs_non_nulles);
        let ratio_ts = Self::ratio(nb_ts_total, self.valeurs_non_nulles);
        let ratio_f64 = Self::ratio(self.nb_f64_ok, self.valeurs_non_nulles);

        if ratio_ts >= 0.995 {
            return DataType::Timestamp(self.choisir_unite_timestamp(), None);
        }

        if ratio_date >= 0.995 {
            return DataType::Date32;
        }

        if ratio_bool >= 0.995 {
            return DataType::Boolean;
        }

        if ratio_f64 >= 0.98 {
            if self.syntaxe_flottante_vue {
                return DataType::Float64;
            }

            let ratio_i128 = Self::ratio(self.nb_i128_ok, self.valeurs_non_nulles);
            let ratio_negatifs = Self::ratio(self.nb_negatifs, self.nb_i128_ok.max(1));

            if ratio_i128 >= 0.98 {
                let min_val = self.min_i128;
                let max_val = self.max_i128;

                if min_val < i64::MIN as i128 || max_val > u64::MAX as i128 {
                    return DataType::Float64;
                }

                if min_val >= 0 && max_val <= u64::MAX as i128 {
                    if max_val > i64::MAX as i128 {
                        return DataType::UInt64;
                    }
                    if ratio_negatifs < 0.005 {
                        return DataType::UInt64;
                    }
                }

                if min_val >= i64::MIN as i128 && max_val <= i64::MAX as i128 {
                    return DataType::Int64;
                }

                return DataType::Float64;
            }

            return DataType::Float64;
        }

        DataType::LargeUtf8
    }
}

pub fn inferer_schema<P: AsRef<Path>>(
    chemin_fichier: P,
    delimiteur: u8,
    analyser_tout: bool,
) -> Result<Schema> {
    let mut lecteur_csv: csv::Reader<File> = ReaderBuilder::new()
        .delimiter(delimiteur)
        .flexible(true)
        .has_headers(true)
        .from_path(&chemin_fichier)?;

    let en_tetes = lecteur_csv.headers()?.clone();
    let nombre_colonnes = en_tetes.len();
    let mut statistiques = vec![StatistiquesColonne::nouvelle(); nombre_colonnes];

    let mut lignes_lues: u64 = 0;

    for resultat in lecteur_csv.records() {
        let ligne = match resultat {
            Ok(l) => l,
            Err(_) => continue,
        };

        lignes_lues += 1;

        for index in 0..nombre_colonnes {
            let valeur = ligne.get(index).unwrap_or("");
            statistiques[index].observer_valeur(valeur);
        }

        if !analyser_tout && lignes_lues >= MAX_LIGNES_INFERENCE {
            break;
        }

        if lignes_lues % 100_000 == 0 {
            eprintln!("[INFO] inférence large: {} lignes analysées", lignes_lues);
        }
    }

    let champs: Vec<Field> = en_tetes
        .iter()
        .enumerate()
        .map(|(index, nom)| Field::new(nom, statistiques[index].choisir_type_large(), true))
        .collect();

    Ok(Schema::new(champs))
}
