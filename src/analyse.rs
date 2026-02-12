// src/analyse.rs

use anyhow::Result;
use arrow::array::*;
use arrow::datatypes::{DataType, TimeUnit};
use arrow::record_batch::RecordBatch;
use csv::ReaderBuilder;
use rayon::prelude::*;
use std::io::Cursor;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::utils::*;

static ERREURS_ANALYSE: AtomicUsize = AtomicUsize::new(0);
static ERREURS_COLONNES_AFFICHEES: AtomicUsize = AtomicUsize::new(0);
static ERREURS_COLONNES_MASQUEES_SIGNALEES: AtomicUsize = AtomicUsize::new(0);

const LIMITE_AFFICHAGE_ERREURS_COLONNES: usize = 10;

pub fn nombre_erreurs_analyse() -> usize {
    ERREURS_ANALYSE.load(Ordering::Relaxed)
}

pub fn analyser_bloc(
    bloc_lignes: &[String],
    schema: Arc<arrow::datatypes::Schema>,
    delimiteur: u8,
) -> Result<RecordBatch> {
    let contenu = bloc_lignes.join("\n");

    let mut lecteur_csv = ReaderBuilder::new()
        .delimiter(delimiteur)
        .has_headers(false)
        .flexible(true)
        .from_reader(Cursor::new(contenu));

    let nombre_colonnes = schema.fields().len();
    let mut valeurs_colonnes: Vec<Vec<String>> = vec![Vec::new(); nombre_colonnes];

    for (index_ligne, resultat) in lecteur_csv.records().enumerate() {
        let enregistrement = match resultat {
            Ok(r) => r,
            Err(e) => {
                ERREURS_ANALYSE.fetch_add(1, Ordering::Relaxed);

                eprintln!(
                    "{}",
                    erreur(format!(
                        "[ERREUR CSV] ligne_bloc={} erreur_parse={} contenu={}",
                        index_ligne,
                        e,
                        bloc_lignes
                            .get(index_ligne)
                            .map(String::as_str)
                            .unwrap_or("<ligne manquante>")
                    ))
                );
                continue;
            }
        };

        if enregistrement.len() != nombre_colonnes {
            ERREURS_ANALYSE.fetch_add(1, Ordering::Relaxed);

            let deja_affichees =
                ERREURS_COLONNES_AFFICHEES.fetch_add(1, Ordering::Relaxed);

            if deja_affichees < LIMITE_AFFICHAGE_ERREURS_COLONNES {
                eprintln!(
                    "{}",
                    erreur(format!(
                        "[ERREUR COLONNES] ligne_bloc={} attendu={} trouvé={} contenu={}",
                        index_ligne,
                        nombre_colonnes,
                        enregistrement.len(),
                        bloc_lignes
                            .get(index_ligne)
                            .map(String::as_str)
                            .unwrap_or("<ligne manquante>")
                    ))
                );
            } else if deja_affichees == LIMITE_AFFICHAGE_ERREURS_COLONNES {
                if ERREURS_COLONNES_MASQUEES_SIGNALEES
                    .compare_exchange(0, 1, Ordering::Relaxed, Ordering::Relaxed)
                    .is_ok()
                {
                    eprintln!(
                        "{}",
                        avertissement(
                            "Erreurs de colonnes supplémentaires masquées (affichage limité)"
                        )
                    );
                }
            }
        }

        for index in 0..nombre_colonnes {
            valeurs_colonnes[index].push(
                enregistrement
                    .get(index)
                    .unwrap_or("")
                    .to_string(),
            );
        }
    }

    let tableaux: Vec<ArrayRef> = schema
        .fields()
        .par_iter()
        .enumerate()
        .map(|(index, champ)| -> ArrayRef {
            match champ.data_type() {
                DataType::Boolean => {
                    let mut constructeur = BooleanBuilder::new();
                    for valeur in &valeurs_colonnes[index] {
                        if est_null_texte(valeur) {
                            constructeur.append_null();
                        } else if let Some(v) = parse_bool(valeur) {
                            constructeur.append_value(v);
                        } else {
                            constructeur.append_null();
                        }
                    }
                    Arc::new(constructeur.finish())
                }

                DataType::Int64 => {
                    let mut constructeur = Int64Builder::new();
                    for valeur in &valeurs_colonnes[index] {
                        if est_null_texte(valeur) {
                            constructeur.append_null();
                        } else if let Ok(entier) = valeur.trim().parse::<i128>() {
                            if entier >= i64::MIN as i128 && entier <= i64::MAX as i128 {
                                constructeur.append_value(entier as i64);
                            } else {
                                constructeur.append_null();
                            }
                        } else {
                            constructeur.append_null();
                        }
                    }
                    Arc::new(constructeur.finish())
                }

                DataType::UInt64 => {
                    let mut constructeur = UInt64Builder::new();
                    for valeur in &valeurs_colonnes[index] {
                        if est_null_texte(valeur) {
                            constructeur.append_null();
                        } else if let Ok(entier) = valeur.trim().parse::<i128>() {
                            if entier >= 0 && entier <= u64::MAX as i128 {
                                constructeur.append_value(entier as u64);
                            } else {
                                constructeur.append_null();
                            }
                        } else {
                            constructeur.append_null();
                        }
                    }
                    Arc::new(constructeur.finish())
                }

                DataType::Float64 => {
                    let mut constructeur = Float64Builder::new();
                    for valeur in &valeurs_colonnes[index] {
                        if est_null_texte(valeur) {
                            constructeur.append_null();
                        } else if let Ok(flottant) = valeur.trim().parse::<f64>() {
                            if flottant.is_finite() {
                                constructeur.append_value(flottant);
                            } else {
                                constructeur.append_null();
                            }
                        } else {
                            constructeur.append_null();
                        }
                    }
                    Arc::new(constructeur.finish())
                }

                DataType::Date32 => {
                    let mut constructeur = Date32Builder::new();
                    for valeur in &valeurs_colonnes[index] {
                        if est_null_texte(valeur) {
                            constructeur.append_null();
                        } else if let Some(jour) = parse_date_ymd(valeur) {
                            constructeur.append_value(jour);
                        } else {
                            constructeur.append_null();
                        }
                    }
                    Arc::new(constructeur.finish())
                }

                DataType::Timestamp(TimeUnit::Second, _) => {
                    let mut constructeur = TimestampSecondBuilder::new();
                    for valeur in &valeurs_colonnes[index] {
                        if est_null_texte(valeur) {
                            constructeur.append_null();
                        } else if let Some(ms) = parse_timestamp_ms(valeur) {
                            constructeur.append_value(ms / 1_000);
                        } else {
                            constructeur.append_null();
                        }
                    }
                    Arc::new(constructeur.finish())
                }

                DataType::Timestamp(TimeUnit::Millisecond, _) => {
                    let mut constructeur = TimestampMillisecondBuilder::new();
                    for valeur in &valeurs_colonnes[index] {
                        if est_null_texte(valeur) {
                            constructeur.append_null();
                        } else if let Some(ms) = parse_timestamp_ms(valeur) {
                            constructeur.append_value(ms);
                        } else {
                            constructeur.append_null();
                        }
                    }
                    Arc::new(constructeur.finish())
                }

                DataType::Timestamp(TimeUnit::Microsecond, _) => {
                    let mut constructeur = TimestampMicrosecondBuilder::new();
                    for valeur in &valeurs_colonnes[index] {
                        if est_null_texte(valeur) {
                            constructeur.append_null();
                        } else if let Some(ms) = parse_timestamp_ms(valeur) {
                            constructeur.append_value(ms * 1_000);
                        } else {
                            constructeur.append_null();
                        }
                    }
                    Arc::new(constructeur.finish())
                }

                DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                    let mut constructeur = TimestampNanosecondBuilder::new();
                    for valeur in &valeurs_colonnes[index] {
                        if est_null_texte(valeur) {
                            constructeur.append_null();
                        } else if let Some(ms) = parse_timestamp_ms(valeur) {
                            constructeur.append_value(ms * 1_000_000);
                        } else {
                            constructeur.append_null();
                        }
                    }
                    Arc::new(constructeur.finish())
                }

                DataType::Utf8 => {
                    let mut constructeur = StringBuilder::new();
                    for valeur in &valeurs_colonnes[index] {
                        if est_null_texte(valeur) {
                            constructeur.append_null();
                        } else {
                            constructeur.append_value(valeur);
                        }
                    }
                    Arc::new(constructeur.finish())
                }

                DataType::LargeUtf8 => {
                    let mut constructeur = LargeStringBuilder::new();
                    for valeur in &valeurs_colonnes[index] {
                        if est_null_texte(valeur) {
                            constructeur.append_null();
                        } else {
                            constructeur.append_value(valeur);
                        }
                    }
                    Arc::new(constructeur.finish())
                }

                DataType::Binary => {
                    let mut constructeur = BinaryBuilder::new();
                    for valeur in &valeurs_colonnes[index] {
                        if est_null_texte(valeur) {
                            constructeur.append_null();
                        } else {
                            constructeur.append_value(valeur.as_bytes());
                        }
                    }
                    Arc::new(constructeur.finish())
                }

                DataType::LargeBinary => {
                    let mut constructeur = LargeBinaryBuilder::new();
                    for valeur in &valeurs_colonnes[index] {
                        if est_null_texte(valeur) {
                            constructeur.append_null();
                        } else {
                            constructeur.append_value(valeur.as_bytes());
                        }
                    }
                    Arc::new(constructeur.finish())
                }

                _ => {
                    let mut constructeur = LargeStringBuilder::new();
                    for valeur in &valeurs_colonnes[index] {
                        if est_null_texte(valeur) {
                            constructeur.append_null();
                        } else {
                            constructeur.append_value(valeur);
                        }
                    }
                    Arc::new(constructeur.finish())
                }
            }
        })
        .collect();

    Ok(RecordBatch::try_new(schema, tableaux)?)
}
