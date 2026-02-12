// src/conversion.rs

use anyhow::Result;
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use std::collections::{BTreeMap, VecDeque};
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use crossbeam::channel::{bounded, Receiver, Sender};
use indicatif::{ProgressBar, ProgressStyle};
use owo_colors::OwoColorize;
use rayon::prelude::*;

use crate::analyse::{analyser_bloc, nombre_erreurs_analyse};
use crate::schema::inferer_schema;
use crate::utils::detecter_delimiteur;

const CAPACITE_FILE_BLOCS: usize = 8;
const CAPACITE_FILE_BATCHES: usize = 8;
const FENETRE_CALCUL_DEBIT: Duration = Duration::from_secs(2);

pub fn convertir_csv_en_parquet<P: AsRef<Path>, Q: AsRef<Path>>(
    chemin_entree: P,
    chemin_sortie: Q,
    inferer_schema_complet: bool,
) -> Result<()> {
    let instant_depart = Instant::now();

    eprintln!("{} Ouverture fichier d’entrée", "[INFO]".blue().bold());
    let delimiteur = detecter_delimiteur(&chemin_entree)?;

    eprintln!("{} Inférence du schéma", "[PHASE]".cyan().bold());
    let schema = Arc::new(schema_rendre_tout_nullable(inferer_schema(
        &chemin_entree,
        delimiteur,
        inferer_schema_complet,
    )?));

    let nombre_colonnes = schema.fields().len();
    eprintln!(
        "{} Schéma détecté : {} colonnes",
        "[OK]".green().bold(),
        nombre_colonnes
    );

    let taille_bloc = if nombre_colonnes <= 20 {
        250_000
    } else if nombre_colonnes <= 50 {
        150_000
    } else {
        5_000
    };

    eprintln!(
        "{} Paramètres : bloc = {} lignes",
        "[CONF]".purple().bold(),
        taille_bloc
    );

    let total_lignes = compter_lignes(&chemin_entree)?;
    let barre = ProgressBar::new(total_lignes as u64);
    barre.set_style(
        ProgressStyle::with_template(
            "{elapsed_precise} [{bar:40.cyan/blue}] {human_pos}/{human_len} lignes {msg} ETA {eta}",
        )?
        .progress_chars("█░"),
    );

    let (tx_blocs, rx_blocs) = bounded::<(usize, Vec<String>)>(CAPACITE_FILE_BLOCS);
    let (tx_batches, rx_batches) =
        bounded::<(usize, arrow::record_batch::RecordBatch)>(CAPACITE_FILE_BATCHES);

    lancer_workers_analyse(rx_blocs, tx_batches.clone(), schema.clone(), delimiteur);

    let handle_writer = lancer_ecrivain_parquet(
        rx_batches,
        chemin_sortie,
        schema.clone(),
        taille_bloc,
        barre.clone(),
    )?;

    let handle_ticker = lancer_ticker(barre.clone());

    produire_blocs(chemin_entree, taille_bloc, tx_blocs)?;
    drop(tx_batches);

    handle_writer.join().unwrap()?;
    barre.finish_with_message("Écriture terminée");
    handle_ticker.join().ok();

    let duree = instant_depart.elapsed();
    let debit_lignes = total_lignes as f64 / duree.as_secs_f64();

    let erreurs = nombre_erreurs_analyse();

    if erreurs > 0 {
        eprintln!(
            "{} Terminé avec erreurs : {} lignes problématiques détectées",
            "[ATTENTION]".yellow().bold(),
            erreurs
        );
    }

    println!(
        "{} Terminé en {:.2?} (~{:.2} µs/ligne, ~{:.0} l/s)",
        "[SUCCESS]".green().bold(),
        duree,
        duree.as_micros() as f64 / total_lignes.max(1) as f64,
        debit_lignes
    );

    Ok(())
}

fn lancer_ticker(barre: ProgressBar) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let mut historique: VecDeque<(Instant, u64)> = VecDeque::new();

        loop {
            thread::sleep(Duration::from_millis(200));

            let maintenant = Instant::now();
            let position = barre.position();
            historique.push_back((maintenant, position));

            while let Some((instant, _)) = historique.front() {
                if maintenant.duration_since(*instant) > FENETRE_CALCUL_DEBIT {
                    historique.pop_front();
                } else {
                    break;
                }
            }

            if historique.len() >= 2 {
                let (t_debut, l_debut) = historique.front().unwrap();
                let (t_fin, l_fin) = historique.back().unwrap();
                let delta_temps = t_fin.duration_since(*t_debut).as_secs_f64();

                if delta_temps > 0.0 && l_fin > l_debut {
                    let debit = ((l_fin - l_debut) as f64 / delta_temps).round().max(1.0) as u64;
                    barre.set_message(format!("{debit} l/s"));
                }
            }

            if barre.is_finished() {
                break;
            }
        }
    })
}

fn lancer_ecrivain_parquet<Q: AsRef<Path>>(
    rx_batches: Receiver<(usize, arrow::record_batch::RecordBatch)>,
    chemin_sortie: Q,
    schema: Arc<arrow::datatypes::Schema>,
    taille_bloc: usize,
    barre: ProgressBar,
) -> Result<thread::JoinHandle<Result<()>>> {
    let fichier_sortie = File::create(chemin_sortie)?;
    let proprietes = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(5)?))
        .set_max_row_group_size(taille_bloc)
        .build();

    let mut ecrivain = ArrowWriter::try_new(fichier_sortie, schema, Some(proprietes))?;

    Ok(thread::spawn(move || {
        barre.set_message("Écriture parquet");

        let mut attente: BTreeMap<usize, arrow::record_batch::RecordBatch> = BTreeMap::new();
        let mut index_attendu = 0usize;

        for (index, batch) in rx_batches {
            attente.insert(index, batch);

            while let Some(batch) = attente.remove(&index_attendu) {
                let lignes = batch.num_rows() as u64;
                ecrivain.write(&batch)?;
                barre.inc(lignes);
                index_attendu += 1;
            }
        }

        barre.set_message("Finalisation");
        ecrivain.close()?;
        Ok(())
    }))
}

fn produire_blocs<P: AsRef<Path>>(
    chemin_entree: P,
    taille_bloc: usize,
    tx_blocs: Sender<(usize, Vec<String>)>,
) -> Result<()> {
    let fichier = File::open(chemin_entree)?;
    let lecteur = BufReader::new(fichier);

    let mut bloc: Vec<String> = Vec::with_capacity(taille_bloc);
    let mut index_bloc = 0usize;

    for ligne in lecteur.lines().skip(1).filter_map(Result::ok) {
        bloc.push(ligne);

        if bloc.len() >= taille_bloc {
            tx_blocs.send((index_bloc, bloc))?;
            bloc = Vec::with_capacity(taille_bloc);
            index_bloc += 1;
        }
    }

    if !bloc.is_empty() {
        tx_blocs.send((index_bloc, bloc))?;
    }

    Ok(())
}

fn lancer_workers_analyse(
    rx_blocs: Receiver<(usize, Vec<String>)>,
    tx_batches: Sender<(usize, arrow::record_batch::RecordBatch)>,
    schema: Arc<arrow::datatypes::Schema>,
    delimiteur: u8,
) {
    thread::spawn(move || {
        rx_blocs
            .into_iter()
            .par_bridge()
            .for_each(|(index, lignes)| {
                let batch =
                    analyser_bloc(&lignes, schema.clone(), delimiteur).expect("Analyse bloc");
                tx_batches.send((index, batch)).expect("Envoi batch");
            });
    });
}

fn compter_lignes<P: AsRef<Path>>(chemin: P) -> Result<usize> {
    let fichier = File::open(chemin)?;
    let lecteur = BufReader::new(fichier);
    Ok(lecteur.lines().count().saturating_sub(1))
}

fn schema_rendre_tout_nullable(schema: arrow::datatypes::Schema) -> arrow::datatypes::Schema {
    let champs: Vec<arrow::datatypes::Field> = schema
        .fields()
        .iter()
        .map(|champ| arrow::datatypes::Field::new(champ.name(), champ.data_type().clone(), true))
        .collect();

    arrow::datatypes::Schema::new(champs)
}
