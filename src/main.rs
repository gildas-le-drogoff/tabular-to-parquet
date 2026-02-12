// src/main.rs

use anyhow::{Context, Result};
use clap::{CommandFactory, Parser};
use log::{info, warn};
use std::io::{self, IsTerminal, Read, Write};
use std::path::{Path, PathBuf};
use tempfile::NamedTempFile;

mod analyse;
mod conversion;
mod schema;
mod utils;

use conversion::convertir_csv_en_parquet;
use utils::{avertissement, chemin, erreur, succes};

#[derive(Parser, Debug)]
#[command(
    name = "tabular_to_parquet",
    version,
    about = "Convertit un fichier tabulaire (CSV/TSV) en Parquet"
)]
struct InterfaceCommande {
    #[arg(long)]
    inferer_schema_complet: bool,

    #[arg(value_name = "ENTREE")]
    entree: Option<String>,
}

fn main() {
    if let Err(erreur_execution) = executer() {
        eprintln!("{} {}", erreur("Erreur :"), erreur_execution);
        std::process::exit(1);
    }
}

fn executer() -> Result<()> {
    initialiser_journalisation();

    let interface = InterfaceCommande::parse();

    let (chemin_entree, chemin_sortie) = match interface.entree.as_deref() {
        Some("-") => {
            if io::stdin().is_terminal() {
                afficher_aide();
                anyhow::bail!("Stdin demandé ('-') mais aucun flux n’est redirigé");
            }
            let chemin_temporaire = ecrire_stdin_dans_fichier_temporaire()?;
            (chemin_temporaire, PathBuf::from("stdin.parquet"))
        }
        Some(fichier) => {
            let sortie = construire_chemin_sortie_parquet(fichier);
            (PathBuf::from(fichier), sortie)
        }
        None => {
            afficher_aide();
            anyhow::bail!("Aucune entrée fournie");
        }
    };

    convertir_csv_en_parquet(
        chemin_entree.clone(),
        chemin_sortie.clone(),
        interface.inferer_schema_complet,
    )
    .with_context(|| {
        format!(
            "Échec de la conversion {} → {}",
            chemin(&chemin_entree),
            chemin(&chemin_sortie)
        )
    })?;

    eprintln!(
        "{} {}",
        succes("Conversion terminée :"),
        chemin(&chemin_sortie)
    );

    Ok(())
}

fn afficher_aide() {
    let mut commande = InterfaceCommande::command();
    let _ = commande.print_help();
    eprintln!();
}

fn initialiser_journalisation() {
    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", "info");
    }

    env_logger::builder()
        .format_timestamp_secs()
        .format_level(true)
        .init();
}

fn ecrire_stdin_dans_fichier_temporaire() -> Result<PathBuf> {
    let mut tampon = Vec::new();
    io::stdin().read_to_end(&mut tampon)?;

    if tampon.is_empty() {
        warn!("Stdin vide");
        afficher_aide();
        eprintln!("{}", avertissement("Entrée standard vide"));
        anyhow::bail!("Stdin vide");
    }

    let mut fichier_temporaire = NamedTempFile::new()?;
    fichier_temporaire.write_all(&tampon)?;

    let (_fichier, chemin_fichier) = fichier_temporaire.keep()?;
    info!("Stdin écrit dans {:?}", chemin_fichier);

    Ok(chemin_fichier)
}

fn construire_chemin_sortie_parquet(entree: &str) -> PathBuf {
    let chemin_entree = Path::new(entree);

    let mut repertoire_sortie = chemin_entree
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .to_path_buf();

    let nom_base = chemin_entree
        .file_stem()
        .unwrap_or_default()
        .to_string_lossy();

    repertoire_sortie.push(format!("{nom_base}.parquet"));
    repertoire_sortie
}
