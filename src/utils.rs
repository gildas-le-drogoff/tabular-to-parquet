// src/utils.rs

use anyhow::Result;
use chrono::{NaiveDate, NaiveDateTime};
use owo_colors::OwoColorize;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

use std::io::{self, IsTerminal};

fn couleurs_actives() -> bool {
    io::stdout().is_terminal() && io::stderr().is_terminal()
}

pub fn erreur(msg: impl std::fmt::Display) -> String {
    if couleurs_actives() {
        format!("{}", msg.red().bold())
    } else {
        msg.to_string()
    }
}

pub fn avertissement(msg: impl std::fmt::Display) -> String {
    if couleurs_actives() {
        format!("{}", msg.yellow())
    } else {
        msg.to_string()
    }
}

pub fn succes(msg: impl std::fmt::Display) -> String {
    if couleurs_actives() {
        format!("{}", msg.green())
    } else {
        msg.to_string()
    }
}

pub fn chemin(p: &Path) -> String {
    if couleurs_actives() {
        format!("{}", p.display().cyan())
    } else {
        p.display().to_string()
    }
}

pub fn est_null_texte(v: &str) -> bool {
    let t = v.trim();
    if t.is_empty() {
        return true;
    }
    matches!(
        t.to_ascii_lowercase().as_str(),
        "null" | "none" | "nan" | "n/a" | "na"
    )
}

pub fn parse_bool(v: &str) -> Option<bool> {
    match v.trim().to_ascii_lowercase().as_str() {
        "true" | "1" | "t" | "y" | "yes" | "on" => Some(true),
        "false" | "0" | "f" | "n" | "no" | "off" => Some(false),
        _ => None,
    }
}

pub fn parse_date_ymd(v: &str) -> Option<i32> {
    let t = v.trim();
    if t.is_empty() {
        return None;
    }
    let date = NaiveDate::parse_from_str(t, "%Y-%m-%d")
        .or_else(|_| NaiveDate::parse_from_str(t, "%d/%m/%Y"))
        .or_else(|_| NaiveDate::parse_from_str(t, "%m/%d/%Y"))
        .ok()?;
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1)?;
    let jours = (date - epoch).num_days();
    i32::try_from(jours).ok()
}

pub fn parse_timestamp_ms(v: &str) -> Option<i64> {
    let t = v.trim();
    if t.is_empty() {
        return None;
    }

    let fmts = [
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%m-%dT%H:%M:%S%.f",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%d/%m/%Y %H:%M:%S",
        "%Y/%m/%d %H:%M:%S",
    ];

    for f in fmts {
        if let Ok(dt) = NaiveDateTime::parse_from_str(t, f) {
            return Some(dt.and_utc().timestamp_millis());
        }
    }

    if let Ok(x) = t.parse::<i128>() {
        if (1_000_000_000..4_000_000_000).contains(&x) {
            return Some((x as i64) * 1000);
        }
        if (1_000_000_000_000..4_000_000_000_000).contains(&x) {
            return Some(x as i64);
        }
        if (1_000_000_000_000_000..4_000_000_000_000_000).contains(&x) {
            return Some((x / 1000) as i64);
        }
        if x >= 1_000_000_000_000_000_000 {
            return Some((x / 1_000_000) as i64);
        }
    }

    None
}

/// Détecter le délimiteur le plus probable (`,`, `;`, `\t`, `|`, `:`, ` `)
pub fn detecter_delimiteur<P: AsRef<Path>>(chemin: P) -> Result<u8> {
    let fichier = File::open(&chemin)?;
    let mut lecteur = BufReader::new(fichier);
    let mut ligne = String::new();
    lecteur.read_line(&mut ligne)?;

    let candidats = [',', ';', '\t', '|', ':', ' '];

    // Compter les occurrences de chaque délimiteur possible
    let (delimiteur, _) = candidats
        .iter()
        .map(|&c| (c, ligne.matches(c).count()))
        .max_by_key(|(_, count)| *count)
        .unwrap_or((',', 0));

    Ok(delimiteur as u8)
}
