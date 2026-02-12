#!/usr/bin/env python3
import csv
import random
import string
from datetime import date, timedelta
from multiprocessing import Pool, cpu_count
import sys

# ─────────────────────────────────────────────
# ⚙  Paramètres
# ─────────────────────────────────────────────
NB_LIGNES = 100000
FICHIER_SORTIE = "jeu_test_types_complet.tsv"
MODE_PARFAIT = True
PROBA_NULL_PCT = 0.000005
PROBA_BRUIT_PCT = 0.0000003
POURCENT_LIGNES_CORROMPUES = 0.004
SEED = 42
DESACTIVER_NULLS = False
NB_PROCESSUS = cpu_count()
TAILLE_CHUNK = 10000


# ─────────────────────────────────────────────
def valider_parametres(
    nb_lignes: int, proba_null: float, proba_bruit: float, pct_corrompues: float
) -> None:
    if nb_lignes <= 0:
        print("Erreur : NB_LIGNES doit être > 0.", file=sys.stderr)
        sys.exit(1)
    if not (0.0 <= proba_null <= 1.0):
        print("Erreur : PROBA_NULL doit être entre 0 et 1.", file=sys.stderr)
        sys.exit(1)
    if not (0.0 <= proba_bruit <= 1.0):
        print("Erreur : PROBA_BRUIT doit être entre 0 et 1.", file=sys.stderr)
        sys.exit(1)
    if proba_null + proba_bruit > 1.0:
        print("Erreur : somme PROBA_NULL + PROBA_BRUIT > 1.", file=sys.stderr)
        sys.exit(1)
    if not (0.0 <= pct_corrompues <= 100.0):
        print(
            "Erreur : POURCENT_LIGNES_CORROMPUES doit être entre 0 et 100.",
            file=sys.stderr,
        )
        sys.exit(1)


def bruit_aleatoire(longueur: int = 8) -> str:
    caracteres = string.ascii_letters + string.digits + "!@#$%^&*()"
    return "".join(random.choices(caracteres, k=longueur))


def generer_ligne_corrompue(nb_colonnes: int) -> str:
    nb_champs = random.randint(1, nb_colonnes + random.randint(1, 3))
    champs = [bruit_aleatoire(random.randint(3, 20)) for _ in range(nb_champs)]
    return "\t".join(champs)


def generer_chunk(args):
    debut, fin, seed_offset, proba_null, proba_bruit, activer_nulls, pct_corrompues = (
        args
    )
    random.seed(SEED + seed_offset)

    lignes = []
    n = fin - debut

    for i in range(n):
        idx_global = debut + i

        # Vérifier corruption ligne
        est_corrompue = random.random() < (pct_corrompues / 100.0)
        if est_corrompue and not MODE_PARFAIT:
            lignes.append(generer_ligne_corrompue(17))
            continue

        # Générer ligne normale
        colonnes = []

        # Boolean
        v = idx_global % 2 == 0
        p = random.random()
        if activer_nulls and p < proba_null:
            colonnes.append("NA")
        elif p < proba_null + proba_bruit:
            colonnes.append(bruit_aleatoire(random.randint(4, 12)))
        else:
            colonnes.append(str(v))

        # Int32
        v = idx_global
        p = random.random()
        if activer_nulls and p < proba_null:
            colonnes.append("NA")
        elif p < proba_null + proba_bruit:
            colonnes.append(bruit_aleatoire(random.randint(4, 12)))
        else:
            colonnes.append(str(v))

        # Int64
        v = idx_global * 1000
        p = random.random()
        if activer_nulls and p < proba_null:
            colonnes.append("NA")
        elif p < proba_null + proba_bruit:
            colonnes.append(bruit_aleatoire(random.randint(4, 12)))
        else:
            colonnes.append(str(v))

        # UInt32
        v = idx_global
        p = random.random()
        if activer_nulls and p < proba_null:
            colonnes.append("NA")
        elif p < proba_null + proba_bruit:
            colonnes.append(bruit_aleatoire(random.randint(4, 12)))
        else:
            colonnes.append(str(v))

        # UInt64
        v = idx_global * 10_000
        p = random.random()
        if activer_nulls and p < proba_null:
            colonnes.append("NA")
        elif p < proba_null + proba_bruit:
            colonnes.append(bruit_aleatoire(random.randint(4, 12)))
        else:
            colonnes.append(str(v))

        # Float16
        v = round(idx_global * 0.5, 2)
        p = random.random()
        if activer_nulls and p < proba_null:
            colonnes.append("NA")
        elif p < proba_null + proba_bruit:
            colonnes.append(bruit_aleatoire(random.randint(4, 12)))
        else:
            colonnes.append(str(v))

        # Float32
        v = idx_global * 0.1
        p = random.random()
        if activer_nulls and p < proba_null:
            colonnes.append("NA")
        elif p < proba_null + proba_bruit:
            colonnes.append(bruit_aleatoire(random.randint(4, 12)))
        else:
            colonnes.append(str(v))

        # Float64
        v = idx_global * 0.0001
        p = random.random()
        if activer_nulls and p < proba_null:
            colonnes.append("NA")
        elif p < proba_null + proba_bruit:
            colonnes.append(bruit_aleatoire(random.randint(4, 12)))
        else:
            colonnes.append(str(v))

        # Utf8
        v = f"texte_{idx_global}"
        p = random.random()
        if activer_nulls and p < proba_null:
            colonnes.append("NA")
        elif p < proba_null + proba_bruit:
            colonnes.append(bruit_aleatoire(random.randint(4, 12)))
        else:
            colonnes.append(v)

        # Utf8View
        v = f"vue_{idx_global}"
        p = random.random()
        if activer_nulls and p < proba_null:
            colonnes.append("NA")
        elif p < proba_null + proba_bruit:
            colonnes.append(bruit_aleatoire(random.randint(4, 12)))
        else:
            colonnes.append(v)

        # LargeUtf8
        v = f"texte_long_{idx_global}" * 2
        p = random.random()
        if activer_nulls and p < proba_null:
            colonnes.append("NA")
        elif p < proba_null + proba_bruit:
            colonnes.append(bruit_aleatoire(random.randint(4, 12)))
        else:
            colonnes.append(v)

        # Binary
        v = f"bin_{idx_global}"
        p = random.random()
        if activer_nulls and p < proba_null:
            colonnes.append("NA")
        elif p < proba_null + proba_bruit:
            colonnes.append(bruit_aleatoire(random.randint(4, 12)))
        else:
            colonnes.append(v)

        # Date32
        d = date(2024, 1, 1) + timedelta(days=idx_global % 10000)
        v = d.isoformat()
        p = random.random()
        if activer_nulls and p < proba_null:
            colonnes.append("NA")
        elif p < proba_null + proba_bruit:
            colonnes.append(bruit_aleatoire(random.randint(4, 12)))
        else:
            colonnes.append(v)

        # Timestamp(Millisecond, None)
        base = "2024-01-01T12:00:00"
        ms_offset = (idx_global * 5) % 86400000
        heures = ms_offset // 3600000
        minutes = (ms_offset % 3600000) // 60000
        secondes = (ms_offset % 60000) // 1000
        ms = ms_offset % 1000
        v = f"2024-01-01T{heures:02d}:{minutes:02d}:{secondes:02d}.{ms:03d}"
        p = random.random()
        if activer_nulls and p < proba_null:
            colonnes.append("NA")
        elif p < proba_null + proba_bruit:
            colonnes.append(bruit_aleatoire(random.randint(4, 12)))
        else:
            colonnes.append(v)

        # Timestamp(Nanosecond, None)
        sec_offset = idx_global % 86400
        heures = sec_offset // 3600
        minutes = (sec_offset % 3600) // 60
        secondes = sec_offset % 60
        v = f"2024-01-01T{heures:02d}:{minutes:02d}:{secondes:02d}"
        p = random.random()
        if activer_nulls and p < proba_null:
            colonnes.append("NA")
        elif p < proba_null + proba_bruit:
            colonnes.append(bruit_aleatoire(random.randint(4, 12)))
        else:
            colonnes.append(v)

        # Decimal32
        v = round(idx_global / 10.0, 2)
        p = random.random()
        if activer_nulls and p < proba_null:
            colonnes.append("NA")
        elif p < proba_null + proba_bruit:
            colonnes.append(bruit_aleatoire(random.randint(4, 12)))
        else:
            colonnes.append(str(v))

        # Decimal128(38, 10)
        v = round(idx_global / 3.1415926535, 10)
        p = random.random()
        if activer_nulls and p < proba_null:
            colonnes.append("NA")
        elif p < proba_null + proba_bruit:
            colonnes.append(bruit_aleatoire(random.randint(4, 12)))
        else:
            colonnes.append(str(v))

        lignes.append("\t".join(colonnes))

    return lignes


# ─────────────────────────────────────────────
def main() -> None:
    proba_null = PROBA_NULL_PCT / 100.0
    proba_bruit = PROBA_BRUIT_PCT / 100.0

    if MODE_PARFAIT:
        proba_null = 0.0
        proba_bruit = 0.0
        pct_corrompues = 0.0
        activer_nulls = False
    else:
        pct_corrompues = POURCENT_LIGNES_CORROMPUES
        activer_nulls = not DESACTIVER_NULLS

    valider_parametres(NB_LIGNES, proba_null, proba_bruit, pct_corrompues)

    # Préparer chunks
    chunks = []
    for i in range(0, NB_LIGNES, TAILLE_CHUNK):
        fin = min(i + TAILLE_CHUNK, NB_LIGNES)
        chunks.append(
            (
                i,
                fin,
                i // TAILLE_CHUNK,
                proba_null,
                proba_bruit,
                activer_nulls,
                pct_corrompues,
            )
        )

    # Entête
    entete = "\t".join(
        [
            "Boolean",
            "Int32",
            "Int64",
            "UInt32",
            "UInt64",
            "Float16",
            "Float32",
            "Float64",
            "Utf8",
            "Utf8View",
            "LargeUtf8",
            "Binary",
            "Date32",
            "Timestamp(Millisecond, None)",
            "Timestamp(Nanosecond, None)",
            "Decimal32",
            "Decimal128(38, 10)",
        ]
    )

    try:
        with open(FICHIER_SORTIE, "w", encoding="utf-8", buffering=8192 * 1024) as f:
            f.write(entete + "\n")

            with Pool(processes=NB_PROCESSUS) as pool:
                for lignes_chunk in pool.imap(generer_chunk, chunks):
                    f.write("\n".join(lignes_chunk) + "\n")
    except OSError as e:
        print(f"Erreur lors de l'écriture du fichier : {e}", file=sys.stderr)
        sys.exit(1)

    print("─────────────────────────────────────────────")
    print("📊 Jeu de test complet généré")
    print(f"   → Fichier : {FICHIER_SORTIE}")
    print(f"   → Seed : {SEED}")
    print(f"   → Lignes totales : {NB_LIGNES}")
    print(f"   → Colonnes : 17")
    print(f"   → Processus : {NB_PROCESSUS}")
    print(f"   → Taille chunk : {TAILLE_CHUNK}")
    print(f"   → Prob. NULL : {PROBA_NULL_PCT}%")
    print(f"   → Prob. bruit : {PROBA_BRUIT_PCT}%")
    print(f"   → Pct. corrompues : {pct_corrompues}%")
    print(f"   → Nulls activés : {'Oui' if activer_nulls else 'Non'}")
    print(f"   → Mode parfait : {'Oui' if MODE_PARFAIT else 'Non'}")
    print("✅ Fichier généré avec tous les types attendus (incluant UInt32 et UInt64).")
    print("─────────────────────────────────────────────")


# ─────────────────────────────────────────────
if __name__ == "__main__":
    main()
