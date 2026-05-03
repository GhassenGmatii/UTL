import pyodbc
import pandas as pd
import numpy as np
from pathlib import Path
from .config import CLEAN_OUTPUT, DEDUP_OUTPUT

# ── Configuration SQL Server ───────────────────────────────────────────────────
SERVER   = r'localhost\SQLEXPRESS'
DATABASE = 'UTL_DB'
DRIVER   = 'ODBC Driver 17 for SQL Server'

TABLE_CLEAN = 'patients_clean'
TABLE_DEDUP = 'patients_dedup'


def _conn_str(database='master'):
    return (
        f'Driver={{{DRIVER}}};'
        f'Server={SERVER};'
        f'Database={database};'
        f'Trusted_Connection=yes;'
    )


def _pandas_dtype_to_sql(dtype, col_name: str) -> str:
    """Convertit un dtype pandas en type SQL Server."""
    col_lower = col_name.lower()
    if 'date' in col_lower:
        return 'DATE'
    if dtype == 'object':
        return 'NVARCHAR(255)'
    if dtype in ('int64', 'int32', 'int16', 'int8'):
        return 'INT'
    if dtype in ('float64', 'float32'):
        return 'FLOAT'
    if dtype == 'bool':
        return 'BIT'
    return 'NVARCHAR(255)'


def create_database() -> bool:
    """Crée UTL_DB si elle n'existe pas (requiert autocommit=True pour DDL)."""
    try:
        conn = pyodbc.connect(_conn_str('master'), autocommit=True)
        cursor = conn.cursor()

        cursor.execute(
            "SELECT name FROM sys.databases WHERE name = ?", DATABASE
        )
        if not cursor.fetchone():
            print(f"[DB] Creation de la base de donnees : {DATABASE} ...")
            cursor.execute(f"CREATE DATABASE [{DATABASE}]")
            print(f"[DB] Base de donnees '{DATABASE}' creee avec succes.")
        else:
            print(f"[DB] La base de donnees '{DATABASE}' existe deja.")

        conn.close()
        return True
    except Exception as e:
        print(f"[DB] Erreur lors de la creation de la base : {e}")
        return False


def create_table_from_df(conn, df: pd.DataFrame, table_name: str) -> bool:
    """Crée (ou recrée) une table SQL dont les colonnes correspondent au DataFrame."""
    cursor = conn.cursor()
    try:
        # Supprimer la table si elle existe déjà
        cursor.execute(
            f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE [{table_name}]"
        )

        # Construire la définition des colonnes
        col_defs = []
        for col in df.columns:
            sql_type = _pandas_dtype_to_sql(str(df[col].dtype), col)
            safe_col = col.replace("'", "")
            col_defs.append(f"[{safe_col}] {sql_type}")

        ddl = f"CREATE TABLE [{table_name}] (\n    {',\n    '.join(col_defs)}\n)"
        cursor.execute(ddl)
        conn.commit()
        print(f"[TABLE] Table '{table_name}' creee ({len(df.columns)} colonnes).")
        return True
    except Exception as e:
        print(f"[TABLE] Erreur lors de la creation de '{table_name}' : {e}")
        conn.rollback()
        return False


def load_data_to_db(csv_path: Path, table_name: str) -> bool:
    """Charge un CSV dans la table SQL correspondante."""
    try:
        df = pd.read_csv(csv_path)
        print(f"[LOAD] Chargement de {len(df)} lignes depuis '{csv_path.name}'...")

        conn = pyodbc.connect(_conn_str(DATABASE))

        # Créer la table avec les colonnes réelles du CSV
        if not create_table_from_df(conn, df, table_name):
            conn.close()
            return False

        cursor = conn.cursor()

        # Préparer les colonnes
        safe_cols = [f"[{c}]" for c in df.columns]
        placeholders = ','.join(['?' for _ in df.columns])
        insert_sql = (
            f"INSERT INTO [{table_name}] ({','.join(safe_cols)}) "
            f"VALUES ({placeholders})"
        )

        # Insérer ligne par ligne avec conversion NaN → None
        rows_inserted = 0
        for _, row in df.iterrows():
            values = []
            for val in row:
                if val is None or (isinstance(val, float) and np.isnan(val)):
                    values.append(None)
                else:
                    values.append(val)
            try:
                cursor.execute(insert_sql, values)
                rows_inserted += 1
            except Exception as row_err:
                print(f"[LOAD] Ligne ignoree (erreur) : {row_err}")

        conn.commit()
        conn.close()
        print(f"[LOAD] {rows_inserted}/{len(df)} lignes inserees dans '{table_name}'.")
        return True

    except Exception as e:
        print(f"[LOAD] Erreur : {e}")
        return False


def verify_data() -> bool:
    """Affiche le nombre de lignes dans chaque table."""
    try:
        conn = pyodbc.connect(_conn_str(DATABASE))
        cursor = conn.cursor()
        results = []
        for tbl in [TABLE_CLEAN, TABLE_DEDUP]:
            try:
                cursor.execute(f"SELECT COUNT(*) FROM [{tbl}]")
                count = cursor.fetchone()[0]
                results.append((tbl, count))
            except Exception:
                results.append((tbl, 'table introuvable'))

        conn.close()
        print("\n" + "="*50)
        print("Verification des donnees dans SQL Server :")
        for tbl, count in results:
            print(f"  - {tbl} : {count} ligne(s)")
        print("="*50)
        return True
    except Exception as e:
        print(f"[VERIFY] Erreur : {e}")
        return False


def main():
    print("\n" + "="*50)
    print("Chargement des donnees dans SQL Server")
    print("="*50)

    # Etape 1 : Creer la base de donnees
    print("\n[1/4] Creation de la base de donnees...")
    if not create_database():
        print("Echec de la creation de la base de donnees. Arret.")
        return

    # Etape 2 : Charger les donnees nettoyees
    print("\n[2/4] Chargement des donnees nettoyees...")
    if CLEAN_OUTPUT.exists():
        load_data_to_db(CLEAN_OUTPUT, TABLE_CLEAN)
    else:
        print(f"  Fichier introuvable : {CLEAN_OUTPUT}")
        print("  Lancez d'abord le pipeline ETL.")

    # Etape 3 : Charger les donnees finales
    print("\n[3/4] Chargement des donnees finales (deduplication + anomalies)...")
    if DEDUP_OUTPUT.exists():
        load_data_to_db(DEDUP_OUTPUT, TABLE_DEDUP)
    else:
        print(f"  Fichier introuvable : {DEDUP_OUTPUT}")

    # Etape 4 : Verification
    print("\n[4/4] Verification...")
    verify_data()

    print("\nTermine. Vous pouvez verifier dans SSMS :")
    print(f"  SELECT * FROM [{TABLE_CLEAN}]")
    print(f"  SELECT * FROM [{TABLE_DEDUP}]")
    print(f"  SELECT * FROM [{TABLE_DEDUP}] WHERE is_anomaly = 1\n")


if __name__ == "__main__":
    main()
