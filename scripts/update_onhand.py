import os
import shutil
import time
import pymysql
from pymysql.cursors import DictCursor
from datetime import datetime

# =======================
# CONFIG
# =======================
SHARE = r"\\mxtolp-fs01\global\materiales\RPT\RPT OnHand.csv"   # origen (Noetix)
LOCAL_DIR = r"C:\omega\imports"
LOCAL = os.path.join(LOCAL_DIR, "RPT_OnHand.csv")
LOG_DIR = r"C:\omega\logs"
LOG = os.path.join(LOG_DIR, f"onhand_{datetime.now():%Y-%m-%d}.log")

DB = dict(
    host="127.0.0.1",
    user="root",
    password="",
    database="omega",
)

# Columnas esperadas exactamente como vienen en el encabezado
EXPECTED_COLS = [
    "Item_Number", "Item_Description", "Qty", "UOM",
    "Locator", "Subinventory", "Planner", "Organization_Code"
]

# =======================
# UTILS
# =======================
def ensure_dirs():
    os.makedirs(LOCAL_DIR, exist_ok=True)
    os.makedirs(LOG_DIR, exist_ok=True)

def log(msg: str):
    ensure_dirs()
    line = f"[{datetime.now():%Y-%m-%d %H:%M:%S}] {msg}"
    print(line)
    with open(LOG, "a", encoding="utf-8") as f:
        f.write(line + "\n")

def wait_until_stable(path, timeout=600, interval=5):
    """Espera a que el archivo deje de crecer (tamaño estable)."""
    start = time.time()
    last_size = -1
    stable_count = 0
    while time.time() - start < timeout:
        if not os.path.exists(path):
            time.sleep(interval)
            continue
        size = os.path.getsize(path)
        if size == last_size:
            stable_count += 1
            if stable_count >= 2:  # estable por 2 intervalos
                return True
        else:
            stable_count = 0
        last_size = size
        time.sleep(interval)
    return False

def detect_line_ending(path):
    """Detecta \\r\\n vs \\n para LINES TERMINATED BY."""
    with open(path, "rb") as f:
        chunk = f.read(4096)
    return "\r\n" if b"\r\n" in chunk else "\n"

def read_header_line(path):
    """Lee la primera línea (encabezado) y quita BOM si existe."""
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        header = f.readline().strip()
    if header and header[0] == "\ufeff":  # BOM
        header = header.lstrip("\ufeff")
    return header

def detect_delimiter(header_line: str) -> str:
    """Detecta delimitador por conteo simple: tab vs coma."""
    tabs = header_line.count("\t")
    commas = header_line.count(",")
    return "\t" if tabs > commas else ","

def get_conn():
    # local_infile=True -> permite LOAD DATA LOCAL INFILE (requiere local_infile=ON en servidor)
    return pymysql.connect(
        host=DB["host"],
        user=DB["user"],
        password=DB["password"],
        database=DB["database"],
        charset="utf8mb4",
        autocommit=True,
        cursorclass=DictCursor,
        local_infile=True,
    )

# =======================
# MAIN
# =======================
def main():
    ensure_dirs()

    # 1) Copiar del share a local
    log("Copiando archivo desde el share…")
    try:
        shutil.copyfile(SHARE, LOCAL)
    except Exception as e:
        log(f"ERROR copiando desde share: {e}")
        raise

    # 2) Esperar a que termine de escribirse
    log("Esperando a que el archivo se estabilice…")
    if not wait_until_stable(LOCAL, timeout=600, interval=5):
        log("ERROR: el archivo no se estabilizó a tiempo.")
        raise RuntimeError("Archivo no estable")

    # 3) Detectar saltos de línea y delimitador; validar encabezados
    line_ending = detect_line_ending(LOCAL)  # '\r\n' o '\n'
    log(f"Salto de línea detectado: {repr(line_ending)}")

    header_line = read_header_line(LOCAL)
    delimiter = detect_delimiter(header_line)  # ',' o '\t'
    log(f"Delimitador detectado: {repr(delimiter)}")

    header_cols = [c.strip() for c in header_line.lstrip("\ufeff").split(delimiter)]
    missing = [c for c in EXPECTED_COLS if c not in header_cols]
    if missing:
        log(f"ERROR: columnas faltantes: {missing}")
        raise RuntimeError(f"Columnas faltantes: {missing}")
    else:
        log(f"Encabezados OK: {header_cols}")

    # Literales para SQL
    delim_sql = "\\t" if delimiter == "\t" else ","
    line_sql  = "\\r\\n" if line_ending == "\r\n" else "\\n"

    # Si es CSV (coma), considerar comillas
    if delimiter == ",":
        fields_clause = "FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' ESCAPED BY '\"'"
    else:
        fields_clause = "FIELDS TERMINATED BY '\\t' ENCLOSED BY '' ESCAPED BY ''"

    # 4) Cargar a STAGING con LOAD DATA (sin swap)
    log("Cargando archivo a STAGING con LOAD DATA LOCAL INFILE…")
    with get_conn() as conn:
        with conn.cursor() as cur:
            try:
                # Modo permisivo para conversiones
                cur.execute("SET SESSION sql_mode=''")
                # No ejecutar SET SESSION local_infile=1 (tu server lo tiene ON globalmente)
                cur.execute("TRUNCATE TABLE existencias_staging")
                cur.execute(
                    f"""
                    LOAD DATA LOCAL INFILE %s
                    INTO TABLE existencias_staging
                    CHARACTER SET utf8mb4
                    {fields_clause}
                    LINES TERMINATED BY '{line_sql}'
                    IGNORE 1 LINES
                    (item_number, item_description, @v_qty, uom, locator, subinventory, planner, organization_code)
                    SET qty = CAST(REPLACE(NULLIF(@v_qty,''), ',', '') AS DECIMAL(12,3));
                    """,
                    (LOCAL,),
                )
                log("Normalizando espacios en STAGING…")
                cur.execute("""
                    UPDATE existencias_staging
                       SET item_number=TRIM(item_number),
                           item_description=TRIM(item_description),
                           uom=TRIM(uom),
                           locator=TRIM(locator),
                           subinventory=TRIM(subinventory),
                           planner=TRIM(planner),
                           organization_code=TRIM(organization_code);
                """)
                log("Carga a STAGING completada OK. (No se hizo swap)")
            except Exception as e:
                log(f"ERROR en carga a STAGING: {e}")
                raise

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        # Ya se registró en log; relanzamos para ver stacktrace si corres manual
        raise
