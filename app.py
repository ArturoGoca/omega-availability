from flask import Flask, request, render_template, send_file, abort
import pandas as pd
import io
from conexion import get_conn

app = Flask(__name__)
app.config['TEMPLATES_AUTO_RELOAD'] = True  # opcional, ayuda mientras desarrollas

def fetch_components_for_item(item_code: str):
    if not item_code:
        return [], None  # <- nunca regreses None

    sql = """
    SELECT
        b.component,
        b.component_description,
        b.qty_per,
        COALESCE(SUM(e.qty), 0)                          AS on_hand,
        GREATEST(b.qty_per - COALESCE(SUM(e.qty),0), 0)  AS faltante,
        lt.ctlt_days                                     AS lead_time_days,
        CASE 
          WHEN GREATEST(b.qty_per - COALESCE(SUM(e.qty),0), 0) = 0 THEN NULL
          WHEN lt.ctlt_days IS NULL THEN NULL
          ELSE (lt.ctlt_days + 14)
        END AS dias_totales,
        CASE 
          WHEN GREATEST(b.qty_per - COALESCE(SUM(e.qty),0), 0) = 0 THEN NULL
          WHEN lt.ctlt_days IS NULL THEN NULL
          ELSE DATE_ADD(CURDATE(), INTERVAL (lt.ctlt_days + 14) DAY)
        END AS fecha_estimada,
        lt.make_or_buy,
        lt.planner,
        lt.buyer,
        lt.abc_class,
        lt.ann_usage_pieces,
        lt.primary_supplier
    FROM bom_omega b
    LEFT JOIN existencias_1 e
           ON e.item_number = b.component
    LEFT JOIN lead_times lt
           ON lt.component = b.component
    WHERE b.item = %s
    GROUP BY b.component, b.component_description, b.qty_per,
             lt.ctlt_days, lt.make_or_buy, lt.planner, lt.buyer,
             lt.abc_class, lt.ann_usage_pieces, lt.primary_supplier
    ORDER BY (GREATEST(b.qty_per - COALESCE(SUM(e.qty),0), 0) > 0) DESC,
             on_hand ASC, b.component ASC;
    """

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (item_code.strip(),))
            rows = cur.fetchall()

    # ← calcula la fecha más lejana entre los componentes con faltante y LT
    best_ship_date = None
    for r in rows:
        if r.get("faltante", 0) and r.get("fecha_estimada") is not None:
            d = r["fecha_estimada"]
            if best_ship_date is None or d > best_ship_date:
                best_ship_date = d

    return rows, best_ship_date  # <- ¡devuelve!

@app.route("/", methods=["GET"])
def index():
    q = request.args.get("q", "", type=str).strip()
    results, best_ship_date = fetch_components_for_item(q) if q else ([], None)
    return render_template("index.html", q=q, results=results, best_ship_date=best_ship_date)

@app.route("/export", methods=["GET"])
def export():
    q = request.args.get("q", "", type=str).strip()
    if not q:
        abort(400, "Falta el parámetro q (item).")
    rows, _ = fetch_components_for_item(q)   # <- ahora devuelve tupla

    if not rows:
        abort(404, f"No se encontraron componentes para el ensamble {q}.")

    # Exporta todas las columnas presentes
    df = pd.DataFrame(rows)
    buf = io.BytesIO()
    buf.write(df.to_csv(index=False).encode("utf-8-sig"))
    buf.seek(0)
    filename = f"omega_{q}_components.csv"
    return send_file(buf, as_attachment=True, download_name=filename, mimetype="text/csv")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True)
