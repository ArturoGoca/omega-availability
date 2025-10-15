
# Omega Availability (Flask + MySQL)

Estructura profesional con **templates** y **static**. Consulta un ensamble Omega y muestra sus componentes y el On Hand (agregado en SQL).

## Estructura
```
.
├─ app.py
├─ conexion.py
├─ requirements.txt
├─ templates/
│  ├─ layout.html
│  └─ index.html
└─ static/
   └─ styles.css
```

## Requisitos
```
pip install -r requirements.txt
```
Asegúrate que MySQL tenga las tablas:
- `bom_omega (item, item_description, component, component_description, qty_per)`
- `existencias_1 (item_number, qty, ...)`

## Variables de entorno (opcional)
```
set DB_HOST=127.0.0.1
set DB_USER=root
set DB_PASS=
set DB_NAME=omega
```

## Ejecutar
```
python app.py
```
Abre `http://localhost:8000` y busca un `Item` (ej. `TC00001X012`).

## Exportar
Botón **Exportar CSV** genera un archivo con los componentes y su on hand.

## Notas
- El agregado de on hand se hace en SQL (`SUM(e.qty)` + `GROUP BY`), evitando cargar toda la tabla a memoria.
- Para performance, agrega índices (si no los tienes):
```
ALTER TABLE bom_omega ADD INDEX idx_item (item), ADD INDEX idx_component (component);
ALTER TABLE existencias_1 ADD INDEX idx_item_number (item_number);
```
