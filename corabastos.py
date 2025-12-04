import os
import datetime as dt
import io
import requests
import pdfplumber
import pandas as pd

import gspread
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request

# =========================
# CONFIG
# =========================

SPREADSHEET_ID = os.environ["GOOGLE_SHEET_ID"]
SHEET_NAME = "Boletin"

SPANISH_MONTHS = {
    1: "enero",
    2: "febrero",
    3: "marzo",
    4: "abril",
    5: "mayo",
    6: "junio",
    7: "julio",
    8: "agosto",
    9: "septiembre",
    10: "octubre",
    11: "noviembre",
    12: "diciembre",
}

# =========================
# AUTH CON OAUTH (SIN CUENTA DE SERVICIO)
# =========================

def get_gspread_client():
    creds = Credentials(
        token=None,
        refresh_token=os.environ["GOOGLE_REFRESH_TOKEN"],
        token_uri="https://oauth2.googleapis.com/token",
        client_id=os.environ["GOOGLE_CLIENT_ID"],
        client_secret=os.environ["GOOGLE_CLIENT_SECRET"],
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    # Refresca el token de acceso usando el refresh_token
    creds.refresh(Request())
    gc = gspread.authorize(creds)
    return gc

# =========================
# LÓGICA CORABASTOS
# =========================

def es_dia_habil(fecha: dt.date) -> bool:
    return fecha.weekday() < 5  # 0=Lunes, 6=Domingo


def construir_urls_candidatas(fecha: dt.date) -> list:
    year = fecha.year
    month = fecha.month
    day = fecha.day
    mes_nombre = SPANISH_MONTHS[month]

    base = f"https://corabastos.com.co/wp-content/uploads/{year}/{month:02d}/"

    candidatos = [
        f"{base}Boletin-{day:02d}{mes_nombre}{year}.pdf",
        f"{base}Boletin-{day}{mes_nombre}{year}.pdf",
        f"{base}Boletin_diario_{year}{month:02d}{day:02d}.pdf",
    ]
    return candidatos


def descargar_pdf(fecha: dt.date) -> bytes | None:
    urls = construir_urls_candidatas(fecha)
    for url in urls:
        try:
            print(f"Probando URL: {url}")
            resp = requests.get(url, timeout=20)
            content_type = resp.headers.get("Content-Type", "").lower()
            if resp.status_code == 200 and "pdf" in content_type:
                print(f"✔ PDF encontrado en: {url}")
                return resp.content
        except Exception as e:
            print(f"Error probando {url}: {e}")
    print(f"No se encontró PDF para la fecha {fecha}")
    return None


def extraer_tablas_pdf(pdf_bytes: bytes) -> pd.DataFrame:
    all_tables = []

    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for page_num, page in enumerate(pdf.pages, start=1):
            tables = page.extract_tables()
            for table_idx, table in enumerate(tables):
                if not table or len(table) < 2:
                    continue
                header = table[0]
                rows = table[1:]
                df = pd.DataFrame(rows, columns=header)
                df["pagina"] = page_num
                df["tabla"] = table_idx + 1
                all_tables.append(df)

    if not all_tables:
        raise ValueError("No se encontraron tablas en el PDF.")

    df_total = pd.concat(all_tables, ignore_index=True)
    df_total.replace("", pd.NA, inplace=True)
    df_total.dropna(how="all", inplace=True)
    return df_total


def agregar_fecha(df: pd.DataFrame, fecha: dt.date) -> pd.DataFrame:
    df = df.copy()
    df.insert(0, "Fecha", fecha.isoformat())
    return df


def inicializar_encabezados_si_vacio(ws, df: pd.DataFrame):
    existing = ws.get_all_values()
    if not existing:
        ws.append_row(list(df.columns))


def append_df_to_sheet(ws, df: pd.DataFrame):
    values = df.values.tolist()
    if values:
        ws.append_rows(values, value_input_option="USER_ENTERED")


def main():
    hoy = dt.date.today()

    if not es_dia_habil(hoy):
        print(f"Hoy {hoy} no es día hábil. No se hace nada.")
        return

    print(f"Procesando boletín para la fecha {hoy}...")

    pdf_bytes = descargar_pdf(hoy)
    if pdf_bytes is None:
        print("No se descargó PDF, termina el proceso.")
        return

    try:
        df = extraer_tablas_pdf(pdf_bytes)
    except Exception as e:
        print(f"Error extrayendo tablas: {e}")
        return

    df = agregar_fecha(df, hoy)

    try:
        gc = get_gspread_client()
        sh = gc.open_by_key(SPREADSHEET_ID)
        try:
            ws = sh.worksheet(SHEET_NAME)
        except gspread.WorksheetNotFound:
            ws = sh.add_worksheet(title=SHEET_NAME, rows=1000, cols=30)

        inicializar_encabezados_si_vacio(ws, df)
        append_df_to_sheet(ws, df)
        print("Carga completada en Google Sheets.")
    except Exception as e:
        print(f"Error escribiendo en Google Sheets: {e}")


if __name__ == "__main__":
    main()
