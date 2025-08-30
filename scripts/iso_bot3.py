import requests
from pathlib import Path
from bs4 import BeautifulSoup
import logging
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials

# === CONFIGURAÇÃO ===
BASE_URL = "https://www.ispdados.rj.gov.br/estatistica.html"
data_dir = Path("data")
logs_dir = Path("logs")
json_keyfile = "calculo-p-valor-3190f56f75a4.json"  # nova chave
sheet_id = "1IrSLMHgg2dNU4Py6X2RiwW7sfrcwPgLpQTxEK3ATTlo"

# === CRIA PASTAS ===
data_dir.mkdir(exist_ok=True)
logs_dir.mkdir(exist_ok=True)

# === LOG ===
log_file = logs_dir / "isp_bot.log"
logging.basicConfig(
    filename=log_file,
    filemode="a",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logging.info("==== Iniciando execução do bot ====")

def baixar_base_municipio():
    try:
        logging.info("Acessando página principal...")
        resp = requests.get(BASE_URL, headers={"User-Agent": "Mozilla/5.0"})
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "BaseMunicipioMensal.csv" in href:
                if not href.startswith("http"):
                    href = requests.compat.urljoin(BASE_URL, href)

                logging.info(f"Baixando arquivo: {href}")
                r = requests.get(href)
                r.raise_for_status()

                destino = data_dir / "BaseMunicipioMensal.csv"
                destino.write_bytes(r.content)

                logging.info(f"Arquivo atualizado e salvo em {destino.resolve()}")

                # envia para Google Sheets
                enviar_para_google_sheets(destino)
                return destino

        raise RuntimeError("Não encontrei BaseMunicipioMensal.csv na página")

    except Exception as e:
        logging.error(f"Erro ao baixar: {e}")
        raise

def enviar_para_google_sheets(csv_path: Path):
    try:
        logging.info("Lendo CSV para envio ao Sheets...")
        # detecta encoding automaticamente
        with open(csv_path, "rb") as f:
            content = f.read()
        df = pd.read_csv(pd.compat.StringIO(content.decode("latin1")), sep=';')

        logging.info("Autenticando com Google Sheets...")
        creds = Credentials.from_service_account_file(
            json_keyfile,
            scopes=["https://www.googleapis.com/auth/spreadsheets"]
        )
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(sheet_id)
        worksheet = sh.sheet1

        logging.info("Limpando planilha antes de escrever...")
        worksheet.clear()

        logging.info("Enviando dados...")
        worksheet.update([df.columns.tolist()] + df.values.tolist())

        logging.info("✅ Planilha atualizada com sucesso.")

    except Exception as e:
        logging.error(f"Erro ao enviar para o Google Sheets: {e}")
        print(f"❌ Falha ao enviar para o Google Sheets: {e}")

if __name__ == "__main__":
    baixar_base_municipio()
