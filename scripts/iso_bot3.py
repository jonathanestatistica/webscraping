import requests
from pathlib import Path
from bs4 import BeautifulSoup
import logging

# URL da página principal
BASE_URL = "https://www.ispdados.rj.gov.br/estatistica.html"

# Pastas destino
data_dir = Path("data")
data_dir.mkdir(exist_ok=True)

logs_dir = Path("logs")
logs_dir.mkdir(exist_ok=True)

# Configuração do log
log_file = logs_dir / "isp_bot.log"
logging.basicConfig(
    filename=log_file,
    filemode="a",  # append (não sobrescreve)
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# 🔑 Linha importante: garante que o log inicial seja escrito
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

                msg = f"Arquivo atualizado e salvo em {destino.resolve()}"
                print(msg)        # terminal
                logging.info(msg) # log
                return destino

        raise RuntimeError("Não encontrei BaseMunicipioMensal.csv na página")

    except Exception as e:
        logging.error(f"Erro ao baixar: {e}")
        raise

if __name__ == "__main__":
    baixar_base_municipio()
