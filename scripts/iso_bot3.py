import requests
from pathlib import Path
from bs4 import BeautifulSoup

# URL da página principal
BASE_URL = "https://www.ispdados.rj.gov.br/estatistica.html"

# Pasta destino
data_dir = Path("data")
data_dir.mkdir(exist_ok=True)

def baixar_base_municipio():
    resp = requests.get(BASE_URL)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    # Procura pelo link BaseMunicipioMensal.csv
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "BaseMunicipioMensal.csv" in href:
            if not href.startswith("http"):
                href = requests.compat.urljoin(BASE_URL, href)
            print("Baixando:", href)

            r = requests.get(href)
            r.raise_for_status()

            destino = data_dir / "BaseMunicipioMensal.csv"
            destino.write_bytes(r.content)

            print(f"Arquivo salvo em {destino.resolve()}")
            return destino

    raise RuntimeError("Não encontrei BaseMunicipioMensal.csv na página")

if __name__ == "__main__":
    baixar_base_municipio()
