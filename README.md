# Bot ISP Dados – RJ (CSV diário)

Este projeto baixa diariamente os arquivos CSV da página do ISP RJ e mantém um histórico (versão por hash).
Os CSVs são normalizados para UTF-8 (separador `;`) e consolidados em um `master.parquet` (ou `master.csv` fallback).

## Como usar

1. Instale as dependências:
   ```bash
   pip install -r requirements.txt
   ```

2. Rode o notebook `notebooks/executar_bot.ipynb` **ou** o script diretamente:
   ```bash
   python scripts/isp_bot.py
   ```

### Estrutura
```
projeto_ispdados_rj_bot/
├── data/
│   ├── raw/          # arquivos baixados com timestamp e hash
│   ├── processed/    # CSV normalizados (UTF-8 ;)
│   ├── seed/         # amostras iniciais (opcional, ex: BaseDPEvolucaoMensalCisp.csv)
│   ├── seed_schema.json
│   └── master.parquet (ou master.csv)
├── logs/
│   └── isp_bot.log
├── notebooks/
│   └── executar_bot.ipynb
└── scripts/
    └── isp_bot.py
```

## Agendamento diário
- **Windows**: Agendador de Tarefas → ação `python {CAMINHO}\scripts\isp_bot.py`
- **Linux/macOS**: `cron` (ex: `0 6 * * * /usr/bin/python /caminho/projeto/scripts/isp_bot.py`)

## Observações
- O bot baixa apenas **novas versões** (hash SHA-256 do conteúdo).
- Ajuste o `CSV_WHITELIST` no script para filtrar por nomes de arquivos específicos.
