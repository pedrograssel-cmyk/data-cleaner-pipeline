# 🧹 Data Cleaner — Limpeza de Dados e Contagem de Erros

Ferramenta em Python para limpeza, padronização e auditoria de arquivos de dados brutos (`.csv`, `.txt`, `.dat`).

Desenvolvida originalmente para dados de sensores meteorológicos e LiDAR, mas adaptável para qualquer fonte de dados estruturada.

---

## ⚙️ O que faz

- **Leitura automática** de arquivos `.csv`, `.txt` e `.dat` com detecção de separador
- **Remoção de valores nulos** com contagem de linhas afetadas
- **Padronização de datas** para o formato `DD/MM/AAAA HH:MM:SS`
- **Contagem de códigos de erro** em colunas numéricas (configurável)
- **Geração de relatório** `.txt` com resumo completo do processamento

---

## 📁 Estrutura

```
data_cleaner/
├── data_cleaner.py       # Script principal
├── requirements.txt      # Dependências
└── README.md
```

---

## 🚀 Como usar

### 1. Instale as dependências

```bash
pip install -r requirements.txt
```

### 2. Execute

```bash
python data_cleaner.py caminho/do/arquivo.csv
```

Com diretório de saída personalizado:

```bash
python data_cleaner.py caminho/do/arquivo.csv --output caminho/de/saida/
```

---

## 📊 Exemplo de relatório gerado

```
============================================================
        RELATÓRIO DE LIMPEZA DE DADOS
============================================================
  Arquivo        : medicao_janeiro.csv
  Processado em  : 08/04/2026 14:32:10
  Status         : ✅ CONCLUÍDO COM SUCESSO
============================================================

── RESUMO GERAL ─────────────────────────────────────────
  Linhas originais            : 8.640
  Linhas com nulos removidas  : 312
  Linhas após limpeza         : 8.328
  Colunas de data convertidas : timestamp

── CÓDIGOS DE ERRO ENCONTRADOS ──────────────────────────
  Coluna 'velocidade_vento' | Código -9999 (Dado ausente / sensor offline): 47 ocorrência(s)
  Coluna 'direcao_vento'    | Código -999  (Fora do range esperado): 12 ocorrência(s)

  Total de erros encontrados: 59

── ARQUIVO DE SAÍDA ─────────────────────────────────────
  medicao_janeiro_limpo.csv
============================================================
```

---

## 🔧 Configuração

No topo do `data_cleaner.py`, adapte conforme sua fonte de dados:

```python
# Códigos de erro do seu sistema ou sensor
ERROR_CODES = {
    -9999: "Dado ausente / sensor offline",
    -999:  "Fora do range esperado",
    -1:    "Falha de comunicação",
    9999:  "Overflow / leitura inválida",
}

# Nomes das colunas de data no seu arquivo
DATE_COLUMNS = ["timestamp", "data", "date", "datetime"]
```

---

## 📦 Dependências

```
pandas
numpy
```

---

## 🌱 Casos de uso

| Fonte de dados | Aplicação |
|---|---|
| Torres anemométricas | Validação de séries temporais de vento |
| Sensores LiDAR | Auditoria de campanhas de medição |
| Sistemas industriais | Detecção de falhas e anomalias |
| Qualquer CSV/TXT/DAT | Limpeza e padronização genérica |

---

## 👤 Autor

**Pedro Paulo Rodrigues Grassel**  
Electrical Engineering Undergraduate | Data Analysis & Automation  
[LinkedIn](https://linkedin.com/in/pedrograssel)
