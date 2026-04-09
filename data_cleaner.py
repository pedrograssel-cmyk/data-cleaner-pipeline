"""
data_cleaner.py
---------------
Ferramenta de limpeza de dados e contagem de códigos de erro.
Suporta arquivos .csv, .txt e .dat

Autor: Pedro Paulo Rodrigues Grassel
"""

import pandas as pd
import numpy as np
import os
import argparse
from datetime import datetime


# ─────────────────────────────────────────────
# CONFIGURAÇÕES — adapte conforme sua fonte
# ─────────────────────────────────────────────

# Códigos de erro conhecidos (sistema ou sensores)
ERROR_CODES = {
    -9999: "Dado ausente / sensor offline",
    -999:  "Fora do range esperado",
    -1:    "Falha de comunicação",
    9999:  "Overflow / leitura inválida",
    0:     "Sinal zerado (possível falha)",
}

# Colunas que contêm timestamps (adapte conforme seu arquivo)
DATE_COLUMNS = ["timestamp", "data", "date", "datetime", "hora", "time"]

# Separadores a tentar na leitura
SEPARATORS = [",", ";", "\t", "|", " "]


# ─────────────────────────────────────────────
# LEITURA DO ARQUIVO
# ─────────────────────────────────────────────

def load_file(filepath: str) -> pd.DataFrame:
    """Carrega .csv, .txt ou .dat testando separadores automaticamente."""
    ext = os.path.splitext(filepath)[1].lower()

    if ext not in [".csv", ".txt", ".dat"]:
        raise ValueError(f"Formato não suportado: {ext}. Use .csv, .txt ou .dat")

    for sep in SEPARATORS:
        try:
            df = pd.read_csv(filepath, sep=sep, engine="python", encoding="utf-8")
            if df.shape[1] > 1:
                print(f"  Arquivo lido com separador: '{sep}'")
                return df
        except Exception:
            continue

    # Fallback: tenta utf-8-sig (BOM) e latin-1
    for enc in ["utf-8-sig", "latin-1"]:
        try:
            df = pd.read_csv(filepath, sep=None, engine="python", encoding=enc)
            if df.shape[1] > 1:
                print(f"  Arquivo lido com encoding: '{enc}'")
                return df
        except Exception:
            continue

    raise ValueError("Não foi possível ler o arquivo. Verifique o formato e separador.")


# ─────────────────────────────────────────────
# PADRONIZAÇÃO DE DATAS
# ─────────────────────────────────────────────

def standardize_dates(df: pd.DataFrame) -> tuple[pd.DataFrame, list]:
    """Detecta colunas de data e padroniza para DD/MM/AAAA HH:MM:SS."""
    converted = []

    for col in df.columns:
        if col.strip().lower() in DATE_COLUMNS:
            try:
                df[col] = pd.to_datetime(df[col], infer_datetime_format=True, dayfirst=True)
                df[col] = df[col].dt.strftime("%d/%m/%Y %H:%M:%S")
                converted.append(col)
            except Exception:
                pass  # Coluna não é data válida, ignora

    return df, converted


# ─────────────────────────────────────────────
# LIMPEZA DE NULOS
# ─────────────────────────────────────────────

def remove_nulls(df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    """Remove linhas com valores nulos e retorna quantas foram removidas."""
    before = len(df)
    df = df.dropna()
    after = len(df)
    return df, before - after


# ─────────────────────────────────────────────
# CONTAGEM DE CÓDIGOS DE ERRO
# ─────────────────────────────────────────────

def count_error_codes(df: pd.DataFrame) -> dict:
    """
    Varre colunas numéricas buscando códigos de erro definidos em ERROR_CODES.
    Retorna dicionário com contagem por código e coluna.
    """
    results = {}

    numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()

    for col in numeric_cols:
        for code, description in ERROR_CODES.items():
            count = (df[col] == code).sum()
            if count > 0:
                key = f"Coluna '{col}' | Código {code} ({description})"
                results[key] = int(count)

    return results


# ─────────────────────────────────────────────
# GERAÇÃO DO RELATÓRIO
# ─────────────────────────────────────────────

def generate_report(
    filepath: str,
    original_rows: int,
    nulls_removed: int,
    date_columns: list,
    error_counts: dict,
    output_path: str,
    success: bool,
) -> None:
    """Gera relatório .txt com resumo do processamento."""

    now = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    total_errors = sum(error_counts.values())
    clean_rows = original_rows - nulls_removed

    lines = [
        "=" * 60,
        "        RELATÓRIO DE LIMPEZA DE DADOS",
        "=" * 60,
        f"  Arquivo        : {os.path.basename(filepath)}",
        f"  Processado em  : {now}",
        f"  Status         : {'✅ CONCLUÍDO COM SUCESSO' if success else '⚠️  CONCLUÍDO COM AVISOS'}",
        "=" * 60,
        "",
        "── RESUMO GERAL ─────────────────────────────────────────",
        f"  Linhas originais         : {original_rows}",
        f"  Linhas com nulos removidas: {nulls_removed}",
        f"  Linhas após limpeza      : {clean_rows}",
        f"  Colunas de data convertidas: {', '.join(date_columns) if date_columns else 'Nenhuma detectada'}",
        "",
        "── CÓDIGOS DE ERRO ENCONTRADOS ──────────────────────────",
    ]

    if error_counts:
        for description, count in error_counts.items():
            lines.append(f"  {description}: {count} ocorrência(s)")
        lines.append(f"\n  Total de erros encontrados: {total_errors}")
    else:
        lines.append("  Nenhum código de erro encontrado.")

    lines += [
        "",
        "── ARQUIVO DE SAÍDA ─────────────────────────────────────",
        f"  {output_path}",
        "",
        "=" * 60,
    ]

    report_path = output_path.replace(".csv", "_relatorio.txt")
    with open(report_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    print("\n".join(lines))
    print(f"\n  Relatório salvo em: {report_path}")


# ─────────────────────────────────────────────
# PIPELINE PRINCIPAL
# ─────────────────────────────────────────────

def run(filepath: str, output_dir: str = None) -> None:
    print(f"\n{'='*60}")
    print(f"  Iniciando processamento: {filepath}")
    print(f"{'='*60}\n")

    # 1. Leitura
    print("[1/4] Lendo arquivo...")
    df = load_file(filepath)
    original_rows = len(df)
    print(f"  {original_rows} linhas carregadas | {df.shape[1]} colunas")

    # 2. Remoção de nulos
    print("\n[2/4] Removendo valores nulos...")
    df, nulls_removed = remove_nulls(df)
    print(f"  {nulls_removed} linhas removidas")

    # 3. Padronização de datas
    print("\n[3/4] Padronizando datas...")
    df, date_columns = standardize_dates(df)
    if date_columns:
        print(f"  Colunas convertidas: {', '.join(date_columns)}")
    else:
        print("  Nenhuma coluna de data detectada automaticamente.")
        print("  Dica: adicione o nome da sua coluna de data em DATE_COLUMNS no topo do script.")

    # 4. Contagem de erros
    print("\n[4/4] Verificando códigos de erro...")
    error_counts = count_error_codes(df)
    total_errors = sum(error_counts.values())
    print(f"  {total_errors} ocorrência(s) de erro encontradas")

    # Saída
    base_name = os.path.splitext(os.path.basename(filepath))[0]
    out_dir = output_dir or os.path.dirname(filepath) or "."
    output_path = os.path.join(out_dir, f"{base_name}_limpo.csv")
    df.to_csv(output_path, index=False, encoding="utf-8-sig")

    success = nulls_removed >= 0  # sempre True, mas adaptável
    generate_report(filepath, original_rows, nulls_removed, date_columns, error_counts, output_path, success)
    print(f"\n  Arquivo limpo salvo em: {output_path}\n")


# ─────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Limpeza de dados e contagem de erros.")
    parser.add_argument("filepath", help="Caminho do arquivo (.csv, .txt ou .dat)")
    parser.add_argument("--output", help="Diretório de saída (opcional)", default=None)
    args = parser.parse_args()

    run(args.filepath, args.output)
