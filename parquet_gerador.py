# =========================
# PIPELINE FINAL – FINBERT-PT
# COM ACOMPANHAMENTO NO TERMINAL
# =========================

import pandas as pd
import re
import numpy as np
from sentence_transformers import SentenceTransformer
import ast

# -------------------------
# Leitura do dataset
# -------------------------
df = pd.read_csv(
    "csv_concatenado_shel.csv",
    sep=",",
    engine="python",
    quotechar='"',
    escapechar="\\",
    on_bad_lines="skip"
)

df = df[df["conteudo"].notna()].reset_index(drop=True)
df = df[~df["url"].str.contains("especial-publicitario", na=False)].copy()
df["id_doc"] = df.index

def limpar_conteudo(conteudo):
    if pd.isna(conteudo):
        return []

    conteudo = str(conteudo)

    # Corrige barras invertidas inválidas (\A, \R, \ )
    conteudo = re.sub(r'\\(?!["\\\/bfnrtu])', r'\\\\', conteudo)

    try:
        lista = ast.literal_eval(conteudo)
    except (ValueError, SyntaxError):
        return []

    if not isinstance(lista, list) or len(lista) <= 2:
        return []

    # --- remover as 2 primeiras strings ---
    lista = [str(x) for x in lista[2:]]

    # --- unir conteúdo ---
    texto = " ".join(lista)

    # --- remover trecho "Foto:" ---
    texto = re.sub(r"foto:.*?(?=\.)", "", texto, flags=re.IGNORECASE)
    texto = re.sub(r"\b\d{2}/\d{2}/\d{4}\b", "", texto)

    # --- lowercase ---
    texto = texto.lower()

    # --- remover tudo a partir de "veja também / veja tambem" ---
    texto = re.sub(r"\bpara\s+se\s+inscrever\b.*", "", texto)
    texto = re.sub(r"\bveja\s+tamb[eé]m\b.*", "", texto)
    texto = re.sub(r"\bvídeos\b.*", "", texto)
    texto = re.sub(r"\be-mail\b.*", "", texto)

    # --- normalizar espaços ---
    texto = re.sub(r"\s+", " ", texto).strip()

    # --- dividir em frases ---
    frases = [f.strip() for f in texto.split(" ") if f.strip()]

    return texto

# --- aplicar ---
df.loc[:, "conteudo"] = df["conteudo"].apply(limpar_conteudo)
print(f"Total de notícias carregadas: {len(df)}")

# -------------------------
# Limpeza mínima (BERT-friendly)
# -------------------------
def limpar_texto_bert(texto):
    texto = str(texto)

    texto = re.sub(r"foto:.*?(?=\.)", "", texto, flags=re.IGNORECASE)
    texto = re.sub(r"\bveja\s+tamb[eé]m\b.*", "", texto, flags=re.IGNORECASE)
    texto = re.sub(r"\bsaiba\s+mais\b.*", "", texto, flags=re.IGNORECASE)

    texto = re.sub(r"\s+", " ", texto).strip()
    return texto

# -------------------------
# Chunking para BERT
# -------------------------
def quebrar_em_chunks(texto, max_tokens=300):
    palavras = texto.split()
    chunks = []

    for i in range(0, len(palavras), max_tokens):
        chunk = " ".join(palavras[i:i + max_tokens])
        if len(chunk) > 50:
            chunks.append(chunk)

    return chunks

# -------------------------
# Processamento do conteúdo
# -------------------------
def processar_conteudo(conteudo):
    texto = limpar_texto_bert(conteudo)
    return quebrar_em_chunks(texto)

df["paragrafos"] = df["conteudo"].apply(processar_conteudo)
df = df[df["paragrafos"].map(len) > 0].reset_index(drop=True)

print(f"Notícias após processamento: {len(df)}")

# -------------------------
# Modelo FinBERT-PT
# -------------------------
modelo = SentenceTransformer("sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2")#SentenceTransformer("lucas-leme/FinBERT-PT-BR")

# -------------------------
# Embedding por notícia
# (com print de progresso)
# -------------------------
def embedding_por_noticia(paragrafos, idx, total):
    embeddings = modelo.encode(
        paragrafos,
        batch_size=512,
        normalize_embeddings=True,
        show_progress_bar=False
    )

    print(f"[{idx+1}/{total}] notícia processada ({len(paragrafos)} chunks)")

    return np.mean(embeddings, axis=0)

# -------------------------
# Gerar embeddings finais
# -------------------------
total = len(df)

df["embedding"] = [
    embedding_por_noticia(paragrafos, i, total)
    for i, paragrafos in enumerate(df["paragrafos"])
]

# -------------------------
# Salvar dataset final
# -------------------------
df[[
    "id_doc",
    "label",
    "url",
    "conteudo",
    "termo",
    
    "embedding"
]].to_parquet(
    "dataset_shell_embeddings_finbert.parquet",
    index=False
)

print("Processamento concluído com sucesso.")
