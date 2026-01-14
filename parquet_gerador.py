import pandas as pd
import spacy
import re
import ast

nlp = spacy.load("pt_core_news_lg")

df = pd.read_csv(
    "csv_concatenado.csv",
    sep=",",
    engine="python",
    quotechar='"',
    escapechar="\\",
    on_bad_lines="skip"
)

df = df[df["conteudo"] != 0].reset_index(drop=True)


df = df[~df["url"].str.contains("especial-publicitario", na=False)].copy()

df["id_doc"] = df.index

def processar_conteudo(conteudo):
    if pd.isna(conteudo):
        return []

    conteudo = str(conteudo)
    conteudo = re.sub(r'\\(?!["\\\/bfnrtu])', r'\\\\', conteudo)

    try:
        lista = ast.literal_eval(conteudo)
    except (ValueError, SyntaxError):
        return []

    if not isinstance(lista, list) or len(lista) <= 2:
        return []

    lista = [str(x) for x in lista[2:]]
    texto = " ".join(lista)

    texto = re.sub(r"foto:.*?(?=\.)", "", texto, flags=re.IGNORECASE)
    texto = re.sub(r"\b\d{2}/\d{2}/\d{4}\b", "", texto)
    texto = texto.lower()
    texto = re.sub(r"\bpara\s+se\s+inscrever\b.*", "", texto)
    texto = re.sub(r"\bveja\s+tamb[eé]m\b.*", "", texto)

    doc = nlp(texto)
    
    palavras_limpas = [
        token.lemma_             
        for token in doc 
        if not token.is_stop      
        and not token.is_punct    
    ]

    return palavras_limpas

df["conteudo"] = df["conteudo"].apply(processar_conteudo)

df_frases = (
    df[["id_doc", "url", "titulo", "label", "conteudo"]]
    .explode("conteudo")
    .rename(columns={"conteudo": "frase"})
    .dropna()
    .reset_index(drop=True)
)

df_frases = df_frases[df_frases["frase"].str.len() > 3].reset_index(drop=True)


from sentence_transformers import SentenceTransformer
import numpy as np

modelo = SentenceTransformer(
    "lucas-leme/FinBERT-PT-BR"
    #"sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"
)

frases = df_frases["frase"].tolist()

embeddings = modelo.encode(
    frases,
    batch_size=512,
    normalize_embeddings=True,
    show_progress_bar=True
)

df_frases["embedding"] = list(embeddings)


df_frases.to_parquet(
    "dataset_frases_embeddings_lucas.parquet",
    index=False
)
