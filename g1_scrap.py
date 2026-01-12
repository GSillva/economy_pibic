import csv
import random
import re
import time
import urllib.parse
from collections import defaultdict
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import requests
from bs4 import BeautifulSoup

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait

import os
def carregar_todos_widgets(driver):   


    SCROLL_PAUSE_TIME = 3

# Get scroll height
    last_height = driver.execute_script("return document.body.scrollHeight")
    count = 0
    while count<3:
        # Scroll down to bottom
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")

        # Wait to load page
        time.sleep(SCROLL_PAUSE_TIME)

        # Calculate new scroll height and compare with last scroll height
        new_height = driver.execute_script("return document.body.scrollHeight")
        if new_height == last_height:
            break
        last_height = new_height 
        count+=1    

    
    items = driver.find_elements(By.CSS_SELECTOR, "li.widget")
    return items


def abrir_busca_g1( termo_formatado, data):
    
    
    termo_codificado = urllib.parse.quote_plus(termo_formatado)
    data_dt = datetime.strptime(data, "%Y-%m-%d").date()

    # soma 1 dia
    data_mais_um = data_dt + timedelta(days=1)

    # volta para string no mesmo formato
    data_mais_um_str = data_mais_um.strftime("%Y-%m-%d")
    
    URL_BASE = "https://g1.globo.com/busca/?q={}&from={}T03%3A00%3A00.000Z&to={}T02%3A59%3A59.999Z"
    #"https://g1.globo.com/busca/?q={}&species=noticias&from={}T03%3A00%3A00.000Z&to=2025-01-01T02%3A59%3A59.999Z"

    url_final = URL_BASE.format(termo_codificado, data, data_mais_um_str)
    options = Options()
    options.add_argument("--incognito")

    driver = webdriver.Chrome(options=options)
    
    driver.get(url_final)
    wait = WebDriverWait(driver, 30)
    '''first_result = wait.until(
    EC.presence_of_element_located((By.CSS_SELECTOR, "li.widget"))
    )'''

    items = carregar_todos_widgets(driver) #items = driver.find_elements(By.CSS_SELECTOR, "li.widget")#carregar_todos_widgets(driver)
    links = []
    total = len(items)
    for i in range(len(items)):
        a = items[i].find_element(By.CSS_SELECTOR, "a")
        links.append(a.get_attribute("href"))
    
    driver.quit()

    return links
    


def processar_noticia(url):
    spam = ['De segunda a sábado, as notícias que você não pode perder diretamente no seu e-mail.', 'Para se inscrever, entre ou crie uma conta Globo gratuita.', 'O podcast O Assunto é produzido por', 'Receba no WhatsApp as notícias d']
    ignoraveis =['VEJA TAMBÉM','LEIA TAMBÉM','LEIA MAIS:']
    headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
    }
    try:
        
        #time.sleep(random.uniform(1, 2))  # não bombar servidor
        r = requests.get(url, timeout=10, headers=headers)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")

        # Título
        title_el = soup.select_one("h1.content-head__title")
        title = title_el.get_text(strip=True) if title_el else ""

        # Subtítulo
        subtitle_el = soup.select_one("h2.content-head__subtitle")
        subtitle = subtitle_el.get_text(strip=True) if subtitle_el else ""

        # Parágrafos
        paragraphs = []
        ignorar = False
        d = True
        data_found = ""

        for el in soup.select("p, li"):
            texto = el.get_text(strip=True)

            if any(s in texto for s in ignoraveis):
                ignorar = True
                continue

            if not texto or any(s in texto.lower() for s in spam):
                continue

            if ignorar:
                if el.name == "p":
                    ignorar = False
                    paragraphs.append(texto)
                continue

            if d:
                m = re.search(r"\b\d{2}/\d{2}/\d{4}\b", texto)
                if m:
                    data_found = m.group()
                    d = False

            paragraphs.append(texto)

        return {
            "url": url,
            "titulo": title,
            "subtitulo": subtitle,
            "data": data_found,
            "conteudo": paragraphs,
            "label": 1
        }

    except Exception as e:
        return {"url": url, "erro": str(e)}

    
def salvar_csv(termo, data, resultados):
    nome_arquivo = f"g1c-{termo}-{data}.csv"
    caminho_arquivo = os.path.join("csvspos", nome_arquivo)
    with open(caminho_arquivo, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["url", "titulo", "subtitulo", "data", "conteudo", "label"])

        for r in resultados:
            if "erro" in r:
                continue
            writer.writerow([
                r["url"],
                r["titulo"],
                r["subtitulo"],
                r["data"],
                r["conteudo"],
                r["label"]
            ])

    print(f"📁 CSV salvo: {caminho_arquivo}")

def read_datas(path_csv):
    df_pos = pd.read_csv(
    path_csv, 
    sep=';', 
    header=0, 
    skiprows=[1, 2], 
    index_col=0, 
    parse_dates=True
    )

    lista_datas = []

# Iterar sobre o DataFrame
    for data_atual in df_pos.index:
        data_anterior = data_atual - pd.Timedelta(days=1)
        lista_datas.append(f"{data_atual.strftime('%Y-%m-%d')}")
        lista_datas.append(f"{data_anterior.strftime('%Y-%m-%d')}")
    
    return lista_datas

def abrir_busca_g1_requests(termo_formatado, data, max_pages=10):
    termo_codificado = urllib.parse.quote_plus(termo_formatado)
    data_dt = datetime.strptime(data, "%Y-%m-%d").date()
    data_mais_um = data_dt + timedelta(days=1)

    data_inicio = f"{data}T03%3A00%3A00.000Z"
    data_fim = f"{data_mais_um.strftime('%Y-%m-%d')}T02%3A59%3A59.999Z"

    headers = {
        "User-Agent": "Mozilla/5.0"
    }

    links = set()

    for page in range(1, max_pages + 1):
        url = (
            "https://g1.globo.com/busca/"
            f"?q={termo_codificado}"
            f"&from={data_inicio}"
            f"&to={data_fim}"
            f"&page={page}"
        )

        r = requests.get(url, headers=headers, timeout=2)
        if r.status_code != 200:
            break

        soup = BeautifulSoup(r.text, "html.parser")
        itens = soup.select("li.widget a")

        if not itens:
            break

        for a in itens:
            href = a.get("href")
            if href:
                links.add(href)

        time.sleep(random.uniform(0.5, 1.2))

    return list(links)


def tarefa_busca_termo_data(termo, data):
    print(f"\n🔎 Buscando notícias sobre: {termo} durante {data}")

    links = abrir_busca_g1( termo, data)
    #abrir_busca_g1

    if not links:
        print(f"⚠ Nenhum link para {termo} em {data}")
        return

    #print(f"➡ {len(links)} links encontrados.")

    resultados = []

    for link in links:
        r = processar_noticia(link)
        resultados.append(r)
        print("✔ Processado:", r.get("url", "??"))

    salvar_csv(termo, data, resultados)

if __name__ == "__main__":
    termos = [
        "AXIA energia", "hidreletrica", "energia solar",
        "eolica", "comercio exterior", "presidente", "eletrica"
    ]

    datas = read_datas('dados_treino_pos.csv')

    print(f"🚀 Processando {len(datas)} datas em paralelo")

    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = [
            executor.submit(tarefa_busca_termo_data, termo, data)
            for data in datas
            for termo in termos
        ]

        for future in as_completed(futures):
            future.result()  # força exceções aparecerem

    print("\n🏁 Finalizado!")
