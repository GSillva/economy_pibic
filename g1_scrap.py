import webbrowser
import urllib.parse
import sys
from selenium import webdriver
from bs4 import BeautifulSoup
import time
from collections import Counter
import urllib.parse
import requests
import pandas as pd
from datetime import datetime, timedelta
import re
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
import random
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import StaleElementReferenceException
from selenium.common.exceptions import NoSuchElementException

from selenium.webdriver.common.by import By
import time
import pandas as pd


from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
import time, random, re

import csv
import time
import random
import re
import urllib.parse
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from concurrent.futures import ThreadPoolExecutor, as_completed

def carregar_todos_widgets(driver):   


    SCROLL_PAUSE_TIME = 3

# Get scroll height
    last_height = driver.execute_script("return document.body.scrollHeight")
    count = 0
    while count<5:
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


def abrir_busca_g1(termo_formatado, data):
    
    
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
    options.add_argument("--incognito")  # modo anônimo

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
        label=1
        time.sleep(random.uniform(1, 2))  # não bombar servidor
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
            "label": label
        }

    except Exception as e:
        return {"url": url, "erro": str(e)}

    
def salvar_csv(termo, data, resultados):
    nome_arquivo = f"g1c-{termo}-{data}.csv"

    with open(nome_arquivo, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["url", "titulo", "subtitulo", "data", "conteudo"])

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

    print(f"📁 CSV salvo: {nome_arquivo}")

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


if __name__ == "__main__":
    termos = ["AXIA", "hidreletrica", "energia solar", "eolica", "comercio exterior", "presidente", "eletrica",]
    datas = read_datas('dados_treino_pos.csv')
    #datas = read_datas('dados_treino_neg.csv')
    for data in datas:
        for termo in termos:
            print(f"\n🔎 Buscando notícias sobre: {termo} durante {data}")
           # time.sleep(10)
            links = abrir_busca_g1(termo, data)
            if (len(links)<1):
                continue
            print(f"➡ {len(links)} links encontrados.")

            resultados = []

            with ThreadPoolExecutor(max_workers=4) as executor:
                futures = {executor.submit(processar_noticia, link): link for link in links}

                for future in as_completed(futures):
                    r = future.result()
                    resultados.append(r)
                    print("✔ Processado:", r.get("url", "??"))

            salvar_csv(termo, data, resultados)

    print("\n🏁 Finalizado!")

