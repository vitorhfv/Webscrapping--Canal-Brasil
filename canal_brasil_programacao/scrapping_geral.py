from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
from datetime import datetime, timedelta
from supabase import create_client
import os
import pandas as pd
import time
import pytz


# configurando o supabase
load_dotenv()
url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_KEY")
supabase = create_client(url, key)

# exclui todos os dados existentes
print("Excluindo todos os registros existentes...")
supabase.table('canal_brasil').delete().neq('id', 0).execute()

# Parâmetros
data_inicial_str = "2025-01-01"  # Data inicial para repopulação
data_inicial = datetime.strptime(data_inicial_str, "%Y-%m-%d")
data_final = datetime.now()  # Data final é hoje

dominio = "https://mi.tv"
pais = "br"
rota = "canais"
canal = "canal-brasil"

def formatar_data(data):
    return data.astimezone(pytz.timezone('America/Manaus')).strftime("%Y-%m-%d")

def extrair_programas(self, data_str):
    programas = []
    full_url = f"{self.dominio}/{self.pais}/{self.rota}/{self.canal}/{data_str}"
    self.driver.get(full_url)
    time.sleep(5)
    # parseia a data da URL para extrair o dia
    data_obj = datetime.strptime(data_str, "%Y-%m-%d")
    dia_url = data_obj.day
    # formata o dia com zero à esquerda (1 -> 01)
    dia_url_str = f"{dia_url:02d}"

    try:
        h1 = self.driver.find_element(By.TAG_NAME, "h1")
        dia_url = data_str.split("-")[-1]
        if dia_url not in h1.text:
            print(f"Dia no título não corresponde ao dia do link para {data_str}. Encerrando...")
            return None
        else:
            print(f"Dia validado com sucesso para {data_str}.")
    except Exception as e:
        print(f"Erro ao verificar o título para {data_str}:", e)
        return None

    lis = driver.find_elements(By.XPATH, '//*[@id="listings"]/ul/li')
    for i, li in enumerate(lis, start=1):
        try:
            horario = li.find_element(By.CLASS_NAME, "time").text.strip()
            titulo = li.find_element(By.TAG_NAME, "h2").text.strip()
            genero_ano = li.find_element(By.XPATH, './/span[2]').text.strip()
            sinopse = li.find_element(By.TAG_NAME, "p").text.strip()

            programas.append({
                "titulo": titulo,
                "data": data_str,
                "horario": horario,
                "genero_ano": genero_ano,
                "sinopse": sinopse
            })
        except Exception as e:
            print(f"Erro ao extrair programa {i} para {data_str}:", e)

    return programas

# Abrir Chrome e fazer o scrap
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))

todos_programas = []

dias_total = (data_final.date() - data_inicial.date()).days + 1

for i in range(dias_total):
    data_atual = data_inicial + timedelta(days=i)
    data_str_formatada = formatar_data(data_atual)
    print(f"Extraindo dados para {data_str_formatada}...")

    programas_do_dia = extrair_programas(driver, data_str_formatada)

    if programas_do_dia is not None and len(programas_do_dia) > 0:
        todos_programas.extend(programas_do_dia)
        print(f"Programas extraídos para {data_str_formatada}.")
    else:
        print(f"Sem dados ou erro para {data_str_formatada}. Parando...")

driver.quit()

# fazendo o insert
if todos_programas:
    df = pd.DataFrame(todos_programas)
    df['data'] = df['data'].astype(str)
    df['horario'] = df['horario'].astype(str)
    # inserindo em lotes p/ evitar problemas com conjuntos grandes de dados
    batch_size = 100
    for i in range(0, len(df), batch_size):
        batch = df.iloc[i:i+batch_size]
        supabase.table('canal_brasil').insert(batch.to_dict('records')).execute()
        print(f"Lote {i//batch_size + 1} inserido com sucesso")
    print(f"Total de {len(df)} registros inseridos no Supabase.")
else:
    print("Nenhum dado para inserir.")
