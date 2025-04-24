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
from datetime import datetime
import pytz


# config supabase

load_dotenv()  # Isso lê o arquivo .env automaticamente
url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_KEY")

supabase = create_client(url, key)


# === inicio
data_str = datetime.now(pytz.timezone('America/Manaus')).strftime('%Y-%m-%d')
dominio = "https://mi.tv"
pais = "br"
rota = "canais"
canal = "canal-brasil"

# função para formatar data para a URL
def formatar_data(data):
    return data.strftime("%Y-%m-%d")

# função para extrair programas de um dia específico
def extrair_programas(driver, data_str):
    programas = []
    full_url = f"{dominio}/{pais}/{rota}/{canal}/{data_str}"
    driver.get(full_url)
    time.sleep(5)

    try:
        h1 = driver.find_element(By.TAG_NAME, "h1")
        dia_url = data_str.split("-")[-1]
        if dia_url not in h1.text:
            print(f"Dia no título não corresponde ao dia do link para {data_str}. Encerrando...")
            return None
        else:
            print(f"Dia validado com sucesso para {data_str}.")
    except Exception as e:
        print(f"Erro ao verificar o título para {data_str}:", e)
        return None

    # extrair os programas
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

######################## abrir chrome e fazer o scrap
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))

todos_programas = []

data_atual = datetime.strptime(data_str, "%Y-%m-%d")
for i in range(7):  
    data_str_formatada = formatar_data(data_atual)
    print(f"Extraindo dados para {data_str_formatada}...")

    programas_do_dia = extrair_programas(driver, data_str_formatada)

    if programas_do_dia is not None and len(programas_do_dia) > 0:
        todos_programas.extend(programas_do_dia)
        print(f"Programas extraídos para {data_str_formatada}.")
    else:
        print(f"Sem dados ou erro para {data_str_formatada}. Parando...")

    data_atual += timedelta(days=1)

driver.quit()

# === Converter para df ===
# df = pd.DataFrame(todos_programas)

# df.to_csv('dataraw.csv', index = False)

def existe_dados_para_data(supabase, tabela, data):
    """Verifica se já existem registros para uma data específica"""
    response = supabase.table(tabela).select('data').eq('data', data).limit(1).execute()
    return len(response.data) > 0

# novo bloco de inserção incremental
if todos_programas:
    df = pd.DataFrame(todos_programas)
    
    df['data'] = df['data'].astype(str)
    df['horario'] = df['horario'].astype(str)

    datas_para_inserir = []
    for data in df['data'].unique():
        if not existe_dados_para_data(supabase, 'canal_brasil', data):
            datas_para_inserir.append(data)
    
    if not datas_para_inserir:
        print("Todas as datas já foram processadas anteriormente.")
    else:
        df_novo = df[df['data'].isin(datas_para_inserir)]
        
        try:
            response = supabase.table('canal_brasil').insert(df_novo.to_dict('records')).execute()
            print(f"Dados inseridos para datas: {', '.join(datas_para_inserir)}")
            
        except APIError as e:
            print("Erro na inserção:", e.args[0]['message'])
