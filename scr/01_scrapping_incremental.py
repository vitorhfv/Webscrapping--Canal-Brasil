from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
from datetime import datetime, timedelta
from supabase import create_client
from postgrest.exceptions import APIError
import os
import pandas as pd
import time
import pytz


class CanalBrasilScraper:
    def __init__(self):
        load_dotenv()
        
        # configuracoes
        self.url = os.environ.get("SUPABASE_URL")
        self.key = os.environ.get("SUPABASE_KEY")
        self.supabase = create_client(self.url, self.key)
        
        self.dominio = "https://mi.tv"
        self.pais = "br"
        self.rota = "canais"
        self.canal = "canal-brasil"
        
        self.timezone = pytz.timezone('America/Manaus')
        self.data_atual = datetime.now(self.timezone).strftime('%Y-%m-%d')
        self.driver = None

    def formatar_data(self, data):
        """Formata data para a URL"""
        return data.strftime("%Y-%m-%d")
    
    def extrair_programas(self, data_str):
        """Extrai programas de um dia específico"""
        programas = []
        full_url = f"{self.dominio}/{self.pais}/{self.rota}/{self.canal}/{data_str}"
        self.driver.get(full_url)
        time.sleep(5)

        try:
            h1 = self.driver.find_element(By.TAG_NAME, "h1")
            dia_url = str(int(data_str.split("-")[-1])) 
            if dia_url not in h1.text:
                print(f"Dia no título não corresponde ao dia do link para {data_str}. Encerrando...")
                return None
            else:
                print(f"Dia validado com sucesso para {data_str}.")
        except Exception as e:
            print(f"Erro ao verificar o título para {data_str}:", e)
            return None

        # extrair os programas
        lis = self.driver.find_elements(By.XPATH, '//*[@id="listings"]/ul/li')
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
    
    def existe_dados_para_data(self, tabela, data):
        """Verifica se já existem registros para uma data específica"""
        response = self.supabase.table(tabela).select('data').eq('data', data).limit(1).execute()
        return len(response.data) > 0
    
    def iniciar_driver(self):
        """Inicializa o driver do Chrome"""
        options = webdriver.ChromeOptions()
        # coloca opções para rodar em ambiente de CI (GitHub Actions)
        if os.environ.get("CI") == "true":
            options.add_argument("--headless")
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            
        self.driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    
    def fechar_driver(self):
        """Fecha o driver"""
        if self.driver:
            self.driver.quit()
    
    def executar_raspagem(self, dias=7):
        """Executa a raspagem completa para o número de dias especificado"""
        todos_programas = []
        
        # inicializa o driver
        self.iniciar_driver()
        
        try:
            data_atual = datetime.strptime(self.data_atual, "%Y-%m-%d")
            for i in range(dias):
                data_str_formatada = self.formatar_data(data_atual)
                print(f"Extraindo dados para {data_str_formatada}...")

                programas_do_dia = self.extrair_programas(data_str_formatada)

                if programas_do_dia is not None and len(programas_do_dia) > 0:
                    todos_programas.extend(programas_do_dia)
                    print(f"Programas extraídos para {data_str_formatada}.")
                else:
                    print(f"Sem dados ou erro para {data_str_formatada}. Parando...")

                data_atual += timedelta(days=1)
        finally:
            self.fechar_driver()
            
        return todos_programas
    
    def inserir_dados_incrementalmente(self, dados):
        """Insere dados no Supabase apenas para datas que não existem"""
        if not dados:
            print("Sem dados para inserir.")
            return
            
        df = pd.DataFrame(dados)
        
        df['data'] = df['data'].astype(str)
        df['horario'] = df['horario'].astype(str)

        datas_para_inserir = []
        for data in df['data'].unique():
            if not self.existe_dados_para_data('canal_brasil', data):
                datas_para_inserir.append(data)
        
        if not datas_para_inserir:
            print("Todas as datas já foram processadas anteriormente.")
            return
            
        df_novo = df[df['data'].isin(datas_para_inserir)]
        
        try:
            response = self.supabase.table('canal_brasil').insert(df_novo.to_dict('records')).execute()
            print(f"Dados inseridos para datas: {', '.join(datas_para_inserir)}")
            return True
        except APIError as e:
            print("Erro na inserção:", e.args[0]['message'])
            return False
    
    def ultimos_n_dias(self, n=2):
        """Retorna lista com as datas dos últimos N dias"""
        hoje = datetime.now(self.timezone).date()
        return [(hoje + timedelta(days=i)).isoformat() for i in range(n)]

    def deletar_ultimos_n_dias(self, tabela, dias=2):
        """Deleta registros dos últimos N dias na tabela especificada"""
        datas = self.ultimos_n_dias(dias)
        print(f"Deletando dados dos últimos {dias} dias: {', '.join(datas)}")
        for data in datas:
            resp = self.supabase.table(tabela).delete().eq('data', data).execute()
            print(f"Registros deletados para {data}: {len(getattr(resp, 'data', []))}")
        return datas

    def inserir_dados_com_refresh(self, dados, dias_refresh=2):
        """Insere dados com refresh para os últimos N dias e incremental para demais datas"""
        if not dados:
            print("Sem dados para inserir.")
            return
            
        df = pd.DataFrame(dados)
        
        df['data'] = df['data'].astype(str)
        df['horario'] = df['horario'].astype(str)
        
        # 1. Deletar e reinserir dados dos últimos N dias
        datas_refresh = self.deletar_ultimos_n_dias('canal_brasil', dias_refresh)
        df_refresh = df[df['data'].isin(datas_refresh)]
        
        # 2. Verificar datas mais antigas para inserção incremental
        datas_antigas = [data for data in df['data'].unique() 
                        if data not in datas_refresh and 
                        not self.existe_dados_para_data('canal_brasil', data)]
        
        # Preparar DataFrame para inserção
        datas_para_inserir = datas_refresh + datas_antigas
        if not datas_para_inserir:
            print("Nenhum dado novo para inserir.")
            return
            
        df_para_inserir = df[df['data'].isin(datas_para_inserir)]
        
        try:
            response = self.supabase.table('canal_brasil').insert(df_para_inserir.to_dict('records')).execute()
            print(f"Dados inseridos para datas: {', '.join(datas_para_inserir)}")
            return True
        except APIError as e:
            print("Erro na inserção:", e.args[0]['message'])
            return False
    
    def executar_pipeline(self):
        """Executa o pipeline completo: raspagem e inserção com refresh para dados recentes"""
        print("Iniciando pipeline de dados do Canal Brasil...")
        dados = self.executar_raspagem()
        status = self.inserir_dados_com_refresh(dados, dias_refresh=2)  # Refresh para últimos 2 dias
        print("Pipeline finalizado!")
        return status


# p / quando executado diretamente (não importado)
if __name__ == "__main__":
    scraper = CanalBrasilScraper()
    scraper.executar_pipeline()
