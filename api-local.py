import os
import pymysql
import requests
import xml.etree.ElementTree as ET
import time

# Carregar a senha do banco de dados do ambiente
password = os.getenv("DB_PASSWORD")

# Conectar ao banco de dados MySQL
def connect_to_db():
    return pymysql.connect(
        host="localhost",
        user="root",
        password=password,  # Atualize com sua senha
        database="sistema_publicacoes",  # Seu banco de dados
        port=3306
    )

# Função para buscar e processar as conferências do DBLP
def fetch_and_store_dblp_data(query, cursor):
    url = f'https://dblp.org/search/venue/api?q={query}&h=1000&format=xml'
    response = requests.get(url)
    
    if response.status_code == 200:
        # Processar o XML da resposta
        tree = ET.ElementTree(ET.fromstring(response.text))
        root = tree.getroot()

        for hit in root.iter('hit'):
            venue = hit.find('.//venue')
            acronym = hit.find('.//acronym')
            type_ = hit.find('.//type')
            url_ = hit.find('.//url')

            if venue is not None and acronym is not None and type_ is not None and url_ is not None:
                nome_local = venue.text
                acronimo = acronym.text
                tipo = type_.text
                url_text = url_.text
                
                try:
                    # Verificar se o local já existe com base no acrônimo ou nome local
                    cursor.execute("SELECT id_local FROM local WHERE acronimo = %s OR nome_local = %s", (acronimo, nome_local))
                    result = cursor.fetchone()

                    if result:
                        # Local já existe, então realiza o UPDATE
                        id_local = result[0]
                        cursor.execute(""" 
                        UPDATE local
                        SET nome_local = %s, acronimo = %s, tipo = %s, url = %s
                        WHERE id_local = %s
                        """, (nome_local, acronimo, tipo, url_text, id_local))
                    else:
                        # Local não existe, então realiza o INSERT
                        cursor.execute(""" 
                        INSERT INTO local (nome_local, acronimo, tipo, url)
                        VALUES (%s, %s, %s, %s)
                        """, (nome_local, acronimo, tipo, url_text))
                        id_local = cursor.lastrowid
                except Exception as e:
                    print(f"Erro ao processar conferência '{nome_local}': {e}")
    else:
        print(f"Erro ao fazer a requisição para {query}: {response.status_code}")

# Função para gerenciar requisições com rate-limiting
def buscar_conferencias_com_rate_limiting(letras, cursor):
    query = ''.join(letras)
    url = f"https://dblp.org/search/venue/api?q={query}&h=1000&format=xml"  # Usar formato XML na URL
    retries = 0
    max_retries = 5
    delay = 1  # Delay inicial em segundos
    
    while retries < max_retries:
        try:
            # Fazendo a requisição
            response = requests.get(url)
            
            # Se a requisição for bem-sucedida (status 200)
            if response.status_code == 200:
                # Processar a resposta XML
                tree = ET.ElementTree(ET.fromstring(response.text))
                root = tree.getroot()
                
                # Para cada conferência encontrada, processar e inserir no banco
                for hit in root.iter('hit'):
                    venue = hit.find('.//venue')
                    acronym = hit.find('.//acronym')
                    type_ = hit.find('.//type')
                    url_ = hit.find('.//url')

                    if venue is not None and acronym is not None and type_ is not None and url_ is not None:
                        # Armazenar as publicações no banco de dados
                        fetch_and_store_dblp_data(query, cursor)
                break

            # Verificar cabeçalhos de rate-limiting
            remaining_requests = int(response.headers.get("X-RateLimit-Remaining", 0))
            reset_time = int(response.headers.get("X-RateLimit-Reset", time.time()))

            # Se o número de requisições restantes for 0, esperar até o reset
            if remaining_requests == 0:
                current_time = time.time()
                wait_time = reset_time - current_time + 1  # Esperar até o reset
                print(f"Limite de requisições atingido. Aguardando {wait_time} segundos até o reset.")
                time.sleep(wait_time)
            else:
                # Se ainda houver requisições permitidas, prosseguir
                print(f"Requisições restantes: {remaining_requests}. Continuando...")

            # Espera de 40 segundos entre as requisições
            time.sleep(40)
        
        except requests.exceptions.RequestException as e:
            print(f"Erro ao fazer requisição para as letras '{letras}': {e}")
            retries += 1
            print(f"Tentativa {retries}/{max_retries}. Tentando novamente...")
            time.sleep(delay)  # Pausa antes de tentar novamente
            delay *= 2  # Aumentar o delay em caso de erro para evitar sobrecarga
    
    if retries == max_retries:
        print("Máximo de tentativas atingido. Não foi possível realizar a requisição.")


# Conectar ao banco de dados
conn = connect_to_db()
cursor = conn.cursor()

# Definir grupos de letras para buscar juntas
letras_grupos = [
    ['A', 'B', 'C', 'D'],
    ['E', 'F', 'G', 'H'],
    ['I', 'J', 'K', 'L'],
    ['M', 'N', 'O', 'P'],
    ['Q', 'R', 'S', 'T'],
    ['U', 'V', 'W', 'X'],
    ['Y', 'Z']
]

# Realiza a busca para cada grupo de letras
for letras in letras_grupos:
    print(f"Buscando conferências para as letras {', '.join(letras)}...")
    buscar_conferencias_com_rate_limiting(letras, cursor)

# Confirmar alterações e fechar conexão
conn.commit()
cursor.close()
conn.close()

print("Conferências inseridas/atualizadas com sucesso no banco de dados!")
