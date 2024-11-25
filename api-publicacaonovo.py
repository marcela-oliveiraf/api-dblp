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
    url = f'https://dblp.org/search/publ/api?q={query}&h=1000&format=xml'
    response = requests.get(url)
    
    if response.status_code == 200:
        # Processar o XML da resposta
        tree = ET.ElementTree(ET.fromstring(response.text))
        root = tree.getroot()

        for hit in root.iter('hit'):
            authors = hit.find('.//authors')
            title = hit.find('.//title')
            venue = hit.find('.//venue')
            year = hit.find('.//year')
            access = hit.find('.//access')
            doi = hit.find('.//doi')
            ee = hit.find('.//ee')
             
            if authors is not None and title is not None and venue is not None and year is not None and access is not None and doi is not None and ee is not None:
                autor_lista = []
                # Extrair autores
                for author in authors.findall('.//author'):
                    autor_lista.append(author.text.strip())  # Remover espaços extras e adicionar à lista
                
                titulo = title.text
                local = venue.text
                ano = year.text
                acesso = access.text
                doi_publicacao = doi.text
                url_leitura = ee.text

                try:
                    # Verificar se publicação já existe com base no título ou DOI
                    cursor.execute("SELECT id_publicacao FROM publicacao WHERE titulo = %s OR doi_publicacao = %s", (titulo, doi_publicacao))
                    result = cursor.fetchone()

                    if result:
                        # Publicação já existe, então realiza o UPDATE
                        id_publicacao = result[0]
                        cursor.execute(""" 
                        UPDATE publicacao
                        SET titulo = %s, ano = %s, acesso = %s, doi_publicacao = %s, url_leitura = %s
                        WHERE id_publicacao = %s
                        """, (titulo, ano, acesso, doi_publicacao, url_leitura, id_publicacao))
                    else:
                        # Publicação não existe, então realiza o INSERT
                        cursor.execute(""" 
                        INSERT INTO publicacao (titulo, ano, acesso, doi_publicacao, url_leitura)
                        VALUES (%s, %s, %s, %s, %s)
                        """, (titulo, ano, acesso, doi_publicacao, url_leitura))
                        id_publicacao = cursor.lastrowid
                    
                    # Associar autores com a publicação
                    for nome_autor in autor_lista:
                        nome_autor = nome_autor.strip()  # Remover espaços extras
                        
                        # Verificar se o autor já existe no banco de dados
                        cursor.execute("SELECT id_autor FROM autor WHERE nome_autor = %s", (nome_autor,))
                        autor_result = cursor.fetchone()

                        if autor_result:
                            id_autor = autor_result[0]
                        else:
                            # Se o autor não existir, cria um novo
                            cursor.execute("INSERT INTO autor (nome_autor) VALUES (%s)", (nome_autor,))
                            id_autor = cursor.lastrowid
                        
                        # Verificar se o relacionamento autor-publicação já existe
                        cursor.execute("SELECT 1 FROM autor_publicacao WHERE id_autor = %s AND id_publicacao = %s", (id_autor, id_publicacao))
                        relacionamento_existente = cursor.fetchone()

                        if not relacionamento_existente:
                            # Se não existir, insira o novo relacionamento
                            cursor.execute("""
                                INSERT INTO autor_publicacao (id_autor, id_publicacao)
                                VALUES (%s, %s)
                            """, (id_autor, id_publicacao))

                    # Buscando informações de conferências
                    url = f'https://dblp.org/search/venue/api?q={query}&h=1000&format=xml'
                    response = requests.get(url)

                    if response.status_code == 200:
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

                                # Verificar se o local já existe no banco de dados
                                cursor.execute("SELECT id_local FROM local WHERE nome_local = %s", (nome_local,))
                                local_result = cursor.fetchone()

                                if not local_result:
                                    # Se o local não existir, insira um novo local
                                    cursor.execute("""
                                    INSERT INTO local (nome_local, acronimo, tipo, url)
                                    VALUES (%s, %s, %s, %s)
                                    """, (nome_local, acronimo, tipo, url_text))
                                    id_local = cursor.lastrowid
                                else:
                                    id_local = local_result[0]
                                    # Verificar se campos estão incompletos e atualizá-los
                                    cursor.execute("""
                                    SELECT acronimo, tipo, url FROM local WHERE id_local = %s
                                    """, (id_local,))
                                    existing_local = cursor.fetchone()

                                    # Atualizar somente campos que estão nulos ou vazios
                                    if not existing_local[0] or not existing_local[1] or not existing_local[2]:
                                        cursor.execute("""
                                        UPDATE local
                                        SET acronimo = COALESCE(%s, acronimo),
                                            tipo = COALESCE(%s, tipo),
                                            url = COALESCE(%s, url)
                                        WHERE id_local = %s
                                        """, (acronimo if acronimo else None, tipo if tipo else None, url_text if url_text else None, id_local))

                                # Associar o local à publicação
                                cursor.execute("""
                                    UPDATE publicacao
                                    SET id_local = %s
                                    WHERE id_publicacao = %s
                                """, (id_local, id_publicacao))
                        
                except Exception as e:
                    print(f"Erro ao processar publicação '{titulo}': {e}")
    else:
        print(f"Erro ao fazer a requisição para {query}: {response.status_code}")

# Função para gerenciar requisições com rate-limiting
def buscar_publicacao_com_rate_limiting(letras, cursor):
    query = ''.join(letras)
    retries = 0
    max_retries = 5
    delay = 1  # Delay inicial em segundos
    
    while retries < max_retries:
        try:
            # Fazendo a requisição
            response = requests.get(f'https://dblp.org/search/publ/api?q={query}&h=1000&format=xml')

            # Se a requisição for bem-sucedida (status 200)
            if response.status_code == 200:
                # Processar a resposta XML
                tree = ET.ElementTree(ET.fromstring(response.text))
                root = tree.getroot()
                
                for hit in root.iter('hit'):
                    authors = hit.find('.//authors')
                    title = hit.find('.//title')
                    venue = hit.find('.//venue')
                    year = hit.find('.//year')
                    access = hit.find('.//access')
                    doi = hit.find('.//doi')
                    ee = hit.find('.//ee')

                    if authors is not None and title is not None and venue is not None and year is not None and access is not None and doi is not None and ee is not None:
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
    print(f"Buscando publicações para as letras {', '.join(letras)}...")
    buscar_publicacao_com_rate_limiting(letras, cursor)

# Confirmar alterações e fechar conexão
conn.commit()
cursor.close()
conn.close()

print("Publicações inseridas/atualizadas com sucesso no banco de dados!")
