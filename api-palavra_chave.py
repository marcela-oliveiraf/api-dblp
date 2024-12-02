import requests
import xml.etree.ElementTree as ET
import pymysql
import os

# Carregar a senha do banco de dados do ambiente
password = os.getenv("DB_PASSWORD")

def conexao_BD():
    connection = pymysql.connect(
        host="localhost",
        user="root",
        password=password,  # A senha carregada do ambiente
        database="sistema_publicacoes",  # Seu banco de dados
        port=3306
    )
    return connection

# Função para remover namespaces
def remove_namespace(xml):
    for elem in xml.iter():
        if '}' in elem.tag:
            elem.tag = elem.tag.split('}', 1)[1]  # Remove o namespace
    return xml

# Função para obter as palavras-chave de um artigo
def get_keywords_from_doi(doi, api_key):
    url = f"https://api.elsevier.com/content/article/doi/{doi}?view=FULL&APIKey={api_key}&httpAccept=text/xml"
    response = requests.get(url)
    
    if response.status_code == 200:
        root = ET.fromstring(response.content)
        root = remove_namespace(root)
        
        # Busque pelas tags <subject> dentro de <coredata>
        subjects = root.findall(".//coredata//subject")
        keywords = [subject.text for subject in subjects]
        return keywords
    else:
        # print(f"Erro {response.status_code}: {response.text}")
        return None

# Função para obter todos os DOIs e ids_publicacao da tabela publicacao
def get_all_dois_from_database():
    connection = conexao_BD()
    
    cursor = connection.cursor()
    cursor.execute("SELECT id_publicacao, doi_publicacao FROM publicacao")  
    result = cursor.fetchall()  # Pega todos os resultados
    connection.close()
    
    return result  # Retorna uma lista de tuplas (id_publicacao, doi)

# Função para verificar se uma palavra-chave já existe para um artigo
def check_if_keyword_exists(id_publicacao, keyword):
    connection = conexao_BD()
    cursor = connection.cursor()
    cursor.execute("SELECT COUNT(*) FROM palavra_chave WHERE id_publicacao = %s AND palavra_chave = %s", (id_publicacao, keyword))
    count = cursor.fetchone()[0]
    connection.close()
    return count > 0  # Retorna True se a palavra-chave já existir

# Função para atualizar ou inserir palavras-chave no banco de dados
def insert_or_update_keywords_to_database(id_publicacao, keywords):
    connection = conexao_BD()
    cursor = connection.cursor()
    
    for keyword in keywords:
        
        # Tinha umas palavras-chave bug. Coloquei para evitar
        if len(keyword) != 50:
            keyword is None
            break

        if check_if_keyword_exists(id_publicacao, keyword):
            print(f"A palavra-chave '{keyword}' já existe. Atualizando...")
            # Se já existe, podemos atualizar se necessário
            # Exemplo de update, caso precise de algo mais específico
            # cursor.execute("UPDATE palavra_chave SET palavra_chave = %s WHERE id_publicacao = %s AND palavra_chave = %s", (new_keyword, id_publicacao, keyword))
        else:
            print(f"Inserindo palavra-chave '{keyword}'")
            cursor.execute("INSERT INTO palavra_chave (id_publicacao, palavra_chave) VALUES (%s, %s)", (id_publicacao, keyword))
    
    connection.commit()
    cursor.close()

# Função principal
def main():
    api_key = "016b6de1c01c60c41172c9e1a68ba565"
    publications = get_all_dois_from_database()  # Obtém todos os DOIs da tabela
    
    for publication in publications:
        id_publicacao, doi = publication
        # print(f"Obtendo palavras-chave para o DOI: {doi}")
        keywords = get_keywords_from_doi(doi, api_key)
        
        if keywords:
            print(f"Palavras-chave encontradas: {keywords}")
            insert_or_update_keywords_to_database(id_publicacao, keywords)
            print("Palavras-chave processadas com sucesso no banco de dados!")
        else:
            print(f"Erro: Nenhuma palavra-chave encontrada para o DOI {doi}.")

if __name__ == "__main__":
    main()
