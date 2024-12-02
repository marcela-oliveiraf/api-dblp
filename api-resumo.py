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


# Função para obter resumo de um artigo
def get_description_from_doi(doi, api_key):
    url = f"https://api.elsevier.com/content/article/doi/{doi}?view=FULL&APIKey={api_key}&httpAccept=text/xml"
    response = requests.get(url)
    
    if response.status_code == 200:
        root = ET.fromstring(response.content)
        
        # Busque pela tag <dc:description> dentro do XML
        description = root.findall(".//dc:description", namespaces={'dc': 'http://purl.org/dc/elements/1.1/'})
        
        if description and description[0].text:
            return description[0].text.strip()
        else:
            # print(f"Resumo não encontrado para o DOI: {doi}")
            return None
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

# Função para atualizar ou inserir o resumo no banco de dados
def insert_or_update_summary_in_database(id_publicacao, resumo):
    connection = conexao_BD()
    cursor = connection.cursor()
    
    # Atualizar o resumo da publicação
    cursor.execute("UPDATE publicacao SET resumo_publicacao = %s WHERE id_publicacao = %s", (resumo, id_publicacao))
    
    connection.commit()
    cursor.close()

# Função principal
def main():
    api_key = "016b6de1c01c60c41172c9e1a68ba565"  # Substitua com sua chave de API Elsevier
    publications = get_all_dois_from_database()  # Obtém todos os DOIs da tabela
    
    for publication in publications:
        id_publicacao, doi = publication
        # print(f"Obtendo resumo para o DOI: {doi}")
        resumo = get_description_from_doi(doi, api_key)
        
        if resumo:
            insert_or_update_summary_in_database(id_publicacao, resumo)
            print("Resumo atualizado com sucesso no banco de dados!")
        else:
            print(f"Erro: Nenhum resumo encontrado para o DOI {doi}.")

if __name__ == "__main__":
    main()