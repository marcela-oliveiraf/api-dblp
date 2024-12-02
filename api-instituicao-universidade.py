# import requests
# import xml.etree.ElementTree as ET
# import pymysql
# import os

# # Carregar a senha do banco de dados do ambiente
# password = os.getenv("DB_PASSWORD")

# # Conexão ao banco de dados
# def conexao_BD():
#     connection = pymysql.connect(
#         host="localhost",
#         user="root",
#         password=password,  # A senha carregada do ambiente
#         database="sistema_publicacoes",  # Seu banco de dados
#         port=3306
#     )
#     return connection

# # Função para obter afiliações de um artigo pelo DOI
# def get_affiliations_from_doi(doi, api_key):
#     url = f"https://api.elsevier.com/content/article/doi/{doi}?view=FULL&APIKey={api_key}&httpAccept=text/xml"
#     response = requests.get(url)

#     if response.status_code == 200:
#         root = ET.fromstring(response.content)
        
#         # Buscar as tags <sa:organization> no XML
#         namespaces = {'sa': 'http://www.elsevier.com/xml/svapi/abstract/dtd'}
#         affiliations = []
        
#         for affiliation in root.findall(".//sa:affiliation", namespaces):
#             organizacoes = affiliation.findall(".//sa:organization", namespaces)
#             if len(organizacoes) >= 2:
#                 instituto = organizacoes[0].text.strip() if organizacoes[0].text else None
#                 universidade = organizacoes[1].text.strip() if organizacoes[1].text else None
#                 affiliations.append((instituto, universidade))
        
#         return affiliations
#     else:
#         print(f"Erro {response.status_code}: {response.text}")
#         return []

# # Função para obter autores de uma publicação no banco de dados
# def get_authors_by_publication(id_publicacao):
#     connection = conexao_BD()
#     cursor = connection.cursor()
#     cursor.execute("""
#         SELECT a.id_autor, a.nome_autor
#         FROM autor_publicacao ap
#         JOIN autor a ON ap.id_autor = a.id_autor
#         WHERE ap.id_publicacao = %s
#     """, (id_publicacao,))
#     authors = cursor.fetchall()  # [(id_autor1, nome_autor1), ...]
#     connection.close()
#     return authors

# # Função para atualizar afiliações no banco de dados
# def update_author_affiliations(id_autor, instituto, universidade):
#     connection = conexao_BD()
#     cursor = connection.cursor()
    
#     cursor.execute("""
#         UPDATE autor
#         SET instituto_autor = %s, universidade_autor = %s
#         WHERE id_autor = %s
#     """, (instituto, universidade, id_autor))
    
#     connection.commit()
#     cursor.close()

# # Função principal
# def main():
#     api_key = "016b6de1c01c60c41172c9e1a68ba565"  # Substitua pela sua chave de API Elsevier
#     connection = conexao_BD()
#     cursor = connection.cursor()
    
#     # Obter todas as publicações com DOIs
#     cursor.execute("SELECT id_publicacao, doi_publicacao FROM publicacao")
#     publicacoes = cursor.fetchall()  # [(id_publicacao, doi), ...]

#     for id_publicacao, doi in publicacoes:
#         print(f"Processando afiliações para o DOI: {doi}")
        
#         # Obter afiliações da Elsevier API
#         affiliations = get_affiliations_from_doi(doi, api_key)
        
#         # Obter autores relacionados à publicação
#         authors = get_authors_by_publication(id_publicacao)
        
#         # Atualizar autores com base nas afiliações
#         for i, (id_autor, nome_autor) in enumerate(authors):
#             if i < len(affiliations):
#                 instituto, universidade = affiliations[i]
#                 update_author_affiliations(id_autor, instituto, universidade)
#                 print(f"Autor {nome_autor} atualizado com instituição: {instituto}, universidade: {universidade}")
#             else:
#                 print(f"Afiliações insuficientes para o autor {nome_autor}.")

#     connection.close()

# if __name__ == "__main__":
#     main()

import requests
import xml.etree.ElementTree as ET
import pymysql
import os

# Carregar a senha do banco de dados do ambiente
password = os.getenv("DB_PASSWORD")

# Conexão ao banco de dados
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

# Função para obter afiliações de um artigo pelo DOI
def get_affiliations_from_doi(doi, api_key):
    url = f"https://api.elsevier.com/content/article/doi/{doi}?view=FULL&APIKey={api_key}&httpAccept=text/xml"
    response = requests.get(url)

    if response.status_code == 200:
        root = ET.fromstring(response.content)
        root = remove_namespace(root)

        # Afiliações serão armazenadas em uma lista
        affiliations = []
        
        # Buscar todas as tags <affiliation> 
        for affiliation in root.findall(".//affiliation"):  # Buscar a tag <affiliation> diretamente
            if affiliation is not None:
                textfn = affiliation.find(".//textfn")  # Verifica o subelemento <textfn> dentro de <affiliation>
                if textfn is not None and textfn.text:
                    text = textfn.text.strip()
                    if ',' in text:
                        instituto, universidade = text.split(',', 1)
                        instituto = instituto.strip()
                        universidade = universidade.strip()
                        affiliations.append((instituto, universidade))
                        print(f"Instituição: {instituto}, Universidade: {universidade}")
        
        return affiliations
    
    else:
        print(f"Erro {response.status_code}: {response.text}")
        return []

# Função para obter autores de uma publicação no banco de dados
def get_authors_by_publication(id_publicacao):
    connection = conexao_BD()
    cursor = connection.cursor()
    cursor.execute("""
        SELECT a.id_autor, a.nome_autor
        FROM autor_publicacao ap
        JOIN autor a ON ap.id_autor = a.id_autor
        WHERE ap.id_publicacao = %s
    """, (id_publicacao,))
    authors = cursor.fetchall()  # [(id_autor1, nome_autor1), ...]
    connection.close()
    print(f"Autores encontrados: {authors}")  # Verificar o que foi retornado pelos autores
    return authors

# Função para atualizar afiliações no banco de dados
def update_author_affiliations(id_autor, instituto, universidade):
    connection = conexao_BD()
    cursor = connection.cursor()
    
    cursor.execute("""
        UPDATE autor
        SET instituto_autor = %s, universidade_autor = %s
        WHERE id_autor = %s
    """, (instituto, universidade, id_autor))
    
    connection.commit()
    print(f"Autor com id {id_autor} atualizado com instituição: {instituto}, universidade: {universidade}")  # Verificar a atualização
    cursor.close()

# Função principal para o teste
def main():
    api_key = "016b6de1c01c60c41172c9e1a68ba565"  # Substitua pela sua chave de API Elsevier
    doi = "10.1016/J.NEUROIMAGE.2022.119215"  # DOI de teste
    id_publicacao = 26  # Substitua pelo ID correto da publicação no banco

    # Obter afiliações do DOI
    print(f"Obtendo afiliações para o DOI: {doi}")
    affiliations = get_affiliations_from_doi(doi, api_key)
    print("Afiliações encontradas:", affiliations)

    # Obter autores da publicação
    authors = get_authors_by_publication(id_publicacao)
    print("Autores encontrados:", authors)

    # Atualizar autores com base nas afiliações
    for i, (id_autor, nome_autor) in enumerate(authors):
        if i < len(affiliations):
            instituto, universidade = affiliations[i]
            update_author_affiliations(id_autor, instituto, universidade)
            print(f"Autor {nome_autor} atualizado com instituição: {instituto}, universidade: {universidade}")
        else:
            print(f"Afiliações insuficientes para o autor {nome_autor}.")

if __name__ == "__main__":
    main()
