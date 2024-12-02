import pymysql
import os
from extract_xml import *

password = os.getenv("DB_PASSWORD")

# Conectar ao banco de dados MySQL
conn = pymysql.connect(
    host="localhost",
    user="root",
    password=password,  # Atualize com sua senha
    database="sistema_publicacoes",
    port=3306
)
cursor = conn.cursor()

# Inserir publicações no banco de dados
for publicacao in publicacoes:
    try:
        # Verificar se a publicação já existe com base no DOI ou título
        if publicacao.get('doi_publicacao'):
            cursor.execute("SELECT id_publicacao FROM publicacao WHERE doi_publicacao = %s", (publicacao['doi_publicacao'],))
        else:
            cursor.execute("SELECT id_publicacao FROM publicacao WHERE titulo = %s", (publicacao['titulo'],))
        
        result = cursor.fetchone()
        if result:
            # A publicação já existe, obter o id_publicacao
            id_publicacao = result[0]

            # Atualizar os campos da publicação
            cursor.execute(""" 
            UPDATE publicacao
            SET titulo = %s, ano = %s, doi_publicacao = %s, acesso_tipo = %s, url_leitura = %s
            WHERE id_publicacao = %s
            """, (
                publicacao['titulo'],
                publicacao['ano'],
                publicacao.get('doi_publicacao'),
                publicacao['acesso_tipo'],
                publicacao['url_leitura'],
                id_publicacao
            ))

        else:
            # Inserir nova publicação
            cursor.execute("""
            INSERT INTO publicacao (titulo, ano, doi_publicacao, acesso_tipo, url_leitura)
            VALUES (%s, %s, %s, %s, %s)
            """, (
                publicacao['titulo'],
                publicacao['ano'],
                publicacao.get('doi_publicacao'),
                publicacao['acesso_tipo'],
                publicacao['url_leitura']
            ))
            id_publicacao = cursor.lastrowid  # Obter o ID da publicação recém-inserida

        # Inserir autores e associá-los à publicação
        for autor in publicacao['autores']:
            # Verificar se o autor já existe
            cursor.execute("SELECT id_autor FROM autor WHERE nome_autor = %s", (autor,))
            result = cursor.fetchone()
            if result:
                # Autor já existe
                id_autor = result[0]
            else:
                # Inserir novo autor
                cursor.execute("INSERT INTO autor (nome_autor) VALUES (%s)", (autor,))
                id_autor = cursor.lastrowid
            
            # Associar autor à publicação na tabela de relacionamento 'publicacao_autores'
            cursor.execute("""
            INSERT IGNORE INTO autor_publicacao (id_publicacao, id_autor)
            VALUES (%s, %s)
            """, (id_publicacao, id_autor))

        conn.commit()
        
    except Exception as e:
        print(f"Erro ao processar publicação '{publicacao['titulo']}': {e}")

# Confirmar alterações e fechar conexão
conn.commit()
cursor.close()
conn.close()

print("Publicações e autores inseridos/atualizados com sucesso no banco de dados!")
