import pymysql
from extract_xml import *


# Conectar ao banco de dados
conn = pymysql.connect(
    host="localhost",
    user="root",
    password="marcela00119m.",  # Atualize com a sua senha
    database="sistema_publicacoes",  # Nome do seu banco de dados
    port=3306
)
cursor = conn.cursor()

# 3. Inserir os dados no banco de dados
for autor in autores:
    try:
        sql = """
        INSERT INTO autor (id_autor, nome_autor)
        VALUES (%s, %s)
        ON DUPLICATE KEY UPDATE
        nome_autor = VALUES(nome_autor)
        """
        cursor.execute(sql, (autor["id_autor"], autor["nome_autor"]))
    except Exception as e:
        print(f"Erro ao inserir autor: {e}")

# 4. Confirmar alterações e fechar conexão
conn.commit()
cursor.close()
conn.close()

# Exibir os primeiros 10 autores como exemplo
print(f"Total de autores encontrados: {len(autores)}")
print("Exemplo de autores:", autores[:10])

print("Autores inseridos/atualizados com sucesso no banco de dados!")


