import requests
import pymysql

# Função para buscar metadados a partir do DOI
def buscar_metadados_doi(doi):
    url = f"https://api.crossref.org/works/{doi}"
    response = requests.get(url)
    
    if response.status_code == 200:
        data = response.json().get("message", {})
        
        # Extrair informações adicionais
        palavra_chave = data.get("subject", [])  # Lista de palavras-chave
        resumo = data.get("abstract", None)  # Resumo do artigo, se disponível
        
        # Limpar o resumo (se necessário)
        if resumo:
            resumo = resumo.replace("<jats:p>", "").replace("</jats:p>", "").strip()
        
        return {
            "palavra_chave": palavra_chave,
            "resumo": resumo,
        }
    else:
        print(f"Erro ao buscar DOI {doi}: {response.status_code}")
        return None

# Conectar ao banco de dados
conn = pymysql.connect(
    host="localhost",
    user="root",
    password="marcela00119m.",  # Atualize com sua senha
    database="sistema_publicacoes",
    port=3306
)
cursor = conn.cursor()

# Recuperar DOIs das publicações
cursor.execute("SELECT id_publicacao, doi_publicacao FROM publicacao WHERE doi_publicacao IS NOT NULL")
publicacoes = cursor.fetchall()

for id_publicacao, doi in publicacoes:
    try:
        metadados = buscar_metadados_doi(doi)
        
        if metadados:
            palavra_chave = metadados.get("palavra_chave", [])
            resumo = metadados.get("resumo", None)
            
            # Atualizar resumo na tabela `publicacao`
            sql_resumo = """
            UPDATE publicacao
            SET resumo = %s
            WHERE id_publicacao = %s
            """
            cursor.execute(sql_resumo, (resumo, id_publicacao))
            
            # Inserir palavras-chave na tabela `palavra_chave_publicacao`
            for palavra in palavra_chave:
                sql_palavra = """
                INSERT INTO palavra_chave_publicacao (id_publicacao, palavra_chave)
                VALUES (%s, %s)
                ON DUPLICATE KEY UPDATE palavra_chave = VALUES(palavra_chave)
                """
                cursor.execute(sql_palavra, (id_publicacao, palavra))
    
    except Exception as e:
        print(f"Erro ao processar DOI {doi}: {e}")

# Confirmar alterações no banco de dados
conn.commit()

# Fechar a conexão
cursor.close()
conn.close()

print("Metadados atualizados com sucesso!")
