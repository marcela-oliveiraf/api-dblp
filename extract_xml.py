from lxml import etree
import html.entities
import time
import re

publicacoes = []  # Lista para armazenar os dados extraídos

# Função para extrair o DOI após 'https://doi.org/' da URL
def extrair_doi_da_url(url):
    if url:
        match = re.search(r"(?<=https://doi\.org/)(.*)", url)
        if match:
            return match.group(1).strip()  # Retorna a parte após 'https://doi.org/'
    return None

# Função para limpar ou ignorar entidades não definidas no XML
def limpar_entidades_nao_definidas(arquivo):
    with open(arquivo, "r", encoding="UTF-8") as f:
        conteudo = f.read()

    # Usar regex para remover entidades desconhecidas (qualquer coisa entre `&` e `;` que não seja reconhecida)
    conteudo = re.sub(r"&[a-zA-Z]+;", "", conteudo)
    return conteudo

# Função para processar o XML
def processar_xml(arquivo):
    try:
        # Corrigir entidades no conteúdo do arquivo
        xml_corrigido = limpar_entidades_nao_definidas(arquivo)

        # Criar um parser customizado
        parser = etree.XMLParser(recover=True, encoding="UTF-8", resolve_entities=False)

        # Parsing do XML corrigido em modo streaming
        context = etree.iterparse(bytes(xml_corrigido, "UTF-8"), events=("end",), tag="inproceedings", encoding="UTF-8")

        publicacoes = []  # Lista para armazenar os dados extraídos

        for _, elem in context:
            publicacao = {
                "titulo": elem.findtext("title", default="").strip(),
                "ano": elem.findtext("year", default=None),
                "acesso_tipo": elem.findtext("access", default=None),
                "url_leitura": elem.findtext("ee", default=None),
            }

            # Extrair autores
            publicacao["autores"] = [
                author.text.strip() for author in elem.findall("author") if author.text
            ]

            # Adicionar à lista de publicações
            if publicacao["titulo"]:
                publicacoes.append(publicacao)

            # Liberar memória
            elem.clear()

        del context  # Fechar o iterador

        return publicacoes

    except Exception as e:
        print(f"Erro ao processar o arquivo {arquivo}: {e}")
        return []

# Lista de arquivos XML para processar
arquivos_array = [
    f"D:\\Users\\Mar-o\\Desktop\\computing\\api-dblp\\dblp_part_a{letra}.xml"
    for letra in ("h", "i")  # Definir os arquivos que você deseja processar
]

# Processar cada arquivo
for arq in arquivos_array:
    publicacoes = processar_xml(arq)
    print(f"Publicações processadas no arquivo {arq}: {len(publicacoes)}")



