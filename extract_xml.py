from lxml import etree
import re
import time
from bs4 import BeautifulSoup  # Importa BeautifulSoup para corrigir o XML automaticamente

# Lista para armazenar publicações como dicionários
publicacoes = []

# Função para extrair o DOI após 'https://doi.org/' da URL
def extrair_doi_da_url(url):
    if url:
        match = re.search(r"(?<=https://doi\.org/)(.*)", url)
        if match:
            return match.group(1).strip()  # Retorna a parte após 'https://doi.org/'
    return None

def corrigir_com_soup(arquivo):
    with open(arquivo, "r", encoding="utf-8") as f:
        conteudo = f.read()
    
    # Corrige automaticamente entidades usando BeautifulSoup
    soup = BeautifulSoup(conteudo, "xml")
    return str(soup)

def processar_xml(arquivo):
    # Corrigir o XML com BeautifulSoup antes de passar para o lxml
    xml_corrigido = corrigir_com_soup(arquivo)

    parser = etree.XMLParser(recover=True, encoding="utf-8", no_network=True, resolve_entities=True)

    try:
        # Parsing do XML corrigido em modo streaming
        context = etree.iterparse(
            bytes(xml_corrigido, "utf-8"), events=("end",), tag="inproceedings"
        )
        for event, elem in context:
            # Criar um dicionário para cada publicação
            publicacao = {}

            if elem is not None:
                titulo = elem.findtext("title", default="").strip()
                publicacao["titulo"] = titulo  # Título já corrigido pelo BeautifulSoup

                publicacao["ano"] = elem.findtext("year", default=None)
                publicacao["acesso"] = elem.findtext("access", default=None)
                publicacao["url_leitura"] = elem.findtext("ee", default=None)

                # URL da tag <ee> (contém o DOI completo)
                url_leitura = elem.findtext("ee", default=None)
                if url_leitura:
                    # Extrair o DOI da URL (se o URL começar com https://doi.org/)
                    publicacao["doi_publicacao"] = extrair_doi_da_url(url_leitura)

                # Autores (no formato de lista)
                autores = [
                    author.text.strip()
                    for author in elem.findall("author")
                    if author.text
                ]
                publicacao["autores"] = autores

            if publicacao["titulo"]:  # Verifica se o título foi extraído
                publicacoes.append(publicacao)

            elem.clear()  # Liberar memória

        del context  # Fechar o iterador

    except Exception as e:
        print(f"Erro ao processar o arquivo {arquivo}: {e}")

# Lista de arquivos XML para processar
arquivos_array = [
    f"D:\\Users\\Mar-o\\Desktop\\computing\\api-dblp-json\\dblp_part_a{letra}.xml"
    for letra in (chr(x) for x in range(ord("a"), ord("i") + 1))
]

# Processar cada arquivo
for arq in arquivos_array:
    processar_xml(arq)
    time.sleep(1)  # Esperar 1 segundo entre arquivos
