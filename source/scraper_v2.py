import os
import time
import json
from bs4 import BeautifulSoup, Tag
import re

# --- Constantes para os textos e tags a serem buscados ---
BL_SEARCH_TEXT = "o BL do Conhecimento de Embarque Orig"
CE_MERCANTE_DT_TAG = "No. CE-MERCANTE Master vinculado :"

# Títulos H2 para as seções de informações a serem coletadas
JSON_FINAL = {
    "bl": "",
    "ce": "",
    "manifesto_conhecimento":
    {
        "num": "",
        "carregamento": "",
        "descarregamento": "",
        "tipo_conhecimento": "",
        "bl_servico": "",
        "n_ce": "",
        "data_emissao_bl": ""
    },
    "embarcador":
    {
        "dados_complementares": ""
    },
    "consignatario":
    {
        "bl_a_ordem": "",
        "cnpj_cpf": "",
        "razao_social_nome": "",
        "dados_complementares": ""
    },
    "parte_a_ser_notificada":
    {
        "cnpj_cpf": "",
        "razao_social_nome": "",
        "dados_complementares": ""
    },
    "transportador_representante":
    {
        "cnpj": "",
        "razao_social": ""
    },
    "origem_destino_carga":
    {
        "bl_conhecimento_embarque_original": "",
        "origem":
        {
            "porto": "",
            "pais_procedencia": ""
        },
        "destino":
        {
            "uf": "",
            "porto": "",
            "numero_doc_despacho": ""
        }
    },
    "mercadoria":
    {
        "descricao1": "",
        "descricao2": "",
        "peso": "",
        "cubagem": "",
        "categoria": "",
        "situacao":
        {
            "atual": "",
            "data_entrega": ""
        }
    },
    "frete":
    {
        "recolhimento": "",
        "modalidade": "",
        "moeda": "",
        "valor": "",
        "componentes":
        {
            "componente": "",
            "moeda": "",
            "valor": "",
            "recolhimento": ""
        }
    }
}

# --- Variável global para o intervalo de reprocessamento (agora pode ser alterada pelo usuário) ---
REPROCESS_INTERVAL_SECONDS = 10 # Valor inicial padrão

# --- Simulação de Serviço de Mensageria (apenas print no console do Colab) ---
def notify_messaging_service(message: str, type: str = "info", data: dict | None = None):
    """
    Simula o envio de uma notificação para um serviço de mensageria, imprimindo no console.
    Args:
        message (str): A mensagem a ser enviada.
        type (str): O tipo de notificação (e.g., 'info', 'success', 'error').
        data (dict | None): Dicionário de dados opcional a ser incluído na notificação.
    """
    timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
    notification_msg = f"[{timestamp}][MENSAGERIA][{type.upper()}] {message}"
    print(notification_msg)

    if data:
        # Adiciona os dados em formato JSON à mensagem de notificação
        print(f"Dados Coletados (JSON):\n{json.dumps(data, indent=2, ensure_ascii=False)}")


# --- Funções de Parsing e Coleta de Dados ---
def read_html_file(filepath: str) -> str | None:
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            return f.read()
    except FileNotFoundError:
        notify_messaging_service(f"Arquivo não encontrado: {filepath}", "error")
        return None
    except Exception as e:
        notify_messaging_service(f"Erro ao ler o arquivo {filepath}: {e}", "error")
        return None

def extrair_dd_por_dt(soup, label, destino_dict, *chaves_json):
    """
    Procura um <dt> com o texto `label` e extrai o valor do próximo <dd>,
    salvando em `destino_dict[chave1][chave2]...`
    """
    dt = soup.find("dt", string=label)
    if dt:
        dd = dt.find_next_sibling("dd")
        if dd:
            valor = dd.get_text(strip=True)
            # Caminho dinâmico de acesso
            d = destino_dict
            for chave in chaves_json[:-1]:
                d = d.setdefault(chave, {})
            d[chaves_json[-1]] = valor

def dividir_html_por_secoes(soup):
    secoes = {}
    h2_tags = soup.find_all("h2")

    for i, h2 in enumerate(h2_tags):
        nome_secao = h2.get_text(strip=True)
        conteudo = []

        # Pega tudo até o próximo <h2> (ou até o fim do HTML)
        next_tag = h2.next_sibling
        while next_tag and not (isinstance(next_tag, Tag) and next_tag.name == "h2"):
            conteudo.append(next_tag)
            next_tag = next_tag.next_sibling

        # Cria novo soup só com essa parte
        secoes[nome_secao] = BeautifulSoup("".join(str(e) for e in conteudo), "html.parser")

    return secoes

def process_html_data(html_content: str, bl_number: str, filename: str) -> dict | None:
    """
    Processa o conteúdo HTML para extrair informações do BL e CE-Mercante.
    Retorna os dados se ambos forem encontrados, caso contrário, retorna None.
    Não faz sleeps aqui para permitir que todos os arquivos sejam processados.
    """
    soup = BeautifulSoup(html_content, 'html.parser')
    extracted_data = {"Arquivo_Origem": filename}

    bl_found = False
    for tag in soup.find_all(lambda tag: tag.name in ['dt', 'dd', 'span', 'p', 'b', 'div']):
        if tag.get_text(strip=True) and (BL_SEARCH_TEXT in tag.get_text(strip=True)):
            if bl_number in tag.get_text(strip=True):
                bl_found = True
                notify_messaging_service(f"BL '{bl_number}' encontrado no arquivo '{filename}'.", "info")
                break

    if not bl_found:
        notify_messaging_service(f"BL '{bl_number}' não encontrado no arquivo '{filename}'.", "info")
        return None # Retorna None imediatamente se o BL não for encontrado

    ce_mercante_value = ""
    dt_ce_mercante = soup.find('dt', string=CE_MERCANTE_DT_TAG)

    if dt_ce_mercante:
        dd_ce_mercante = dt_ce_mercante.find_next_sibling('dd')
        if dd_ce_mercante and dd_ce_mercante.get_text(strip=True):
            ce_mercante_value = dd_ce_mercante.get_text(strip=True)
            notify_messaging_service(f"CE-MERCANTE Master Vinculado: '{ce_mercante_value}' encontrado em '{filename}'.", "info")
        else:
            notify_messaging_service(f"BL '{bl_number}' encontrado em '{filename}', mas não contem CE Mercante ou está vazio.", "info")
            return None # Retorna None se CE-Mercante não for encontrado/vazio
    else:
        notify_messaging_service(f"BL '{bl_number}' encontrado em '{filename}', mas a tag '{CE_MERCANTE_DT_TAG}' não foi encontrada.", "info")
        return None # Retorna None se a tag CE-Mercante não for encontrada

    extracted_data["json_final"] = JSON_FINAL

    extracted_data["json_final"]["bl"] = bl_number
    extracted_data["json_final"]["ce"] = ce_mercante_value

    ## ----- SEÇÃO Manifesto/Conhecimento ---------

    # Busca "Numero do Manifesto"
    link = soup.find("a", href=lambda x: x and "ConsultarManifestoMaritimoPorNumero.do" in x)

    if link:
        href = link["href"]
        # Ex: /carga-web/ConsultarManifestoMaritimoPorNumero.do?hora=1698412002965&nrManifesto=1823501459664
        for part in href.split("&"):
            if "nrManifesto=" in part:
                extracted_data["json_final"]["manifesto_conhecimento"]["num"] = part.split("=")[1]

    # Busca "Porto / Terminal de Carregamento"
    th = soup.find("th", string="Porto / Terminal de Carregamento")

    if th:
        # Pega a linha (tr) que contém essa th
        tr = th.find_parent("tr").find_parent("thead")
        if tr:
            # Pega o próximo tr (a linha dos dados)
            next_tr = tr.find_next_sibling("tr")
            if next_tr:
                tds = next_tr.find_all("td")
                if len(tds) >= 3:
                    porto_carregamento = tds[1].get_text(separator='', strip=True).replace('\xa0', ' ')
                    extracted_data["json_final"]["manifesto_conhecimento"]["carregamento"] = re.sub(r'\s{2,}', ' ', porto_carregamento)

                    porto_descarregamento = tds[2].get_text(separator='', strip=True).replace('\xa0', ' ')
                    extracted_data["json_final"]["manifesto_conhecimento"]["descarregamento"] = re.sub(r'\s{2,}', ' ', porto_descarregamento)
    
    secoes = dividir_html_por_secoes(soup)

    extrair_dd_por_dt(secoes['Manifesto/Conhecimento'], "Tipo de Conhecimento :", extracted_data["json_final"], "manifesto_conhecimento", "tipo_conhecimento")
    extrair_dd_por_dt(secoes['Manifesto/Conhecimento'], "BL de Serviço :", extracted_data["json_final"], "manifesto_conhecimento", "bl_servico")
    extrair_dd_por_dt(secoes['Manifesto/Conhecimento'], "No. CE-MERCANTE Master vinculado :", extracted_data["json_final"], "manifesto_conhecimento", "n_ce")
    extrair_dd_por_dt(secoes['Manifesto/Conhecimento'], "Data de Emissão do BL :", extracted_data["json_final"], "manifesto_conhecimento", "data_emissao_bl")

    ## ----- SEÇÃO Embarcador ---------

    extrair_dd_por_dt(secoes['Embarcador'], "Dados Complementares do Embarcador :", extracted_data["json_final"], "embarcador", "dados_complementares")

    ## ----- SEÇÃO Consignatário ---------

    extrair_dd_por_dt(secoes['Consignatário'], "BL a Ordem :", extracted_data["json_final"], "consignatario", "bl_a_ordem")
    extrair_dd_por_dt(secoes['Consignatário'], "CNPJ/CPF :", extracted_data["json_final"], "consignatario", "cnpj_cpf")
    extrair_dd_por_dt(secoes['Consignatário'], "Razão Social/Nome :", extracted_data["json_final"], "consignatario", "razao_social_nome")
    extrair_dd_por_dt(secoes['Consignatário'], "Dados Complementares do Consignatário :", extracted_data["json_final"], "consignatario", "dados_complementares")

    ## ----- SEÇÃO Parte a ser Notificada ---------

    extrair_dd_por_dt(secoes['Parte a ser Notificada'], "CNPJ/CPF :", extracted_data["json_final"], "parte_a_ser_notificada", "cnpj_cpf")
    extrair_dd_por_dt(secoes['Parte a ser Notificada'], "Razão Social/Nome :", extracted_data["json_final"], "parte_a_ser_notificada", "razao_social_nome")
    extrair_dd_por_dt(secoes['Parte a ser Notificada'], "Dados Complementares :", extracted_data["json_final"], "parte_a_ser_notificada", "dados_complementares")

    ## ----- SEÇÃO Transportador/Representante ---------

    extrair_dd_por_dt(secoes['Transportador ou representante:'], "CNPJ :", extracted_data["json_final"], "transportador_representante", "cnpj")
    extrair_dd_por_dt(secoes['Transportador ou representante:'], "Razão Social :", extracted_data["json_final"], "transportador_representante", "razao_social")

    ## ----- SEÇÃO Origem/Destino da Carga ---------

    extrair_dd_por_dt(secoes['Procedência e Destino da Carga'], "Número BL do Conhecimento de Embarque Original :", extracted_data["json_final"], "origem_destino_carga", "bl_conhecimento_embarque_original")
    extrair_dd_por_dt(secoes['Procedência e Destino da Carga'], "Porto de Origem :", extracted_data["json_final"], "origem_destino_carga", "origem", "porto")
    extrair_dd_por_dt(secoes['Procedência e Destino da Carga'], "País de Procedência :", extracted_data["json_final"], "origem_destino_carga", "origem", "pais_procedencia")
    extrair_dd_por_dt(secoes['Procedência e Destino da Carga'], "UF de Destino Final :", extracted_data["json_final"], "origem_destino_carga", "destino", "uf")
    extrair_dd_por_dt(secoes['Procedência e Destino da Carga'], "Porto de Destino Final :", extracted_data["json_final"], "origem_destino_carga", "destino", "porto")
    extrair_dd_por_dt(secoes['Procedência e Destino da Carga'], "Número/Tipo do Documento de Despacho", extracted_data["json_final"], "origem_destino_carga", "destino", "numero_doc_despacho")

    ## ----- SEÇÃO Mercadoria ---------

    extrair_dd_por_dt(secoes['Mercadoria'], "Descrição 1 :", extracted_data["json_final"], "mercadoria", "descricao1")
    extrair_dd_por_dt(secoes['Mercadoria'], "Descrição 2 :", extracted_data["json_final"], "mercadoria", "descricao2")
    extrair_dd_por_dt(secoes['Mercadoria'], "Peso Bruto da Carga (Kg) :", extracted_data["json_final"], "mercadoria", "peso")
    extrair_dd_por_dt(secoes['Mercadoria'], "Cubagem (em m3) :", extracted_data["json_final"], "mercadoria", "cubagem")
    extrair_dd_por_dt(secoes['Mercadoria'], "Categoria :", extracted_data["json_final"], "mercadoria", "categoria")

    # Situação da carga e data de entrega (aninhado)
    dt = soup.find("dt", string="Situação :")
    if dt:
        dd = dt.find_next_sibling("dd")
        if dd:
            texto = dd.get_text(separator=' ', strip=True).replace('\xa0', ' ')
            partes = texto.split()
            if partes:
                extracted_data["json_final"]["mercadoria"]["situacao"]["atual"] = partes[0]
                if len(partes) > 1:
                    extracted_data["json_final"]["mercadoria"]["situacao"]["data_entrega"] = partes[-1]

    ## ----- SEÇÃO Frete ---------

    extrair_dd_por_dt(secoes['Frete e Despesas de Transporte'], "Recolhimento de Frete :", extracted_data["json_final"], "frete", "recolhimento")
    extrair_dd_por_dt(secoes['Frete e Despesas de Transporte'], "Modalidade de Frete :", extracted_data["json_final"], "frete", "modalidade")
    extrair_dd_por_dt(secoes['Frete e Despesas de Transporte'], "Moeda do Frete :", extracted_data["json_final"], "frete", "moeda")
    extrair_dd_por_dt(secoes['Frete e Despesas de Transporte'], "Valor do Frete Básico :", extracted_data["json_final"], "frete", "valor")

    # Componentes do frete (tabela dentro de <dd>)
    dl = soup.find("dt", string="Componentes do Frete :")
    if dl:
        dd = dl.find_next_sibling("dd")
        if dd:
            linhas = dd.find_all("tr")
            if len(linhas) > 1:
                cols = linhas[1].find_all("td")
                if len(cols) >= 4:
                    extracted_data["json_final"]["frete"]["componentes"]["componente"] = cols[0].get_text(strip=True).replace('\xa0', ' ')
                    extracted_data["json_final"]["frete"]["componentes"]["componente"] = re.sub(r'\s{2,}', ' ', extracted_data["json_final"]["frete"]["componentes"]["componente"])
                    
                    extracted_data["json_final"]["frete"]["componentes"]["moeda"] = cols[1].get_text(strip=True).replace('\xa0', ' ')
                    extracted_data["json_final"]["frete"]["componentes"]["moeda"] = re.sub(r'\s{2,}', ' ', extracted_data["json_final"]["frete"]["componentes"]["moeda"])
                    
                    extracted_data["json_final"]["frete"]["componentes"]["valor"] = cols[2].get_text(strip=True).replace('\xa0', ' ')
                    extracted_data["json_final"]["frete"]["componentes"]["recolhimento"] = cols[3].get_text(strip=True).replace('\xa0', ' ')


    return extracted_data

# --- Função para notificar sucesso/insucesso e exibir dados ---
def handle_processing_results(data_list: list[dict]):
    """
    Lida com os resultados do processamento, exibindo mensagem de sucesso ou insucesso.
    """
    if data_list: # Se a lista de dados coletados não estiver vazia
        # print("\n" + "="*50)
        # print("      ✅ DADOS COLETADOS COM SUCESSO! ✅")
        # print("="*50)
        # print("Os seguintes dados foram extraídos e estão prontos para processamento:")
        # print(json.dumps(data_list, indent=2, ensure_ascii=False))
        # print("="*50 + "\n")
        notify_messaging_service("Dados coletados com sucesso em uma ou mais arquivos.", "success", data=data_list)
        return True # Indica sucesso
    else:
        print("\n" + "!"*50)
        print("      ❌ PROCESSO CONCLUÍDO SEM SUCESSO ❌")
        print("!"*50)
        print("O BL e/ou CE-Mercante não foi encontrado em nenhum dos arquivos processados.")
        notify_messaging_service("Nenhum dado relevante encontrado nesta rodada de processamento.", "warning")
        return False # Indica insucesso

# --- Função Principal do Programa ---
def main(current_bl_number, current_folder_path):
    global REPROCESS_INTERVAL_SECONDS # Permite modificar a variável global

    running = True
    while running:
        html_files = [f for f in os.listdir(current_folder_path) if f.endswith(('.html', '.htm'))]


        if not html_files:
            notify_messaging_service(f"Nenhum arquivo HTML (.html ou .htm) encontrado na pasta: {current_folder_path}", "warning")
            action_no_files = input("Nenhum arquivo HTML encontrado. Deseja (f)inalizar ou (m)udar pasta/BL? [f/m]: ").lower()
            if action_no_files == 'f':
                running = False
                print("Finalizando o programa.")
            elif action_no_files == 'm':
                # Mudar pasta/BL
                while True:
                    new_folder_path = input("Digite o NOVO caminho completo da pasta com os arquivos HTML: ")
                    if os.path.isdir(new_folder_path):
                        current_folder_path = new_folder_path
                        break
                    else:
                        notify_messaging_service(f"O caminho da nova pasta '{new_folder_path}' não é válido ou não existe. Por favor, tente novamente.", "error")


                # Novo BL para a nova pasta
                while True:
                    new_bl_number = input("Digite o NOVO número do BL (DEVE TER 17 DIGITOS): ")
                    if len(new_bl_number) == 17 and new_bl_number.isdigit():
                        current_bl_number = new_bl_number
                        break
                    else:
                        print("ERRO: O número do BL deve ter exatamente 17 dígitos. Por favor, tente novamente.")
                print(f"Caminho da pasta e BL atualizados. Reprocessando com novos dados...")
                continue # Volta para o início do loop 'while running'

            
            else:
                print("Opção inválida. Finalizando por segurança.")
                running = False
            continue # Pula o resto desta iteração do loop principal

        print(f"\n--- Iniciando processamento de {len(html_files)} arquivo(s) na pasta '{current_folder_path}' ---")
        current_round_data = []

        for filename in html_files:
            full_filepath = os.path.join(current_folder_path, filename)
            print(f"\nProcessando arquivo: {filename}")
            html_content = read_html_file(full_filepath)

            if html_content:
                # Usa o BL da rodada atual
                collected_data = process_html_data(html_content, current_bl_number, filename)
                if collected_data:
                    current_round_data.append(collected_data)

        # --- Lógica de Sucesso/Insucesso e Reprocessamento ---
        if handle_processing_results(current_round_data):
            # Se houve sucesso em pelo menos um arquivo, perguntar se quer continuar
            print("\n--- Rodada de processamento concluída. ---")
            running = False

if __name__ == "__main__":
    main(current_bl_number = '99999999999999123', current_folder_path = "siscarga/html_gerados")