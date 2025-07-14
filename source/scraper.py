import os
import time
import json
from bs4 import BeautifulSoup, Tag

# --- Constantes para os textos e tags a serem buscados ---
BL_SEARCH_TEXT = "o BL do Conhecimento de Embarque Orig"
CE_MERCANTE_DT_TAG = "No. CE-MERCANTE Master vinculado :"

# Títulos H2 para as seções de informações a serem coletadas
H2_TAGS = {
    "Manifesto/Conhecimento": "Manifesto/Conhecimento",
    "Embarcador": "Embarcador",
    "Consignatario": "Consignatario",
    "Parte a ser Notificada": "Parte a ser Notificada",
    "Procedencia e Destino da Carga": "Procedência e Destino da Carga",
    "Mercadoria": "Mercadoria"
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

def process_html_data(html_content: str, bl_number: str, filename: str) -> dict | None:
    """
    Processa o conteúdo HTML para extrair informações do BL e CE-Mercante.
    Retorna os dados se ambos forem encontrados, caso contrário, retorna None.
    Não faz sleeps aqui para permitir que todos os arquivos sejam processados.
    """
    soup = BeautifulSoup(html_content, 'html.parser')
    extracted_data = {"Arquivo_Origem": filename, "BL_Buscado": bl_number}

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

    extracted_data["CE-MERCANTE Master Vinculado"] = ce_mercante_value

    for key, h2_text in H2_TAGS.items():
        h2_tag = soup.find('h2', string=h2_text)
        if h2_tag:
            current_tag = h2_tag.next_sibling
            section_content = []
            while current_tag and not (isinstance(current_tag, Tag) and current_tag.name == 'h2'):
                if isinstance(current_tag, Tag):
                    section_content.append(current_tag.get_text(separator=' ', strip=True))
                elif isinstance(current_tag, str):
                    section_content.append(current_tag.strip())
                current_tag = current_tag.next_sibling
            extracted_data[key] = "\n".join(filter(None, section_content)).strip()
            notify_messaging_service(f"Seção '{key}' coletada de '{filename}'.", "info")
        else:
            extracted_data[key] = "Não encontrada"
            notify_messaging_service(f"Seção '{key}' não encontrada em '{filename}'.", "info")

    return extracted_data

# --- Função para notificar sucesso/insucesso e exibir dados ---
def handle_processing_results(data_list: list[dict]):
    """
    Lida com os resultados do processamento, exibindo mensagem de sucesso ou insucesso.
    """
    if data_list: # Se a lista de dados coletados não estiver vazia
        print("\n" + "="*50)
        print("      ✅ DADOS COLETADOS COM SUCESSO! ✅")
        print("="*50)
        print("Os seguintes dados foram extraídos e estão prontos para processamento:")
        print(json.dumps(data_list, indent=2, ensure_ascii=False))
        print("="*50 + "\n")
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
def main(current_bl_number):
    global REPROCESS_INTERVAL_SECONDS # Permite modificar a variável global
    current_folder_path = "siscarga/html_gerados" # Caminho dos htmls

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
            action = input("Deseja (n)ovo processamento (mesma pasta), (f)inalizar ou (m)udar pasta/BL? [n/f/m]: ").lower()

            if action == 'f':
                running = False
                print("Finalizando o programa.")
            elif action == 'm':
                # Mudar pasta/BL
                while True:
                    new_folder_path = input("Digite o NOVO caminho completo da pasta com os arquivos HTML: ")
                    if os.path.isdir(new_folder_path):
                        current_folder_path = new_folder_path
                        break
                    else:
                        notify_messaging_service(f"O caminho da nova pasta '{new_folder_path}' não é válido ou não existe. Por favor, tente novamente.", "error")

                # NOVO BL SEMPRE
                while True:
                    new_bl_number = input("Digite o NOVO número do BL (DEVE TER 17 DIGITOS): ")
                    if len(new_bl_number) == 17 and new_bl_number.isdigit():
                        current_bl_number = new_bl_number
                        break
                    else:
                        print("ERRO: O número do BL deve ter exatamente 17 dígitos. Por favor, tente novamente.")
                print(f"Caminho da pasta e BL atualizados. Reprocessando com novos dados...")
            else: # 'n' ou qualquer outra entrada, assume novo processamento
                print("Iniciando um novo processamento na mesma pasta. Digite o NOVO BL.")
                # NOVO BL SEMPRE
                while True:
                    new_bl_number = input("Digite o NOVO número do BL (DEVE TER 17 DIGITOS): ")
                    if len(new_bl_number) == 17 and new_bl_number.isdigit():
                        current_bl_number = new_bl_number
                        break
                    else:
                        print("ERRO: O número do BL deve ter exatamente 17 dígitos. Por favor, tente novamente.")
                print("Reprocessando com o novo BL...")

        else: # Se não houve sucesso na rodada
            print("\n--- Rodada de processamento concluída com INSUCESSO. ---")
            action_failure = input("Deseja (r)eprocessar (com novo tempo), (f)inalizar ou (m)udar pasta/BL? [r/f/m]: ").lower()

            if action_failure == 'f':
                running = False
                print("Finalizando o programa.")
            elif action_failure == 'm':
                # Mudar pasta/BL
                while True:
                    new_folder_path = input("Digite o NOVO caminho completo da pasta com os arquivos HTML: ")
                    if os.path.isdir(new_folder_path):
                        current_folder_path = new_folder_path
                        break
                    else:
                        notify_messaging_service(f"O caminho da nova pasta '{new_folder_path}' não é válido ou não existe. Por favor, tente novamente.", "error")

                # NOVO BL SEMPRE
                while True:
                    new_bl_number = input("Digite o NOVO número do BL (DEVE TER 17 DIGITOS): ")
                    if len(new_bl_number) == 17 and new_bl_number.isdigit():
                        current_bl_number = new_bl_number
                        break
                    else:
                        print("ERRO: O número do BL deve ter exatamente 17 dígitos. Por favor, tente novamente.")
                print(f"Caminho da pasta e BL atualizados. Reprocessando com novos dados...")

            elif action_failure == 'r':
                try:
                    new_interval = int(input(f"Digite o tempo em segundos para esperar antes de reprocessar: "))
                    if new_interval <= 0:
                        running = False
                        print("Tempo inválido ou 0. Finalizando o programa.")
                    else:
                        REPROCESS_INTERVAL_SECONDS = new_interval # Atualiza o intervalo global
                        print(f"Aguardando {REPROCESS_INTERVAL_SECONDS} segundos para reprocessar todos os arquivos com o mesmo BL...")
                        time.sleep(REPROCESS_INTERVAL_SECONDS)
                        print("Reprocessando todos os arquivos após a espera...")
                except ValueError:
                    print("Entrada inválida. Finalizando o programa.")
                    running = False
            else:
                print("Opção inválida. Finalizando por segurança.")
                running = False

if __name__ == "__main__":
    main('99999999999999123')