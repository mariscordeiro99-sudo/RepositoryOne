import os
import glob
import pandas as pd
import xlrd
import pymongo
from datetime import datetime
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi


# ==============================
# CONFIGURAÇÕES GERAIS
# ==============================

PASTA = r"C:\Users\Administrador\Documents\Dados"
PADRAO = "Boletim_Diario_dos_Atendimentos_*"
COLS_FORCE_STR = ["Nr. Registro", "CNS"]
CSV_CHUNK_SIZE = 500_000
PARQUET_SAIDA = os.path.join(PASTA, "consolidado.parquet")

def solicita_senha():
    print("-------------------------------------")
    print("##### Conexão com o Banco de Dados #####")
    usuario = input("Digite o usuario do mongo: ")
    senha = input("Digite sua senha do mongo: ")
    uri =  "mongodb+srv://"+usuario+":"+senha+"@cluster0.5vugpvf.mongodb.net/?appName=Cluster0"
    return uri

MONGO_URI = solicita_senha()


# ==============================
# LOG PADRÃO
# ==============================





def log(msg):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}")


# ==============================
# FUNÇÕES DO PARQUET
# ==============================

def read_xls_old(path):
    book = xlrd.open_workbook(path)
    sheet = book.sheet_by_index(0)
    data = []

    for r in range(sheet.nrows):
        data.append(sheet.row_values(r))

    df = pd.DataFrame(data[1:], columns=data[0])
    return df


def safe_read_excel(arquivo):
    ext = os.path.splitext(arquivo)[1].lower()

    try:
        if ext == ".xls":
            log("  → Lendo como XLS antigo (xlrd)...")
            return read_xls_old(arquivo)

        if ext in [".xlsx", ".xlsm"]:
            log("  → Lendo como XLSX/XLSM (openpyxl)...")
            return pd.read_excel(arquivo, engine="openpyxl")

        if ext == ".xlsb":
            log("  → Lendo como XLSB (pyxlsb)...")
            return pd.read_excel(arquivo, engine="pyxlsb")

        raise ValueError(f"Extensão não suportada: {ext}")

    except Exception as e:
        raise RuntimeError(f"Falha ao ler {arquivo}: {e}")


def normalizar_dataframe(df):
    for col in COLS_FORCE_STR:
        if col in df.columns:
            df[col] = df[col].astype(str).fillna("").replace("nan", "")

    for col in df.select_dtypes(include=["object"]).columns:
        df[col] = (
            df[col]
            .astype(str)
            .str.replace("\x00", "", regex=False)
            .fillna("")
            .replace("nan", "")
        )

    return df


def carregar_arquivos():
    arquivos = glob.glob(os.path.join(PASTA, PADRAO))
    arquivos = [a for a in arquivos if a.lower().endswith((".xls", ".xlsx", ".xlsm", ".xlsb"))]

    log(f"{len(arquivos)} arquivos encontrados.")
    dfs = []

    for arq in arquivos:
        log(f"Lendo arquivo: {os.path.basename(arq)}")
        try:
            df = safe_read_excel(arq)
            df = normalizar_dataframe(df)
            df["arquivo_origem"] = os.path.basename(arq)

            dfs.append(df)
            log(f"  ✓ OK — {len(df)} linhas lidas.")
        except Exception as e:
            log(f"  ✗ ERRO ao ler {arq}: {e}")

    if not dfs:
        raise RuntimeError("Nenhum arquivo válido foi carregado.")

    return pd.concat(dfs, ignore_index=True)


def salvar_parquet(df):
    try:
        log("Salvando em Parquet...")
        df.to_parquet(PARQUET_SAIDA, index=False)
        log(f"✓ Parquet salvo com sucesso: {PARQUET_SAIDA}")
        return True
    except Exception as e:
        log(f"✗ ERRO ao salvar Parquet: {e}")
        return False


def fallback_csv(df):
    log("⚠ Erro no Parquet — salvando como CSV em partes...")
    for i in range(0, len(df), CSV_CHUNK_SIZE):
        chunk = df.iloc[i:i + CSV_CHUNK_SIZE]
        nome = f"fallback_parte_{i//CSV_CHUNK_SIZE + 1}.csv"
        caminho = os.path.join(PASTA, nome)
        chunk.to_csv(caminho, sep=";", index=False)
        log(f"  ✓ Chunk salvo: {nome}")


def gerar_parquet():
    log("Iniciando geração de Parquet...")
    df = carregar_arquivos()

    if not salvar_parquet(df):
        fallback_csv(df)

    log("Processo finalizado.")


# ==============================
# FUNÇÃO – CRIAR LOG LOCAL
# ==============================

def criar_log_alteracoes():
    if not os.path.exists(PARQUET_SAIDA):
        print("\n❌ Arquivo Parquet não encontrado.\n")
        return

    m_time_timestamp = os.path.getmtime(PARQUET_SAIDA)
    m_time_datetime = datetime.fromtimestamp(m_time_timestamp)

    print(f"\nÚltima modificação: {m_time_datetime}")
    print(f"Formatado: {m_time_datetime.strftime('%Y-%m-%d')}\n")


# ==============================
# FUNÇÃO – SALVAR NO MONGO
# ==============================

def salvar_log_mongo():
    if not os.path.exists(PARQUET_SAIDA):
        print("\n❌ Arquivo Parquet não encontrado.\n")
        return

    m_time_timestamp = os.path.getmtime(PARQUET_SAIDA)
    m_time_datetime = datetime.fromtimestamp(m_time_timestamp)

    client = MongoClient(MONGO_URI, server_api=ServerApi('1'))

    db = client["meu_banco"]
    collection = db["modificacoes_arquivos"]

    documento = {
        "file_path": PARQUET_SAIDA,
        "last_modified": m_time_datetime,
        "last_modified_str": m_time_datetime.strftime('%Y-%m-%d')
    }

    result = collection.insert_one(documento)

    print(f"\nDocumento inserido no MongoDB. ID: {result.inserted_id}\n")


# ==============================
# MENU PRINCIPAL
# ==============================

def menu():
    while True:
        print("\n========== MENU PRINCIPAL ==========")
        print("1 - Gerar arquivo Parquet")
        print("2 - Criar Log de alterações")
        print("3 - Salvar log no Banco (MongoDB)")
        print("0 - Sair")
        print("====================================")

        opcao = input("Escolha uma opção: ")

        if opcao == "1":
            gerar_parquet()
        elif opcao == "2":
            criar_log_alteracoes()
        elif opcao == "3":
            salvar_log_mongo()
        elif opcao == "0":
            print("\nSaindo...\n")
            break
        else:
            print("\n❌ Opção inválida.\n")


# ==============================
# INÍCIO DO PROGRAMA
# ==============================

if __name__ == "__main__":
    menu()