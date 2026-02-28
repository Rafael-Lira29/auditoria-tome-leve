import streamlit as st
import pandas as pd
import sqlite3
import re
import unicodedata
from rapidfuzz import process, fuzz
import xml.etree.ElementTree as ET
import io
from datetime import datetime
from openpyxl import Workbook
from openpyxl.styles import PatternFill, Font, Alignment

# --- CONFIGURAÇÃO VISUAL DO APP ---
st.set_page_config(page_title="FLV Enterprise - Tome Leve", page_icon="🍎", layout="wide")

st.markdown("""
    <style>
    .main { background-color: #f8f9fa; }
    div.stButton > button:first-child {
        background-color: #002060; color: white; height: 3em; font-weight: bold; width: 100%; border-radius: 8px;
    }
    div.stButton > button:first-child:hover { background-color: #00133d; }
    </style>
""", unsafe_allow_html=True)

st.title("🍎 Sistema Integrado FLV Enterprise")

# ==========================================================
# CÉREBRO LÓGICO DA AUDITORIA E TAXONOMIA
# ==========================================================
NAMESPACE_NFE = {'nfe': 'http://www.portalfiscal.inf.br/nfe'}
DB_NAME = "auditoria_flv_app.db"
TOLERANCIA_DIF = 0.001

def normalizar(texto):
    if pd.isna(texto) or texto is None: return ""
    texto = str(texto).upper().strip()
    texto = unicodedata.normalize('NFKD', texto).encode('ASCII', 'ignore').decode('ASCII')
    return re.sub(r'[^\w\s\.\-]', '', texto)

def descobrir_loja(cnpj_dest, nome_dest):
    nome = normalizar(nome_dest)
    cnpj = ''.join(filter(str.isdigit, str(cnpj_dest)))
    if 'LOJA 01' in nome or 'LOJA-1' in nome or '( 01 )' in nome: return 'Loja_1'
    if 'LOJA 02' in nome or 'LOJA-2' in nome or '( 02 )' in nome: return 'Loja_2'
    if 'LOJA 03' in nome or 'LOJA-3' in nome or '( 03 )' in nome: return 'Loja_3'
    if 'LOJA 05' in nome or 'LOJA-5' in nome or '( 05 )' in nome: return 'Loja_5'
    if cnpj.endswith('000100'): return 'Loja_1'
    if cnpj.endswith('000363'): return 'Loja_2'
    if cnpj.endswith('000444'): return 'Loja_3'
    if cnpj.endswith('000606'): return 'Loja_5'
    if cnpj.endswith('000101'): return 'Loja_6'
    if cnpj.endswith('000365'): return 'Loja_7'
    if 'BARRETOS' in nome: return 'Loja_6'
    if 'COLINA' in nome or 'ANGELICOLA' in nome or cnpj.endswith('000184'): return 'Loja_8'
    return 'Loja_Desconhecida'

def traduzir_fornecedor(nome_bruto):
    nome = normalizar(nome_bruto)
    if 'RASTEIRA' in nome or 'RIBER' in nome: return 'RIBER FRUTAS'
    if 'HERCULES' in nome or 'RICARDO' in nome: return 'RICARDO'
    if 'CLAUDIO MARCELO' in nome or 'MARCELO' in nome: return 'MARCELO MILHO'
    if '2A COMERCIO' in nome or 'PIMENTA' in nome or '2 A COMERCIO' in nome: return 'IRMAOS PIMENTA'
    if 'ND COMERCIO' in nome or ' ND ' in f" {nome} " or nome == 'ND' or 'N D COM' in nome or 'N.D' in nome: return 'ND'
    if 'NICOLETI' in nome: return 'NICOLETI'
    if 'COAL' in nome or 'ARANDA' in nome: return 'COAL'
    if 'DRUB' in nome or 'ADILSON' in nome: return 'DRUB'
    if 'ZERO' in nome.split() or 'FRUTAS ZERO' in nome: return 'FRUTAS ZERO'
    if 'TAIS' in nome.split(): return 'TAIS'
    if 'LUCIO' in nome: return 'LUCIO ORLANDO'
    return nome.replace("FORNECEDOR", "").strip()

def descobrir_familia(nome):
    n = normalizar(nome)
    if "MELANCIA" in n and ("BABY" in n or "MINI" in n): return "MELANCIA_BABY"
    if "MELANCIA" in n: return "MELANCIA"
    if "BANANA" in n and "NANICA" in n: return "BANANA_NANICA"
    if "BANANA" in n and "PRATA" in n: return "BANANA_PRATA"
    if "BANANA" in n and "MACA" in n: return "BANANA_MACA"
    if "BANANA" in n and "MARMELO" in n: return "BANANA_MARMELO"
    if "BANANA" in n and "DA TERRA" in n: return "BANANA_TERRA"
    if "BANANA" in n: return "BANANA_OUTRA"
    if "BATATA" in n and "DOCE" in n: return "BATATA_DOCE"
    if "BATATA" in n: return "BATATA"
    if "CEBOLA" in n and "ROXA" in n: return "CEBOLA_ROXA"
    if "CEBOLA" in n: return "CEBOLA"
    if "ALHO" in n and "ROXO" in n: return "ALHO_ROXO"
    if "ALHO" in n: return "ALHO"
    if "PIMENTAO" in n and "VERMELHO" in n: return "PIMENTAO_VERMELHO"
    if "PIMENTAO" in n and "AMARELO" in n: return "PIMENTAO_AMARELO"
    if "PIMENTAO" in n and "VERDE" in n: return "PIMENTAO_VERDE"
    if "COLORIDO" in n or "COLCORIDO" in n: return "PIMENTAO_COLORIDO"
    if "PIMENTAO" in n: return "PIMENTAO_OUTRO"
    if "PAPAIA" in n or "PAPAYA" in n: return "MAMAO_PAPAIA"
    if "FORMOSA" in n: return "MAMAO_FORMOSA"
    if "MAMAO" in n: return "MAMAO_OUTRO"
    if "MORANGO" in n or "MORANGUINHO" in n: return "MORANGO"

    regras_simples = [
        "MELAO", "LARANJA", "LIMAO", "TANGERINA", "PONKAN", "MURCOTE",
        "CHUCHU", "CENOURA", "BETERRABA", "BERINJELA", "REPOLHO", "COUVE FLOR", "COUVE",
        "BROCOLIS", "TOMATE", "MACA", "PERA", "MANGA", "ABACATE", "ABACAXI", "QUIABO",
        "PEPINO", "MARACUJA", "MILHO", "VAGEM", "JILO", "KIWI", "GENGIBRE", "GOIABA",
        "INHAME", "SALSAO", "RABANETE", "AIPIM"
    ]
    for r in regras_simples:
        if r in n:
            if r in ["PONKAN", "MURCOTE"]: return "TANGERINA"
            return r

    if "ABOBRINHA" in n or "ABOBORA" in n or "CABOTIA" in n: return "ABOBORA_ABOBRINHA"
    return n.split()[0] if n else ""

def criar_banco():
    with sqlite3.connect(DB_NAME) as conn:
        cursor = conn.cursor()
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS auditoria (
            id INTEGER PRIMARY KEY AUTOINCREMENT, id_execucao TEXT, loja TEXT, fornecedor TEXT,
            produto_pedido TEXT, produto_xml TEXT, qtd_pedido REAL, qtd_nota REAL,
            diferenca REAL, status_visual TEXT, status_codigo INTEGER
        )
        """)
        conn.commit()

def classificar(qtd_ped, qtd_fat, tipo):
    if tipo == "SEM_FORNECEDOR": return ("⚪ SEM NOTA P/ FORNECEDOR", 98, -qtd_ped)
    if tipo == "SEM_PRODUTO": return ("⚪ PRODUTO NÃO FATURADO", 99, -qtd_ped)
    diferenca = qtd_fat - qtd_ped
    if abs(diferenca) < TOLERANCIA_DIF: return ("🟢 OK", 0, 0.0)
    if diferenca < 0: return (f"🔴 FALTA {abs(diferenca):.2f}".replace('.00',''), -1, diferenca)
    return (f"🟡 SOBRA {diferenca:.2f}".replace('.00',''), 1, diferenca)

def gerar_excel_auditoria(df_final):
    df_final = df_final.fillna("")
    wb = Workbook()
    wb.remove(wb.active)

    for loja in sorted(df_final['loja'].unique()):
        df_loja = df_final[df_final['loja'] == loja].copy()
        ws = wb.create_sheet(title=loja)
        ws.append([f"AUDITORIA - {loja.upper().replace('_', ' ')}"])
        ws.merge_cells('A1:E1')
        ws['A1'].fill = PatternFill(start_color="000000", end_color="000000", fill_type="solid")
        ws['A1'].font = Font(color="FFFFFF", bold=True, size=14)
        ws['A1'].alignment = Alignment(horizontal="center", vertical="center")
        ws.append(['Produto Pedido', 'Qtd Pedida', 'Produto na Nota (XML)', 'Qtd Nota', 'Status'])
        for cell in ws[2]: cell.font = Font(bold=True)

        current_forn = None
        for _, row in df_loja.iterrows():
            if row['fornecedor'] != current_forn:
                if current_forn is not None: ws.append([])
                current_forn = row['fornecedor']
                ws.append([f"Fornecedor: {current_forn}", "", "", "", ""])
                ws.merge_cells(start_row=ws.max_row, start_column=1, end_row=ws.max_row, end_column=5)
                for cell in ws[ws.max_row]:
                    if "FATURADO SEM PEDIDO" in current_forn:
                        cell.fill = PatternFill(start_color="E4DFEC", end_color="E4DFEC", fill_type="solid")
                        cell.font = Font(color="60497A", bold=True)
                    else:
                        cell.fill = PatternFill(start_color="D9E1F2", end_color="D9E1F2", fill_type="solid")
                        cell.font = Font(color="002060", bold=True)

            ws.append([row['produto_pedido'], row['qtd_pedido'], row['produto_xml'], row['qtd_nota'], row['status_visual']])
            status_cell = ws.cell(row=ws.max_row, column=5)
            val = status_cell.value
            if val:
                if "🟢" in val: status_cell.fill = PatternFill(start_color="C6EFCE", end_color="C6EFCE", fill_type="solid")
                elif "🔴" in val: status_cell.fill = PatternFill(start_color="FFC7CE", end_color="FFC7CE", fill_type="solid")
                elif "🟡" in val: status_cell.fill = PatternFill(start_color="FFEB9C", end_color="FFEB9C", fill_type="solid")
                elif "🔵" in val: status_cell.fill = PatternFill(start_color="B4C6E7", end_color="B4C6E7", fill_type="solid")
                elif "⚪" in val: status_cell.fill = PatternFill(start_color="F2F2F2", end_color="F2F2F2", fill_type="solid")
                elif "🟣" in val: status_cell.fill = PatternFill(start_color="E4DFEC", end_color="E4DFEC", fill_type="solid")

        for col in ['A', 'C']: ws.column_dimensions[col].width = 45
        for col in ['B', 'D']: ws.column_dimensions[col].width = 15
        ws.column_dimensions['E'].width = 45
    return wb

def gerar_dashboard_operacional(id_execucao):
    with sqlite3.connect(DB_NAME) as conn:
        df = pd.read_sql("SELECT * FROM auditoria WHERE id_execucao = ?", conn, params=[id_execucao])
    df_problemas = df[df['status_codigo'].isin([-1, 1, 98, 99])]
    if df_problemas.empty: return None

    resumo = df_problemas.groupby(['loja','fornecedor']).agg(
        Itens_Divergentes=('id','count'), Saldo_Kg_Divergente=('diferenca','sum')
    ).reset_index().sort_values(by=["loja", "Saldo_Kg_Divergente"])

    wb_dash = Workbook()
    ws_dash = wb_dash.active
    ws_dash.title = "Resumo_Estoque"
    ws_dash.append(["DASHBOARD OPERACIONAL - DIVERGÊNCIAS DE ESTOQUE"])
    ws_dash.merge_cells('A1:D1')
    ws_dash['A1'].fill = PatternFill(start_color="002060", end_color="002060", fill_type="solid")
    ws_dash['A1'].font = Font(color="FFFFFF", bold=True, size=14)
    ws_dash['A1'].alignment = Alignment(horizontal="center", vertical="center")

    ws_dash.append(['Loja', 'Fornecedor (Aba do Pedido)', 'Itens c/ Erro na Doca', 'Falta/Sobra (Kg ou Unid)'])
    for cell in ws_dash[2]:
        cell.font = Font(bold=True)
        cell.fill = PatternFill(start_color="D9D9D9", end_color="D9D9D9", fill_type="solid")

    loja_atual = None
    for _, row in resumo.iterrows():
        if row['loja'] != loja_atual:
            loja_atual = row['loja']
            ws_dash.append([f"⯈ {loja_atual.upper().replace('_', ' ')}", "", "", ""])
            ws_dash.merge_cells(start_row=ws_dash.max_row, start_column=1, end_row=ws_dash.max_row, end_column=4)
            ws_dash.cell(row=ws_dash.max_row, column=1).fill = PatternFill(start_color="D9E1F2", end_color="D9E1F2", fill_type="solid")
            ws_dash.cell(row=ws_dash.max_row, column=1).font = Font(color="002060", bold=True)

        ws_dash.append([row['loja'], row['fornecedor'], row['Itens_Divergentes'], row['Saldo_Kg_Divergente']])
        saldo_cell = ws_dash.cell(row=ws_dash.max_row, column=4)
        if saldo_cell.value and float(saldo_cell.value) < 0:
            saldo_cell.font = Font(color="9C0006", bold=True)
            saldo_cell.fill = PatternFill(start_color="FFC7CE", end_color="FFC7CE", fill_type="solid")
        elif saldo_cell.value and float(saldo_cell.value) > 0:
            saldo_cell.font = Font(color="006100", bold=True)
            saldo_cell.fill = PatternFill(start_color="C6EFCE", end_color="C6EFCE", fill_type="solid")

    ws_dash.column_dimensions['A'].width = 15
    ws_dash.column_dimensions['B'].width = 35
    ws_dash.column_dimensions['C'].width = 25
    ws_dash.column_dimensions['D'].width = 30
    return wb_dash

# ==========================================================
# CRIAÇÃO DAS ABAS (TABS) DA INTERFACE
# ==========================================================
aba_preparador, aba_auditoria = st.tabs(["🧹 1. Preparador de Pedidos", "🍎 2. Auditoria de XMLs"])

# ----------------------------------------------------------
# TELA 1: O SEU CÓDIGO DE FILTRAGEM E MAPEAMENTO DINÂMICO
# ----------------------------------------------------------
with aba_preparador:
    st.header("🧹 Preparador de Planilha do Comprador")
    st.write("Insira a planilha bruta recebida do comprador para limpá-la, aplicar o design corporativo e isolar suas docas (Loja 5 removida).")
    
    arquivo_bruto = st.file_uploader("Arraste a Planilha Bruta (CSV ou Excel) aqui", type=['csv', 'xlsx'], key="uploader_bruto")
    
    if st.button("Limpar e Preparar Planilha"):
        if not arquivo_bruto:
            st.warning("Envie a planilha bruta primeiro.")
        else:
            with st.spinner("Lendo estrutura, mapeando colunas e aplicando blindagem..."):
                try:
                    nome_planilha = arquivo_bruto.name
                    if nome_planilha.endswith('.csv'):
                        df_bruto = pd.read_csv(arquivo_bruto, header=None)
                    else:
                        todas_as_abas = pd.read_excel(arquivo_bruto, sheet_name=None, header=None)
                        abas_validas = []
                        for nome_aba, df_aba in todas_as_abas.items():
                            if str(nome_aba).lower() in ['ped', 'com', 'sis'] or str(nome_aba).isdigit():
                                continue
                            abas_validas.append(df_aba)
                        if abas_validas:
                            df_bruto = pd.concat(abas_validas, ignore_index=True)
                        else:
                            df_bruto = pd.read_excel(arquivo_bruto, header=None)

                    lojas_alvo = {}
                    coluna_padrao = -1
                    coluna_custo = -1

                    for index, row in df_bruto.head(50).iterrows():
                        for col_idx, val in enumerate(row):
                            texto = str(val).strip().upper()
                            if texto == 'L1': lojas_alvo['Loja_1'] = col_idx
                            elif texto == 'L2': lojas_alvo['Loja_2'] = col_idx
                            elif texto == 'L3': lojas_alvo['Loja_3'] = col_idx
                            # 🛡️ TRAVA SEXTA-FEIRA APLICADA NA RAIZ: Ignora a L5 se encontrar
                            elif texto == 'L6': lojas_alvo['Loja_6'] = col_idx
                            elif texto == 'L7': lojas_alvo['Loja_7'] = col_idx
                            elif texto == 'L8': lojas_alvo['Loja_8'] = col_idx
                            elif 'PADRÃO' in texto or 'PADRAO' in texto: coluna_padrao = col_idx
                            elif 'CUSTO' in texto: coluna_custo = col_idx

                        if lojas_alvo: 
                            break

                    max_col = df_bruto.shape[1]
                    if coluna_padrao == -1: coluna_padrao = 10 if max_col > 10 else (max_col - 1)
                    if coluna_custo == -1: coluna_custo = 11 if max_col > 11 else (max_col - 1)

                    fornecedor_atual = "DESCONHECIDO"
                    cod_fornecedor_atual = "-"
                    lista_fornecedores = []
                    lista_codigos = []

                    for index, row in df_bruto.iterrows():
                        col0_str = str(row[0]).strip().upper()
                        col1_str = str(row[1]).strip()

                        if "PEDIDO FLV" in col0_str:
                            nome_sujo = col0_str.replace("PEDIDO FLV", "").split("202")[0].replace(",", "").strip()
                            fornecedor_atual = nome_sujo if nome_sujo else "FORNECEDOR"
                        elif "CÓD" in col0_str and "FORN" in col0_str:
                            cod_fornecedor_atual = col1_str

                        lista_fornecedores.append(fornecedor_atual)
                        lista_codigos.append(cod_fornecedor_atual)

                    df_bruto['Fornecedor'] = lista_fornecedores
                    df_bruto['Cod_Fornecedor'] = lista_codigos

                    df_bruto[0] = pd.to_numeric(df_bruto[0], errors='coerce')
                    df_dados = df_bruto.dropna(subset=[0]).copy()
                    df_dados[0] = df_dados[0].astype(int)

                    wb = Workbook()
                    wb.remove(wb.active)

                    fill_loja = PatternFill(start_color="000000", end_color="000000", fill_type="solid")
                    font_loja = Font(color="FFFFFF", bold=True, size=14)
                    align_center = Alignment(horizontal="center", vertical="center")
                    fill_fornecedor = PatternFill(start_color="D9E1F2", end_color="D9E1F2", fill_type="solid")
                    font_fornecedor = Font(color="002060", bold=True, size=11)
                    font_cabecalho = Font(bold=True)

                    for nome_loja, indice_coluna in lojas_alvo.items():
                        if indice_coluna >= max_col: continue

                        df_loja = df_dados[[0, 1, 'Cod_Fornecedor', 'Fornecedor', indice_coluna, coluna_padrao, coluna_custo]].copy()
                        df_loja.columns = ['Código', 'Descrição', 'Cod_Fornecedor', 'Fornecedor', 'Qtd_Pedida', 'Padrão_Cx', 'Custo']

                        df_loja['Qtd_Pedida'] = pd.to_numeric(df_loja['Qtd_Pedida'], errors='coerce')
                        df_loja['Custo'] = pd.to_numeric(df_loja['Custo'], errors='coerce').fillna(0)
                        df_loja = df_loja[df_loja['Qtd_Pedida'] > 0]

                        if df_loja.empty: continue

                        df_loja = df_loja.sort_values(by=['Fornecedor', 'Descrição'])
                        ws = wb.create_sheet(title=nome_loja)

                        ws.append([f"CONFERÊNCIA - {nome_loja.upper().replace('_', ' ')}"])
                        ws.merge_cells('A1:E1')
                        ws['A1'].fill = fill_loja
                        ws['A1'].font = font_loja
                        ws['A1'].alignment = align_center

                        ws.append(['Código', 'Descrição', 'Qtd_Pedida', 'Padrão_Cx', 'Custo'])
                        for cell in ws[2]: cell.font = font_cabecalho

                        current_forn = None
                        for _, row in df_loja.iterrows():
                            if row['Fornecedor'] != current_forn:
                                if current_forn is not None: ws.append([])
                                current_forn = row['Fornecedor']
                                cod_forn = row['Cod_Fornecedor']

                                ws.append([f"Fornecedor: {cod_forn} - {current_forn}", "", "", "", ""])
                                row_idx = ws.max_row
                                ws.merge_cells(start_row=row_idx, start_column=1, end_row=row_idx, end_column=5)
                                for cell in ws[row_idx]:
                                    cell.fill = fill_fornecedor
                                    cell.font = font_fornecedor

                            ws.append([row['Código'], row['Descrição'], row['Qtd_Pedida'], row['Padrão_Cx'], row['Custo']])
                            celula_custo = ws.cell(row=ws.max_row, column=5)
                            celula_custo.number_format = 'R$ #,##0.00'

                        ws.column_dimensions['A'].width = 12
                        ws.column_dimensions['B'].width = 45
                        ws.column_dimensions['C'].width = 15
                        ws.column_dimensions['D'].width = 15
                        ws.column_dimensions['E'].width = 15

                    out_excel = io.BytesIO()
                    wb.save(out_excel)
                    
                    st.success("✨ Planilha preparada com sucesso! Baixe o arquivo abaixo e use-o na Aba 2 (Auditoria).")
                    st.download_button(
                        label="📥 Baixar Planilha de Pedidos Blindada", 
                        data=out_excel.getvalue(), 
                        file_name=f"Pedidos_FLV_Blindado_{datetime.now().strftime('%Y%m%d')}.xlsx", 
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )

                except Exception as e:
                    st.error(f"❌ Erro ao processar a planilha bruta: {e}")

# ----------------------------------------------------------
# TELA 2: A AUDITORIA (MOTOR V21)
# ----------------------------------------------------------
with aba_auditoria:
    st.header("🍎 Auditoria de XMLs (Cruzamento Inteligente)")
    
    col1, col2 = st.columns(2)
    with col1:
        st.subheader("1. Planilha de Pedidos")
        arquivo_excel = st.file_uploader("Arraste o Excel de Pedidos (Gerado na Aba 1) aqui", type=['xlsx'], key="uploader_pedidos")
    with col2:
        st.subheader("2. Notas Fiscais (XML)")
        arquivos_xml = st.file_uploader("Arraste os XMLs dos Fornecedores", type=['xml'], accept_multiple_files=True, key="uploader_xmls")

    if st.button("Executar Auditoria Implacável"):
        if not arquivo_excel or not arquivos_xml:
            st.warning("⚠️ Você precisa enviar a planilha de pedidos e pelo menos um arquivo XML.")
        else:
            with st.spinner("Sexta-feira processando os dados e rastreando sobras de estoque..."):
                criar_banco()
                id_execucao = datetime.now().strftime("%Y%m%d%H%M%S")

                try:
                    df_pedidos_raw = pd.read_excel(arquivo_excel, sheet_name=None, header=None)
                    pedidos_lista = []
                    for aba, df in df_pedidos_raw.items():
                        forn_orig, forn_macro = "DESCONHECIDO", "DESCONHECIDO"
                        for _, row in df.iterrows():
                            col0 = str(row[0]).strip()
                            if col0.startswith("Fornecedor:"):
                                forn_orig = col0.replace("Fornecedor:", "").strip()
                                forn_macro = traduzir_fornecedor(forn_orig)
                            else:
                                val = pd.to_numeric(col0, errors='coerce')
                                if pd.notna(val) and val > 0:
                                    pedidos_lista.append({
                                        'Loja': aba, 'Fornecedor_Original': forn_orig, 'Fornecedor_Macro': forn_macro,
                                        'Produto': normalizar(row[1]), 'Qtd': float(row[2]) if pd.notna(row[2]) else 0.0
                                    })
                    df_pedidos = pd.DataFrame(pedidos_lista)
                    
                    df_pedidos = df_pedidos[~df_pedidos['Loja'].astype(str).str.upper().str.contains('5')]
                    df_pedidos = df_pedidos.groupby(['Loja', 'Fornecedor_Original', 'Fornecedor_Macro', 'Produto'], as_index=False)['Qtd'].sum()

                    notas = []
                    for xml_file in arquivos_xml:
                        try:
                            tree = ET.parse(io.BytesIO(xml_file.read()))
                            root = tree.getroot()
                            inf = root.find('.//nfe:infNFe', NAMESPACE_NFE)
                            if inf is None: continue
                            
                            emit_node = inf.find('nfe:emit/nfe:xNome', NAMESPACE_NFE)
                            dest_node = inf.find('nfe:dest/nfe:CNPJ', NAMESPACE_NFE)
                            dest_nome_node = inf.find('nfe:dest/nfe:xNome', NAMESPACE_NFE)
                            
                            fornecedor_xml_macro = traduzir_fornecedor(emit_node.text) if emit_node is not None else "DESCONHECIDO"
                            cnpj_xml = dest_node.text if dest_node is not None else "0"
                            nome_xml_dest = dest_nome_node.text if dest_nome_node is not None else ""
                            loja_xml = descobrir_loja(cnpj_xml, nome_xml_dest)

                            for det in inf.findall('nfe:det', NAMESPACE_NFE):
                                prod_node = det.find('nfe:prod/nfe:xProd', NAMESPACE_NFE)
                                qtd_node = det.find('nfe:prod/nfe:qCom', NAMESPACE_NFE)
                                if prod_node is None or qtd_node is None: continue
                                notas.append({
                                    "Loja": loja_xml, "Fornecedor_Macro": fornecedor_xml_macro,
                                    "Produto": normalizar(prod_node.text), "Qtd": float(qtd_node.text)
                                })
                        except Exception as e: pass
                        
                    df_notas = pd.DataFrame(notas)
                    
                    if not df_notas.empty:
                        df_notas = df_notas[df_notas['Loja'] != 'Loja_5']
                        df_notas_agg = df_notas.groupby(['Loja', 'Fornecedor_Macro', 'Produto'], as_index=False)['Qtd'].sum()
                    else:
                        df_notas_agg = pd.DataFrame()

                    registros = []
                    for (loja, forn_macro), df_ped_group in df_pedidos.groupby(['Loja', 'Fornecedor_Macro']):
                        if df_notas_agg.empty:
                            notas_forn = pd.DataFrame()
                        else:
                            notas_forn = df_notas_agg[(df_notas_agg['Loja'] == loja) & (df_notas_agg['Fornecedor_Macro'] == forn_macro)]

                        if notas_forn.empty:
                            for _, ped in df_ped_group.iterrows():
                                stat_v, stat_c, dif = classificar(ped['Qtd'], 0, "SEM_FORNECEDOR")
                                registros.append((id_execucao, loja, ped['Fornecedor_Original'], ped['Produto'], "❌ NOTA NÃO ENCONTRADA", ped['Qtd'], 0, dif, stat_v, stat_c))
                            continue

                        matched_ped_idx = set()
                        matched_xml_idx = set()
                        pairs = []
                        
                        for idx_ped, ped in df_ped_group.iterrows():
                            familia_ped = descobrir_familia(ped['Produto'])
                            fam_ampla_ped = familia_ped.split('_')[0] if familia_ped else ""
                            
                            for idx_xml, nota in notas_forn.iterrows():
                                prod_xml = nota['Produto']
                                familia_xml = descobrir_familia(prod_xml)
                                fam_ampla_xml = familia_xml.split('_')[0] if familia_xml else ""
                                
                                for root_word in ["MELANCIA", "BATATA", "CEBOLA", "ALHO"]:
                                    if root_word in fam_ampla_ped or root_word in fam_ampla_xml:
                                        fam_ampla_ped = familia_ped
                                        fam_ampla_xml = familia_xml
                                
                                if familia_ped == familia_xml or fam_ampla_ped == fam_ampla_xml:
                                    score = fuzz.token_sort_ratio(ped['Produto'], prod_xml)
                                    pairs.append((score, idx_ped, idx_xml, prod_xml, ped['Qtd'], nota['Qtd']))
                                    
                        pairs.sort(key=lambda x: x[0], reverse=True)
                        
                        for score, idx_ped, idx_xml, prod_xml, qtd_ped, qtd_fat in pairs:
                            if idx_ped not in matched_ped_idx and idx_xml not in matched_xml_idx:
                                matched_ped_idx.add(idx_ped)
                                matched_xml_idx.add(idx_xml)
                                
                                ped = df_ped_group.loc[idx_ped]
                                if forn_macro == "DRUB" and "PIMENTAO" in descobrir_familia(ped['Produto']):
                                    stat_v, stat_c, dif = "🔵 AVALIAR PESO (PEDIDO EM UN vs XML EM KG)", 0, 0.0
                                else:
                                    stat_v, stat_c, dif = classificar(qtd_ped, qtd_fat, "OK")
                                registros.append((id_execucao, loja, ped['Fornecedor_Original'], ped['Produto'], prod_xml, qtd_ped, qtd_fat, dif, stat_v, stat_c))

                        for idx_ped, ped in df_ped_group.iterrows():
                            if idx_ped not in matched_ped_idx:
                                stat_v, stat_c, dif = classificar(ped['Qtd'], 0, "SEM_PRODUTO")
                                registros.append((id_execucao, loja, ped['Fornecedor_Original'], ped['Produto'], "❌ PRODUTO NÃO FATURADO", ped['Qtd'], 0, dif, stat_v, stat_c))
                        
                        for idx_xml, nota in notas_forn.iterrows():
                            if idx_xml not in matched_xml_idx:
                                prod_xml = nota['Produto']
                                qtd_fat = nota['Qtd']
                                fornecedor_extra = f"⚠️ {forn_macro} - FATURADO SEM PEDIDO"
                                qtd_format = f"{qtd_fat:.2f}".replace('.00','')
                                stat_v, stat_c, dif = f"🟣 SEM PEDIDO (SOBRA {qtd_format})", 1, qtd_fat
                                registros.append((id_execucao, loja, fornecedor_extra, "❌ NÃO SOLICITADO", prod_xml, 0, qtd_fat, qtd_fat, stat_v, stat_c))

                    if registros:
                        df_final = pd.DataFrame(registros, columns=[
                            'id_execucao','loja','fornecedor','produto_pedido','produto_xml',
                            'qtd_pedido','qtd_nota','diferenca','status_visual','status_codigo'
                        ])
                        
                        df_final.sort_values(by=['loja', 'fornecedor', 'produto_pedido'], inplace=True)
                        
                        with sqlite3.connect(DB_NAME) as conn:
                            df_final.to_sql("auditoria", conn, if_exists="append", index=False)
                        
                        st.success("✅ Auditoria Concluída com Precisão Matemática! Baixe os relatórios abaixo:")
                        
                        wb_audit = gerar_excel_auditoria(df_final)
                        out_audit = io.BytesIO()
                        wb_audit.save(out_audit)
                        
                        wb_dash = gerar_dashboard_operacional(id_execucao)
                        
                        col_btn1, col_btn2 = st.columns(2)
                        with col_btn1:
                            st.download_button(label="📥 Baixar Auditoria Visual", data=out_audit.getvalue(), file_name=f"Auditoria_{id_execucao}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
                        
                        if wb_dash:
                            out_dash = io.BytesIO()
                            wb_dash.save(out_dash)
                            with col_btn2:
                                st.download_button(label="📊 Baixar Dashboard Operacional", data=out_dash.getvalue(), file_name=f"Dashboard_{id_execucao}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
                    else:
                        st.error("❌ Erro no cruzamento. Nenhum dado foi processado.")
                except Exception as e:
                    st.error(f"❌ Erro crítico: {e}")
