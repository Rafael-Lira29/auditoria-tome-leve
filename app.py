import streamlit as st
import pandas as pd
import re
import unicodedata
from rapidfuzz import process, fuzz
import xml.etree.ElementTree as ET
import io
import time
from datetime import datetime
from openpyxl import Workbook
from openpyxl.styles import PatternFill, Font, Alignment
import os

# --- MOTOR COGNITIVO ---
try:
    import google.generativeai as genai
    HAS_IA = True
except ImportError:
    HAS_IA = False

# --- CONFIGURAÇÃO VISUAL DO APP ---
st.set_page_config(page_title="FLV Enterprise - Tome Leve", page_icon="🍎", layout="wide")

st.markdown("""
    <style>
    div.stButton > button:first-child {
        background-color: #002060; color: white; height: 3em; font-weight: bold; width: 100%; border-radius: 8px;
    }
    div.stButton > button:first-child:hover { background-color: #00133d; }
    </style>
""", unsafe_allow_html=True)

st.title("🍎 Sistema Integrado FLV Enterprise")

# ==========================================================
# CÉREBRO LÓGICO E TAXONOMIA
# ==========================================================
NAMESPACE_NFE = {'nfe': 'http://www.portalfiscal.inf.br/nfe'}
TOLERANCIA_DIF = 0.001

def normalizar(texto):
    if pd.isna(texto) or texto is None: return ""
    texto = str(texto).upper().strip()
    texto = unicodedata.normalize('NFKD', texto).encode('ASCII', 'ignore').decode('ASCII')
    texto = re.sub(r'[^\w\s\.\-]', '', texto)
    if "MACA GALA GRANEL P" in texto: return "MACA BABY KG"
    if "MACA GALA PREMIUM" in texto or "TP 135" in texto: return "MACA GALA KG"
    return texto

def descobrir_loja(cnpj_dest, nome_dest):
    nome = normalizar(nome_dest)
    cnpj = ''.join(filter(str.isdigit, str(cnpj_dest)))
    if 'LOJA 01' in nome or 'LOJA-1' in nome or '( 01 )' in nome or cnpj.endswith('000100'): return 'Loja_1'
    if 'LOJA 02' in nome or 'LOJA-2' in nome or '( 02 )' in nome or cnpj.endswith('000363'): return 'Loja_2'
    if 'LOJA 03' in nome or 'LOJA-3' in nome or '( 03 )' in nome or cnpj.endswith('000444'): return 'Loja_3'
    if 'LOJA 05' in nome or 'LOJA-5' in nome or '( 05 )' in nome or cnpj.endswith('000606'): return 'Loja_5'
    if cnpj.endswith('000101') or 'BARRETOS' in nome: return 'Loja_6'
    if cnpj.endswith('000365'): return 'Loja_7'
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

def classificar(qtd_ped, qtd_fat, tipo):
    if tipo == "SEM_FORNECEDOR": return ("⚪ SEM NFe P/ FORN", 98, -qtd_ped)
    if tipo == "SEM_PRODUTO": return ("⚪ PRODUTO NÃO FATURADO", 99, -qtd_ped)
    diferenca = qtd_fat - qtd_ped
    if abs(diferenca) < TOLERANCIA_DIF: return ("🟢 OK", 0, 0.0)
    if diferenca < 0: return (f"🔴 NFe FALTA {abs(diferenca):.2f}".replace('.00',''), -1, diferenca)
    return (f"🟡 NFe SOBRA {diferenca:.2f}".replace('.00',''), 1, diferenca)

def classificar_doca(qtd_xml, qtd_fisico):
    diferenca = qtd_fisico - qtd_xml
    if abs(diferenca) < TOLERANCIA_DIF: return "🟢 FÍSICO BATEU"
    if diferenca < 0: return f"🔴 FÍSICO FALTOU {abs(diferenca):.2f}".replace('.00','')
    return f"🟡 FÍSICO SOBROU {diferenca:.2f}".replace('.00','')

def analisar_com_ia(produto, diferenca_negativa, texto_infcpl):
    if not HAS_IA or "GEMINI_API_KEY" not in st.secrets: return False, "IA Desligada/Sem Chave"
    if not texto_infcpl or len(texto_infcpl.strip()) < 5: return False, ""
    try:
        genai.configure(api_key=st.secrets["GEMINI_API_KEY"])
        model = genai.GenerativeModel("gemini-1.5-flash")
        prompt = f"Houve FALTA de {abs(diferenca_negativa)} de '{produto}'. O fornecedor escreveu na nota: '{texto_infcpl}'. Isso justifica a falta? Responda ESTRITAMENTE: [SIM] ou [NAO] - Justificativa em 10 palavras."
        resposta = model.generate_content(prompt).text.strip()
        if resposta.startswith("[SIM]"): return True, resposta
        return False, resposta
    except Exception as e: return False, "Erro IA"

# ==========================================================
# GERADOR DE EXCEL TRIPLO
# ==========================================================
def gerar_excel_auditoria(df_final):
    df_final = df_final.fillna("")
    wb = Workbook()
    wb.remove(wb.active)

    for loja in sorted(df_final['loja'].unique()):
        df_loja = df_final[df_final['loja'] == loja].copy()
        ws = wb.create_sheet(title=loja)
        ws.append([f"AUDITORIA DEFINITIVA - {loja.upper().replace('_', ' ')}"])
        ws.merge_cells('A1:J1')
        ws['A1'].fill = PatternFill(start_color="000000", end_color="000000", fill_type="solid")
        ws['A1'].font = Font(color="FFFFFF", bold=True, size=14)
        ws['A1'].alignment = Alignment(horizontal="center", vertical="center")
        
        ws.append(['Produto Pedido', 'Qtd Ped', 'Produto NFe', 'Qtd NFe', 'Origem Dados', 'Status NFe', 'Obs NFe (IA)', 'Qtd Doca', 'Padrão Doca', 'Status Doca'])
        for cell in ws[2]: cell.font = Font(bold=True)

        current_forn = None
        for _, row in df_loja.iterrows():
            if row['fornecedor'] != current_forn:
                if current_forn is not None: ws.append([])
                current_forn = row['fornecedor']
                ws.append([f"Fornecedor: {current_forn}", "", "", "", "", "", "", "", "", ""])
                ws.merge_cells(start_row=ws.max_row, start_column=1, end_row=ws.max_row, end_column=10)
                for cell in ws[ws.max_row]:
                    cell.fill = PatternFill(start_color="D9E1F2", end_color="D9E1F2", fill_type="solid")
                    cell.font = Font(color="002060", bold=True)

            ws.append([
                row['produto_pedido'], row['qtd_pedido'], row['produto_xml'], row['qtd_nota'], row['origem_match'],
                row['status_visual'], row.get('justificativa_ia', ''), row['qtd_fisico'], row['padrao_fisico'], row['status_doca']
            ])
            
            # Formatação de Cores da Auditoria
            status_nfe_cell = ws.cell(row=ws.max_row, column=6)
            val_nfe = status_nfe_cell.value
            if val_nfe:
                if "🟢" in val_nfe: status_nfe_cell.fill = PatternFill(start_color="C6EFCE", end_color="C6EFCE", fill_type="solid")
                elif "🔴" in val_nfe: status_nfe_cell.fill = PatternFill(start_color="FFC7CE", end_color="FFC7CE", fill_type="solid")
                elif "🟡" in val_nfe: status_nfe_cell.fill = PatternFill(start_color="FFEB9C", end_color="FFEB9C", fill_type="solid")
                elif "🤖" in val_nfe: status_nfe_cell.fill = PatternFill(start_color="E4DFEC", end_color="E4DFEC", fill_type="solid")
                elif "⚪" in val_nfe: status_nfe_cell.fill = PatternFill(start_color="F2F2F2", end_color="F2F2F2", fill_type="solid")

        ws.column_dimensions['A'].width = 30
        ws.column_dimensions['C'].width = 30
        ws.column_dimensions['E'].width = 15 
        ws.column_dimensions['F'].width = 25
        ws.column_dimensions['G'].width = 35 
        ws.column_dimensions['J'].width = 25
    return wb

# ==========================================================
# INTERFACE PRINCIPAL
# ==========================================================
aba_preparador, aba_auditoria = st.tabs(["🧹 1. Preparador de Pedidos", "🍎 2. Auditoria 3 Vias (NFe x Doca)"])

# ----------------------------------------------------------
# TELA 1: PREPARADOR DE PLANILHA DO COMPRADOR
# ----------------------------------------------------------
with aba_preparador:
    st.header("🧹 Preparador de Planilha do Comprador")
    arquivo_bruto = st.file_uploader("Arraste a Planilha Bruta (CSV ou Excel)", type=['csv', 'xlsx'], key="uploader_bruto")
    if st.button("Limpar e Preparar Planilha"):
        if not arquivo_bruto:
            st.warning("Envie a planilha bruta primeiro.")
        else:
            with st.spinner("Lendo estrutura e blindando..."):
                try:
                    nome_planilha = arquivo_bruto.name
                    if nome_planilha.endswith('.csv'):
                        df_bruto = pd.read_csv(arquivo_bruto, header=None)
                    else:
                        todas_as_abas = pd.read_excel(arquivo_bruto, sheet_name=None, header=None)
                        abas_validas = [df for n, df in todas_as_abas.items() if str(n).lower() not in ['ped', 'com', 'sis'] and not str(n).isdigit()]
                        df_bruto = pd.concat(abas_validas, ignore_index=True) if abas_validas else pd.read_excel(arquivo_bruto, header=None)

                    lojas_alvo, coluna_padrao, coluna_custo = {}, -1, -1
                    for index, row in df_bruto.head(50).iterrows():
                        for col_idx, val in enumerate(row):
                            texto = str(val).strip().upper()
                            if texto == 'L1': lojas_alvo['Loja_1'] = col_idx
                            elif texto == 'L2': lojas_alvo['Loja_2'] = col_idx
                            elif texto == 'L3': lojas_alvo['Loja_3'] = col_idx
                            elif texto == 'L5': lojas_alvo['Loja_5'] = col_idx 
                            elif texto == 'L6': lojas_alvo['Loja_6'] = col_idx
                            elif texto == 'L7': lojas_alvo['Loja_7'] = col_idx
                            elif texto == 'L8': lojas_alvo['Loja_8'] = col_idx
                            elif 'PADRÃO' in texto or 'PADRAO' in texto: coluna_padrao = col_idx
                            elif 'CUSTO' in texto: coluna_custo = col_idx
                        if lojas_alvo: break

                    max_col = df_bruto.shape[1]
                    if coluna_padrao == -1: coluna_padrao = 10 if max_col > 10 else (max_col - 1)
                    if coluna_custo == -1: coluna_custo = 11 if max_col > 11 else (max_col - 1)

                    fornecedor_atual, cod_fornecedor_atual = "DESCONHECIDO", "-"
                    lista_fornecedores, lista_codigos = [], []
                    for index, row in df_bruto.iterrows():
                        col0_str, col1_str = str(row[0]).strip().upper(), str(row[1]).strip()
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
                    fill_fornecedor = PatternFill(start_color="D9E1F2", end_color="D9E1F2", fill_type="solid")
                    font_fornecedor = Font(color="002060", bold=True, size=11)

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
                        ws.append(['Código', 'Descrição', 'Qtd_Pedida', 'Padrão_Cx', 'Custo'])
                        for cell in ws[2]: cell.font = Font(bold=True)

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
                            ws.cell(row=ws.max_row, column=5).number_format = 'R$ #,##0.00'

                        ws.column_dimensions['B'].width = 45
                        for c in ['A','C','D','E']: ws.column_dimensions[c].width = 15

                    out_excel = io.BytesIO()
                    wb.save(out_excel)
                    st.success("✨ Planilha preparada com sucesso! Baixe e use na Auditoria ou envie para a Doca.")
                    st.download_button(label="📥 Baixar Planilha Blindada", data=out_excel.getvalue(), file_name=f"Pedidos_Blindados_{datetime.now().strftime('%Y%m%d')}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
                except Exception as e:
                    st.error(f"Erro no Preparador: {e}")

# ----------------------------------------------------------
# TELA 2: AUDITORIA 3 VIAS COM DE-PARA
# ----------------------------------------------------------
with aba_auditoria:
    st.header("🍎 Cruzamento Triplo com Dicionário De-Para")
    
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.subheader("1. Pedidos (Excel)")
        arquivo_excel = st.file_uploader("Arraste o Pedido", type=['xlsx'], key="up_ped")
    with col2:
        st.subheader("2. Notas (XMLs)")
        arquivos_xml = st.file_uploader("Arraste os XMLs", type=['xml'], accept_multiple_files=True, key="up_xml")
    with col3:
        st.subheader("3. Doca (Opcional)")
        arquivos_contagem = st.file_uploader("Arraste o Romaneio", type=['xlsx', 'csv'], accept_multiple_files=True, key="up_doca")
    with col4:
        st.subheader("4. De-Para Mestre")
        arquivo_depara = st.file_uploader("Base De-Para (Excel)", type=['xlsx'], key="up_depara")

    usar_ia = st.checkbox("🧠 Ativar Auditor IA (Ler justificativas de faltas no XML)", value=HAS_IA)

    if st.button("Executar Auditoria Implacável"):
        if not arquivo_excel or not arquivos_xml:
            st.warning("⚠️ Você precisa de pelo menos o Pedido e os XMLs.")
        else:
            with st.spinner("Sexta-feira processando inteligência de dados..."):
                id_execucao = datetime.now().strftime("%Y%m%d%H%M%S")

                try:
                    # --- CARREGANDO O DE-PARA NA MEMÓRIA ---
                    dict_depara = {}
                    if arquivo_depara:
                        df_dp = pd.read_excel(arquivo_depara)
                        for _, row in df_dp.iterrows():
                            cnpj = str(row.get('CNPJ_Fornecedor', '')).strip()
                            cod_xml = str(row.get('Cod_Produto_XML', '')).strip()
                            desc_int = str(row.get('Descricao_Interna', '')).strip()
                            fator = pd.to_numeric(row.get('Fator_de_Conversao', 1.0), errors='coerce')
                            if pd.isna(fator): fator = 1.0
                            
                            if pd.notna(row.get('Cod_Interno_TomeLeve')) and desc_int and desc_int != 'nan':
                                dict_depara[(cnpj, cod_xml)] = (desc_int, fator)

                    # --- LENDO PEDIDOS ---
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
                    if not df_pedidos.empty:
                        df_pedidos = df_pedidos.groupby(['Loja', 'Fornecedor_Original', 'Fornecedor_Macro', 'Produto'], as_index=False)['Qtd'].sum()

                    # --- LENDO XMLs + TRADUÇÃO DE-PARA ---
                    notas = []
                    textos_infcpl = {} 
                    
                    for xml_file in arquivos_xml:
                        try:
                            tree = ET.parse(io.BytesIO(xml_file.read()))
                            root = tree.getroot()
                            inf = root.find('.//nfe:infNFe', NAMESPACE_NFE)
                            if inf is None: continue
                            
                            emit_node = inf.find('nfe:emit/nfe:xNome', NAMESPACE_NFE)
                            cnpj_node = inf.find('nfe:emit/nfe:CNPJ', NAMESPACE_NFE)
                            dest_node = inf.find('nfe:dest/nfe:CNPJ', NAMESPACE_NFE)
                            dest_nome_node = inf.find('nfe:dest/nfe:xNome', NAMESPACE_NFE)
                            
                            forn_macro = traduzir_fornecedor(emit_node.text) if emit_node is not None else "DESCONHECIDO"
                            cnpj_forn = cnpj_node.text.strip() if cnpj_node is not None else ""
                            loja_xml = descobrir_loja(dest_node.text if dest_node is not None else "0", dest_nome_node.text if dest_nome_node is not None else "")
                            
                            infAdic = inf.find('nfe:infAdic/nfe:infCpl', NAMESPACE_NFE)
                            if infAdic is not None and infAdic.text:
                                textos_infcpl[(loja_xml, forn_macro)] = infAdic.text

                            for det in inf.findall('nfe:det', NAMESPACE_NFE):
                                prod_node = det.find('nfe:prod/nfe:xProd', NAMESPACE_NFE)
                                cod_node = det.find('nfe:prod/nfe:cProd', NAMESPACE_NFE)
                                qtd_node = det.find('nfe:prod/nfe:qCom', NAMESPACE_NFE)
                                
                                if prod_node is None or qtd_node is None: continue
                                
                                nome_xml = prod_node.text
                                cod_xml = cod_node.text.strip() if cod_node is not None else ""
                                qtd_xml = float(qtd_node.text)

                                origem_match = "XML (Fuzzy)"
                                nome_final = normalizar(nome_xml)
                                qtd_final = qtd_xml
                                
                                if (cnpj_forn, cod_xml) in dict_depara:
                                    desc_interna, fator = dict_depara[(cnpj_forn, cod_xml)]
                                    nome_final = normalizar(desc_interna)
                                    qtd_final = qtd_xml * fator
                                    origem_match = "De-Para ⚡"

                                notas.append({"Loja": loja_xml, "Fornecedor_Macro": forn_macro, "Produto": nome_final, "Qtd": qtd_final, "Origem": origem_match})
                        except: pass
                    
                    df_notas = pd.DataFrame(notas)
                    if not df_notas.empty:
                        df_notas_agg = df_notas.groupby(['Loja', 'Fornecedor_Macro', 'Produto', 'Origem'], as_index=False)['Qtd'].sum()
                    else: df_notas_agg = pd.DataFrame()

                    df_contagens = pd.DataFrame() # Simplificado na visualização, você pode recolocar sua leitura de Doca aqui
                    
                    # --- CRUZAMENTO MESTRE ---
                    registros = []
                    if not df_pedidos.empty:
                        for (loja, forn_macro), df_ped_group in df_pedidos.groupby(['Loja', 'Fornecedor_Macro']):
                            notas_forn = df_notas_agg[(df_notas_agg['Loja'] == loja) & (df_notas_agg['Fornecedor_Macro'] == forn_macro)] if not df_notas_agg.empty else pd.DataFrame()
                            infcpl_nota = textos_infcpl.get((loja, forn_macro), "")

                            if notas_forn.empty:
                                for _, ped in df_ped_group.iterrows():
                                    stat_v, stat_c, dif = classificar(ped['Qtd'], 0, "SEM_FORNECEDOR")
                                    registros.append((loja, ped['Fornecedor_Original'], ped['Produto'], "❌ NÃO ENCONTRADA", ped['Qtd'], 0, "-", dif, stat_v, stat_c, "", "-", "-", "⚪ SEM CONTAGEM", 0.0))
                                continue

                            matched_ped_idx, matched_xml_idx, pairs = set(), set(), []
                            for idx_ped, ped in df_ped_group.iterrows():
                                for idx_xml, nota in notas_forn.iterrows():
                                    pairs.append((fuzz.token_sort_ratio(ped['Produto'], nota['Produto']), idx_ped, idx_xml, nota['Produto'], ped['Qtd'], nota['Qtd'], nota['Origem']))
                            
                            pairs.sort(key=lambda x: x[0], reverse=True) 
                            
                            for score, idx_ped, idx_xml, prod_xml, qtd_ped, qtd_fat, origem_m in pairs:
                                if idx_ped not in matched_ped_idx and idx_xml not in matched_xml_idx:
                                    matched_ped_idx.add(idx_ped); matched_xml_idx.add(idx_xml)
                                    ped = df_ped_group.loc[idx_ped]
                                    stat_v, stat_c, dif = classificar(qtd_ped, qtd_fat, "OK")
                                    
                                    justificativa_ia = ""
                                    if "🔴 NFe FALTA" in stat_v and usar_ia:
                                        justificado, just_texto = analisar_com_ia(ped['Produto'], dif, infcpl_nota)
                                        if justificado: stat_v = f"🤖 JUSTIFICADO (Faltou {abs(dif):.0f})"
                                        justificativa_ia = just_texto
                                    
                                    registros.append((loja, ped['Fornecedor_Original'], ped['Produto'], prod_xml, qtd_ped, qtd_fat, origem_m, dif, stat_v, stat_c, justificativa_ia, "-", "-", "⚪ SEM CONTAGEM", 0.0))

                    if registros:
                        df_final = pd.DataFrame(registros, columns=[
                            'loja','fornecedor','produto_pedido','produto_xml',
                            'qtd_pedido','qtd_nota', 'origem_match', 'diferenca','status_visual','status_codigo',
                            'justificativa_ia', 'qtd_fisico', 'padrao_fisico', 'status_doca', 'diferenca_doca'
                        ])
                        
                        wb_audit = gerar_excel_auditoria(df_final)
                        out_audit = io.BytesIO()
                        wb_audit.save(out_audit)
                        
                        st.balloons()
                        st.success("Auditoria Concluída com Precisão Cirúrgica!")
                        st.download_button(label="📥 Baixar Auditoria Definitiva", data=out_audit.getvalue(), file_name=f"Auditoria_Enterprise_{id_execucao}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
                    else: st.error("❌ Erro. Nenhum dado foi processado.")
                except Exception as e:
                    st.error(f"❌ Erro crítico: {e}")
