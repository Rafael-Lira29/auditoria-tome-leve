import streamlit as st
import pandas as pd
import re
import unicodedata
from rapidfuzz import fuzz
import xml.etree.ElementTree as ET
import io
from datetime import datetime, timedelta, timezone
from openpyxl import Workbook
from openpyxl.styles import PatternFill, Font, Alignment
import psycopg2

# --- MOTOR COGNITIVO ---
try:
    import google.generativeai as genai
    HAS_IA = True
except ImportError:
    HAS_IA = False

st.set_page_config(page_title="FLV Enterprise - Tome Leve", page_icon="🍎", layout="wide")
st.markdown("""<style>div.stButton > button:first-child { background-color: #002060; color: white; height: 3em; font-weight: bold; width: 100%; border-radius: 8px; } div.stButton > button:first-child:hover { background-color: #00133d; }</style>""", unsafe_allow_html=True)
st.title("🍎 Sistema Integrado FLV Enterprise")

NAMESPACE_NFE = {'nfe': 'http://www.portalfiscal.inf.br/nfe'}

def get_db_connection():
    return psycopg2.connect(st.secrets["DATABASE_URL"])

def normalizar(texto):
    if pd.isna(texto) or texto is None: return ""
    texto = str(texto).upper().strip()
    texto = unicodedata.normalize('NFKD', texto).encode('ASCII', 'ignore').decode('ASCII')
    return re.sub(r'[^\w\s\.\-]', '', texto)

def traduzir_fornecedor(nome_bruto, mapping_forn):
    nome = normalizar(nome_bruto)
    for original, macro in mapping_forn.items():
        if original in nome: return macro
    return nome.replace("FORNECEDOR", "").strip()

def descobrir_loja(cnpj_dest, nome_dest, mapping_nome, mapping_cnpj):
    nome = normalizar(nome_dest)
    cnpj = ''.join(filter(str.isdigit, str(cnpj_dest)))
    for padrao, loja in mapping_cnpj:
        if cnpj.endswith(padrao): return loja
    for padrao, loja in mapping_nome:
        if padrao in nome: return loja
    return 'Loja_Desconhecida'

# ==========================================
# 1. REPOSITORY LAYER
# ==========================================
class DatabaseRepository:
    def carregar_dicionario_depara(self):
        dict_depara = {}
        conn = None
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT cnpj_fornecedor, cod_produto_xml, descricao_interna, fator_conversao FROM depara_flv")
            for row in cursor.fetchall():
                cnpj, cod_xml, desc_int, fator = row
                # BLINDAGEM DAS UVAS: Remove zeros à esquerda e letras do CNPJ
                cnpj_limpo = ''.join(filter(str.isdigit, str(cnpj)))
                cod_xml_limpo = str(cod_xml).strip().lstrip('0')
                dict_depara[(cnpj_limpo, cod_xml_limpo)] = (str(desc_int).strip(), float(fator))
        finally:
            if conn: conn.close()
        return dict_depara

    def carregar_mapeamento_fornecedores(self):
        mapping = {}
        conn = None
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT nome_original, nome_macro FROM fornecedores_mapeamento ORDER BY prioridade DESC")
            for original, macro in cursor.fetchall():
                mapping[original.upper().strip()] = macro
        finally:
            if conn: conn.close()
        return mapping

    def carregar_mapeamento_lojas(self):
        mapping_nome, mapping_cnpj = [], []
        conn = None
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT padrao, tipo, loja FROM lojas_mapeamento ORDER BY prioridade DESC")
            for padrao, tipo, loja in cursor.fetchall():
                if tipo == 'N': mapping_nome.append((padrao.upper(), loja))
                else: mapping_cnpj.append((padrao, loja))
        finally:
            if conn: conn.close()
        return mapping_nome, mapping_cnpj

class NFeRepository:
    def extrair_dados_xml(self, arquivos_xml, dict_depara, mapping_forn, mapping_nome, mapping_cnpj):
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

                forn_macro = traduzir_fornecedor(emit_node.text if emit_node is not None else "", mapping_forn)
                cnpj_forn = cnpj_node.text.strip() if cnpj_node is not None else ""
                loja_xml = descobrir_loja(dest_node.text if dest_node is not None else "0",
                                          dest_nome_node.text if dest_nome_node is not None else "",
                                          mapping_nome, mapping_cnpj)

                infAdic = inf.find('nfe:infAdic/nfe:infCpl', NAMESPACE_NFE)
                if infAdic is not None and infAdic.text:
                    textos_infcpl[(loja_xml, forn_macro)] = infAdic.text

                for det in inf.findall('nfe:det', NAMESPACE_NFE):
                    prod_node = det.find('nfe:prod/nfe:xProd', NAMESPACE_NFE)
                    cod_node = det.find('nfe:prod/nfe:cProd', NAMESPACE_NFE)
                    qtd_node = det.find('nfe:prod/nfe:qCom', NAMESPACE_NFE)
                    if prod_node is None or qtd_node is None: continue

                    cod_xml = cod_node.text.strip() if cod_node is not None else ""
                    qtd_xml = float(qtd_node.text)

                    # BLINDAGEM DAS UVAS (Lado do XML)
                    cnpj_forn_limpo = ''.join(filter(str.isdigit, cnpj_forn))
                    cod_xml_limpo = cod_xml.lstrip('0')

                    # ==========================================
                    # 🕵️‍♂️ INÍCIO DO CÓDIGO ESPIÃO
                    # ==========================================
                    if prod_node.text and "UVA" in prod_node.text.upper():
                        print(f"\n🕵️ ACHOU UVA NO XML!")
                        print(f"   -> Produto: {prod_node.text}")
                        print(f"   -> CNPJ Limpo: '{cnpj_forn_limpo}'")
                        print(f"   -> Cód XML Limpo: '{cod_xml_limpo}'")
                        print(f"   -> Loja Destino: {loja_xml}\n")
                    
                    if loja_xml == "Loja_Desconhecida":
                        print(f"\n⚠️ XML SEM DONO (LOJA NÃO MAPEADA)!")
                        print(f"   -> CNPJ Destino no XML: '{dest_node.text}'")
                        print(f"   -> Nome Destino no XML: '{dest_nome_node.text}'\n")
                    # ==========================================
                    # FIM DO CÓDIGO ESPIÃO
                    # ==========================================

                    origem_match = "XML (Fuzzy)"
                    nome_final = normalizar(prod_node.text)
                    qtd_final = qtd_xml

                    if (cnpj_forn_limpo, cod_xml_limpo) in dict_depara:
                        desc_interna, fator = dict_depara[(cnpj_forn_limpo, cod_xml_limpo)]
                        nome_final = normalizar(desc_interna)
                        qtd_final = qtd_xml * fator
                        origem_match = "De-Para ⚡"

                    notas.append({"Loja": loja_xml, "Fornecedor_Macro": forn_macro, "Produto": nome_final, "Qtd": qtd_final, "Origem": origem_match})
            except Exception as e: 
                print(f"Erro ao ler um XML: {e}")
                pass
        return pd.DataFrame(notas), textos_infcpl

class PedidoRepository:
    def extrair_pedidos_excel(self, arquivo_excel, mapping_forn):
        df_pedidos_raw = pd.read_excel(arquivo_excel, sheet_name=None, header=None)
        pedidos_lista = []
        for aba, df in df_pedidos_raw.items():
            forn_orig, forn_macro = "DESCONHECIDO", "DESCONHECIDO"
            for _, row in df.iterrows():
                col0 = str(row[0]).strip()
                if col0.startswith("Fornecedor:"):
                    forn_orig = col0.replace("Fornecedor:", "").strip()
                    forn_macro = traduzir_fornecedor(forn_orig, mapping_forn)
                else:
                    val = pd.to_numeric(col0, errors='coerce')
                    if pd.notna(val) and val > 0:
                        try:
                            qtd_bruta = float(row[2]) if pd.notna(row[2]) else 0.0
                            padrao_cx = float(row[3]) if pd.notna(row[3]) else 1.0
                            qtd_convertida_kg = qtd_bruta * padrao_cx
                        except: qtd_convertida_kg = 0.0
                        pedidos_lista.append({'Loja': aba, 'Fornecedor_Original': forn_orig, 'Fornecedor_Macro': forn_macro, 'Produto': normalizar(row[1]), 'Qtd': qtd_convertida_kg})
        return pd.DataFrame(pedidos_lista)

# ==========================================
# 2. SERVICE LAYER (As 4 Fases Implacáveis)
# ==========================================
class AuditoriaService:
    def __init__(self, usar_ia, fuzzy_threshold):
        self.usar_ia = usar_ia
        self.TOLERANCIA_DIF = 0.001
        self.FUZZY_THRESHOLD = fuzzy_threshold

    def _classificar(self, qtd_ped, qtd_fat, tipo):
        if tipo == "SEM_FORNECEDOR": return ("⚪ SEM NFe P/ FORN", 98, -qtd_ped)
        if tipo == "SEM_PRODUTO": return ("⚪ PRODUTO NÃO FATURADO", 99, -qtd_ped)
        diferenca = qtd_fat - qtd_ped
        if abs(diferenca) < self.TOLERANCIA_DIF: return ("🟢 OK", 0, 0.0)
        if diferenca < 0: return (f"🔴 NFe FALTA {abs(diferenca):.2f}".replace('.00',''), -1, diferenca)
        return (f"🟡 NFe SOBRA {diferenca:.2f}".replace('.00',''), 1, diferenca)

    def _analisar_com_ia(self, produto, diferenca_negativa, texto_infcpl):
        if not self.usar_ia or not HAS_IA or "GEMINI_API_KEY" not in st.secrets: return False, ""
        if not texto_infcpl or len(texto_infcpl.strip()) < 5: return False, ""
        try:
            genai.configure(api_key=st.secrets["GEMINI_API_KEY"])
            model = genai.GenerativeModel("gemini-1.5-flash")
            prompt = f"Houve FALTA de {abs(diferenca_negativa)} de '{produto}'. O fornecedor escreveu na nota: '{texto_infcpl}'. Isso justifica a falta? Responda ESTRITAMENTE: [SIM] ou [NAO] - Justificativa em 10 palavras."
            resposta = model.generate_content(prompt).text.strip()
            if resposta.startswith("[SIM]"): return True, resposta
            return False, resposta
        except Exception: return False, "Erro IA"

    def processar_cruzamento(self, df_pedidos, df_notas, textos_infcpl):
        if df_pedidos.empty: return pd.DataFrame()
        
        df_pedidos = df_pedidos.groupby(['Loja', 'Fornecedor_Original', 'Fornecedor_Macro', 'Produto'], as_index=False)['Qtd'].sum()
        df_notas_agg = df_notas.groupby(['Loja', 'Fornecedor_Macro', 'Produto', 'Origem'], as_index=False)['Qtd'].sum() if not df_notas.empty else pd.DataFrame()

        registros = []
        for (loja, forn_macro), df_ped_group in df_pedidos.groupby(['Loja', 'Fornecedor_Macro']):
            notas_forn = df_notas_agg[(df_notas_agg['Loja'] == loja) & (df_notas_agg['Fornecedor_Macro'] == forn_macro)] if not df_notas_agg.empty else pd.DataFrame()
            infcpl_nota = textos_infcpl.get((loja, forn_macro), "")

            if notas_forn.empty:
                for _, ped in df_ped_group.iterrows():
                    stat_v, stat_c, dif = self._classificar(ped['Qtd'], 0, "SEM_FORNECEDOR")
                    registros.append((loja, ped['Fornecedor_Original'], ped['Produto'], "❌ NÃO ENCONTRADA", ped['Qtd'], 0, "-", dif, stat_v, stat_c, "", "-", "-", "⚪ SEM CONTAGEM", 0.0))
                continue

            matched_ped_idx, matched_xml_idx = set(), set()

            # FASE 1: Match Perfeito ou De-Para
            for idx_ped, ped in df_ped_group.iterrows():
                for idx_xml, nota in notas_forn.iterrows():
                    if idx_ped in matched_ped_idx or idx_xml in matched_xml_idx: continue
                    if ped['Produto'] == nota['Produto']:
                        matched_ped_idx.add(idx_ped); matched_xml_idx.add(idx_xml)
                        stat_v, stat_c, dif = self._classificar(ped['Qtd'], nota['Qtd'], "OK")
                        just_ia = ""
                        if "🔴" in stat_v:
                            justificado, just_texto = self._analisar_com_ia(ped['Produto'], dif, infcpl_nota)
                            if justificado: stat_v = f"🤖 JUSTIFICADO (Faltou {abs(dif):.0f})"; just_ia = just_texto
                        registros.append((loja, ped['Fornecedor_Original'], ped['Produto'], nota['Produto'], ped['Qtd'], nota['Qtd'], nota['Origem'], dif, stat_v, stat_c, just_ia, "-", "-", "⚪ SEM CONTAGEM", 0.0))

            # FASE 2: Match Fuzzy Controlado (SÓ entra se bater o Threshold)
            pairs = []
            for idx_ped, ped in df_ped_group.iterrows():
                if idx_ped in matched_ped_idx: continue
                for idx_xml, nota in notas_forn.iterrows():
                    if idx_xml in matched_xml_idx: continue
                    score = fuzz.token_sort_ratio(ped['Produto'], nota['Produto'])
                    if score >= self.FUZZY_THRESHOLD:
                        pairs.append((score, idx_ped, idx_xml, nota['Produto'], ped['Qtd'], nota['Qtd'], nota['Origem']))
            
            pairs.sort(key=lambda x: x[0], reverse=True)
            for score, idx_ped, idx_xml, prod_xml, qtd_ped, qtd_fat, origem_m in pairs:
                if idx_ped not in matched_ped_idx and idx_xml not in matched_xml_idx:
                    matched_ped_idx.add(idx_ped); matched_xml_idx.add(idx_xml)
                    stat_v, stat_c, dif = self._classificar(qtd_ped, qtd_fat, "OK")
                    just_ia = ""
                    if "🔴" in stat_v:
                        justificado, just_texto = self._analisar_com_ia(df_ped_group.loc[idx_ped, 'Produto'], dif, infcpl_nota)
                        if justificado: stat_v = f"🤖 JUSTIFICADO (Faltou {abs(dif):.0f})"; just_ia = just_texto
                    registros.append((loja, df_ped_group.loc[idx_ped, 'Fornecedor_Original'], df_ped_group.loc[idx_ped, 'Produto'], prod_xml, qtd_ped, qtd_fat, origem_m, dif, stat_v, stat_c, just_ia, "-", "-", "⚪ SEM CONTAGEM", 0.0))

            # FASE 3: Faltas (Não encontrou na NFe)
            for idx_ped, ped in df_ped_group.iterrows():
                if idx_ped not in matched_ped_idx:
                    stat_v, stat_c, dif = self._classificar(ped['Qtd'], 0, "SEM_PRODUTO")
                    registros.append((loja, ped['Fornecedor_Original'], ped['Produto'], "❌ NÃO ENCONTRADA", ped['Qtd'], 0, "-", dif, stat_v, stat_c, "", "-", "-", "⚪ SEM CONTAGEM", 0.0))

            # FASE 4: Notas Extras (Veio na NFe mas não foi pedido)
            for idx_xml, nota in notas_forn.iterrows():
                if idx_xml not in matched_xml_idx:
                    stat_v = f"🟡 NFe EXTRA {nota['Qtd']:.2f}".replace('.00','')
                    registros.append((loja, "N/A (extra na nota)", "❌ NÃO PEDIDO", nota['Produto'], 0.0, nota['Qtd'], nota['Origem'], nota['Qtd'], stat_v, 2, "", "-", "-", "⚪ SEM CONTAGEM", 0.0))

        if not registros: return pd.DataFrame()
        return pd.DataFrame(registros, columns=['loja','fornecedor','produto_pedido','produto_xml','qtd_pedido','qtd_nota','origem_match','diferenca','status_visual','status_codigo','justificativa_ia','qtd_fisico','padrao_fisico','status_doca','diferenca_doca'])

# ==========================================
# 3. CONTROLLER & GERADOR EXCEL
# ==========================================
class AuditoriaController:
    def executar_auditoria(self, arquivo_excel, arquivos_xml, usar_ia, fuzzy_threshold):
        db_repo = DatabaseRepository()
        pedido_repo = PedidoRepository()
        nfe_repo = NFeRepository()
        
        dict_depara = db_repo.carregar_dicionario_depara()
        mapping_forn = db_repo.carregar_mapeamento_fornecedores()
        mapping_nome, mapping_cnpj = db_repo.carregar_mapeamento_lojas()

        df_pedidos = pedido_repo.extrair_pedidos_excel(arquivo_excel, mapping_forn)
        df_notas, textos_infcpl = nfe_repo.extrair_dados_xml(arquivos_xml, dict_depara, mapping_forn, mapping_nome, mapping_cnpj)

        service = AuditoriaService(usar_ia, fuzzy_threshold)
        return service.processar_cruzamento(df_pedidos, df_notas, textos_infcpl)

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

            ws.append([row['produto_pedido'], row['qtd_pedido'], row['produto_xml'], row['qtd_nota'], row['origem_match'], row['status_visual'], row.get('justificativa_ia', ''), row['qtd_fisico'], row['padrao_fisico'], row['status_doca']])
            
            status_nfe_cell = ws.cell(row=ws.max_row, column=6)
            val_nfe = status_nfe_cell.value
            if val_nfe:
                if "🟢" in val_nfe: status_nfe_cell.fill = PatternFill(start_color="C6EFCE", end_color="C6EFCE", fill_type="solid")
                elif "🔴" in val_nfe: status_nfe_cell.fill = PatternFill(start_color="FFC7CE", end_color="FFC7CE", fill_type="solid")
                elif "🟡 NFe SOBRA" in val_nfe or "🟡 NFe EXTRA" in val_nfe: status_nfe_cell.fill = PatternFill(start_color="FFEB9C", end_color="FFEB9C", fill_type="solid")
                elif "🤖" in val_nfe: status_nfe_cell.fill = PatternFill(start_color="E4DFEC", end_color="E4DFEC", fill_type="solid")
                elif "⚪" in val_nfe: status_nfe_cell.fill = PatternFill(start_color="F2F2F2", end_color="F2F2F2", fill_type="solid")

        ws.column_dimensions['A'].width = 30; ws.column_dimensions['C'].width = 30; ws.column_dimensions['E'].width = 15; ws.column_dimensions['F'].width = 25; ws.column_dimensions['G'].width = 35; ws.column_dimensions['J'].width = 25
    return wb

# ==========================================
# INTERFACE PRINCIPAL
# ==========================================
aba_preparador, aba_auditoria, aba_gestao = st.tabs(["🧹 1. Preparador", "🍎 2. Auditoria DB", "⚙️ 3. Gestão De-Para"])

with aba_preparador:
    st.header("🧹 Preparador de Planilha do Comprador (FLV)")
    st.markdown("Filtra os pedidos não solicitados, separa por loja e fornecedor e gera o formato Padrão Sauron.")

    arquivo_flv_bruto = st.file_uploader("📂 Arraste a planilha de Pedidos FLV (Matriz Comercial)", type=["xlsx", "xls", "csv"], key="up_preparador")

    if arquivo_flv_bruto:
        if st.button("⚙️ Processar, Limpar e Roteirizar Pedidos"):
            with st.spinner("Decodificando matriz complexa de pedidos da área comercial..."):
                try:
                    fuso_br = timezone(timedelta(hours=-3))

                    if arquivo_flv_bruto.name.endswith('.csv'):
                        df_raw = pd.read_csv(arquivo_flv_bruto, header=None, low_memory=False)
                    else:
                        df_raw = pd.read_excel(arquivo_flv_bruto, header=None)

                    records = []
                    current_forn = "FORNECEDOR INDEFINIDO"
                    stores_cols = {}
                    padrao_col = None

                    # Motor de Varredura Furtiva (Sensível a Padrões, não a posições fixas)
                    for idx, row in df_raw.iterrows():
                        col0 = str(row[0]).strip().upper()
                        
                        # 1. Captura o título do Fornecedor
                        if col0.startswith("PEDIDO FLV"):
                            current_forn = str(row[0]).strip().replace("PEDIDO FLV", "").strip()
                            continue
                        
                        # 2. Varredura Inteligente: Detecta a linha que contém as Lojas (L1, L2...)
                        is_mapping_row = False
                        for val in row.values:
                            val_str = str(val).strip().upper()
                            if val_str.startswith("L") and val_str[1:].isdigit():
                                is_mapping_row = True
                                break
                        
                        if is_mapping_row:
                            stores_cols = {}
                            padrao_col = None
                            for c_idx, val in enumerate(row.values):
                                val_str = str(val).strip().upper()
                                if val_str.startswith("L") and val_str[1:].isdigit():
                                    stores_cols[c_idx] = val_str
                                elif "PADRÃO" in val_str or "PADRAO" in val_str:
                                    padrao_col = c_idx
                            continue
                        
                        # 3. Pula linhas secundárias de cabeçalho do ERP ("CODIGO", "DESCRIÇÃO", etc)
                        if col0 in ["CÓDIGO", "CODIGO", "CÓD. FORN.", "CÓD. FORN"]:
                            continue
                        
                        # 4. Processamento de Produtos (Apenas se a coluna 0 for numérica)
                        if str(row[0]).replace('.', '').isdigit():
                            produto = str(row[1]).strip()
                            if not produto or produto.upper() == "NAN": continue
                            
                            # Extração Segura do Peso Padrão da Caixa (Assume 1kg se falhar)
                            padrao = 1.0
                            if padrao_col is not None and pd.notna(row.iloc[padrao_col]):
                                val_padrao = str(row.iloc[padrao_col]).replace(',', '.').strip()
                                match_padrao = re.search(r'[\d\.]+', val_padrao)
                                if match_padrao:
                                    try: padrao = float(match_padrao.group())
                                    except: pass
                            
                            # Roteamento Apenas do Solicitado (> 0)
                            for c_idx, loja in stores_cols.items():
                                val = row.iloc[c_idx]
                                if pd.notna(val) and str(val).strip() != "":
                                    try:
                                        qtd_cx = float(str(val).replace(',', '.'))
                                        if qtd_cx > 0:
                                            records.append({
                                                "Loja": loja.strip(),
                                                "Fornecedor": current_forn,
                                                "Produto": produto,
                                                "Qtd_Cx": qtd_cx,
                                                "Padrao": padrao,
                                                "Qtd_KG": round(qtd_cx * padrao, 2)
                                            })
                                    except: pass

                    if not records:
                        st.error("❌ O robô não encontrou pedidos. Verifique se o formato bate com a leitura matricial.")
                        st.stop()

                    df_pedidos = pd.DataFrame(records)

                    # Construção Visual do Excel (Padrão Sauron)
                    wb = Workbook()
                    wb.remove(wb.active)
                    data_hoje = datetime.now(fuso_br).strftime("%d/%m/%Y %H:%M")

                    # Estilização
                    header_fill = PatternFill(start_color="002060", end_color="002060", fill_type="solid")
                    forn_fill = PatternFill(start_color="D9E1F2", end_color="D9E1F2", fill_type="solid")
                    font_branca = Font(color="FFFFFF", bold=True)
                    font_forn = Font(color="002060", bold=True)
                    align_center = Alignment(horizontal="center", vertical="center")

                    lojas_encontradas = sorted(df_pedidos['Loja'].unique())

                    for loja in lojas_encontradas:
                        ws = wb.create_sheet(title=loja)
                        
                        # Cabeçalho Principal (Com Fuso Horário)
                        ws.append([f"PEDIDOS - LOJA {loja} - GERADO EM: {data_hoje}"])
                        ws.merge_cells('A1:E1')
                        ws['A1'].fill = header_fill
                        ws['A1'].font = font_branca
                        ws['A1'].alignment = align_center

                        ws.append(["CÓD", "PRODUTO", "QTD (CAIXAS)", "PADRÃO (KG)", "TOTAL (KG)"])
                        for cell in ws[2]: 
                            cell.font = Font(bold=True)
                            cell.alignment = align_center

                        df_loja = df_pedidos[df_pedidos['Loja'] == loja]
                        
                        for fornecedor in sorted(df_loja['Fornecedor'].unique()):
                            ws.append([f"Fornecedor: {fornecedor}", "", "", "", ""])
                            ws.merge_cells(start_row=ws.max_row, start_column=1, end_row=ws.max_row, end_column=5)
                            for cell in ws[ws.max_row]:
                                cell.fill = forn_fill
                                cell.font = font_forn
                            
                            df_forn = df_loja[df_loja['Fornecedor'] == fornecedor]
                            for _, r in df_forn.iterrows():
                                ws.append(["", r['Produto'], r['Qtd_Cx'], r['Padrao'], r['Qtd_KG']])
                        
                        ws.column_dimensions['A'].width = 15
                        ws.column_dimensions['B'].width = 45
                        ws.column_dimensions['C'].width = 15
                        ws.column_dimensions['D'].width = 15
                        ws.column_dimensions['E'].width = 15

                    out_io = io.BytesIO()
                    wb.save(out_io)
                    
                    st.success(f"✅ Matriz Furtiva Concluída! {len(df_pedidos)} itens de múltiplos fornecedores roteirizados para {len(lojas_encontradas)} lojas.")
                    st.download_button(
                        label="📥 Baixar Pedidos Tratados (Pronto p/ Auditoria)",
                        data=out_io.getvalue(),
                        file_name=f"Pedidos_FLV_Formatado_{datetime.now(fuso_br).strftime('%d_%m')}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )

                except Exception as e:
                    st.error(f"Erro na extração dos dados comerciais: {e}")

with aba_auditoria:
    st.header("🍎 Cruzamento Triplo via Neon DB")
    col1, col2, col3 = st.columns(3)
    with col1: arquivo_excel = st.file_uploader("1. Pedidos", type=['xlsx'], key="up_ped")
    with col2: arquivos_xml = st.file_uploader("2. Notas (XMLs)", type=['xml'], accept_multiple_files=True, key="up_xml")
    with col3: arquivos_contagem = st.file_uploader("3. Doca", type=['xlsx', 'csv'], accept_multiple_files=True, key="up_doca")
    
    col_ia, col_slider = st.columns([1, 2])
    with col_ia: usar_ia = st.checkbox("🧠 Ativar Auditor IA", value=HAS_IA)
    with col_slider: fuzzy_threshold = st.slider("🎯 Limiar de Similaridade (Abaixo disso = Não Encontrada)", min_value=50, max_value=100, value=85, step=1)

    if st.button("Executar Auditoria Implacável"):
        if not arquivo_excel or not arquivos_xml: st.warning("⚠️ Precisa de Pedido e XMLs.")
        else:
            with st.spinner("Conectando ao PostgreSQL e processando..."):
                try:
                    controller = AuditoriaController()
                    df_final = controller.executar_auditoria(arquivo_excel, arquivos_xml, usar_ia, fuzzy_threshold)
                    
                    if not df_final.empty:
                        wb_audit = gerar_excel_auditoria(df_final)
                        out_audit = io.BytesIO()
                        wb_audit.save(out_audit)
                        st.success("✅ Auditoria Concluída com Sucesso!")
                        st.download_button(label="📥 Baixar Auditoria", data=out_audit.getvalue(), file_name=f"Auditoria_{datetime.now().strftime('%Y%m%d%H%M')}.xlsx")
                    else: st.error("❌ Nenhum dado cruzado.")
                except Exception as e: st.error(f"❌ Erro crítico: {e}")

with aba_gestao:
    st.header("⚙️ Painel de Gestão (De-Para)")
    st.info("A gestão do dicionário De-Para continua a operar no banco de forma segura. O código desta aba permanece igual ao original.")
