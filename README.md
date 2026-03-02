# 🍎 FLV Enterprise - Auditoria & Inteligência

**Painel Central de Processamento e Cruzamento de Dados (Three-Way Match).**

Este é o aplicativo principal utilizado pela coordenação para preparar as operações do dia e auditar os resultados financeiros e físicos do setor de FLV.

## 🛠️ Funcionalidades Principais
### 1. Preparador de Pedidos
* Processa a planilha bruta do comprador.
* Aplica a "Blindagem de Dados" (gera o arquivo que alimenta o Módulo Doca).
* Formatação automática com cores e logos da rede.

### 2. Auditoria 3-Vias (O Coração do Sistema)
* **Cruzamento XML vs Pedido:** Identifica o que o fornecedor faturou a mais ou a menos.
* **Integração com a Doca:** Lê os resultados da conferência física coletados pelo Módulo Doca.
* **Fuzzy Matching:** Algoritmo que reconhece produtos mesmo com nomes diferentes entre a nota e o pedido.

## 📊 Saídas (Relatórios)
* **Auditoria Visual:** Excel detalhado com status coloridos (OK, Falta, Sobra, Sem Nota).
* **Dashboard Operacional:** Resumo executivo para tomada de decisão rápida sobre divergências de estoque.

## 🚀 Tecnologias
* Python 3.11
* Pandas (Data Science)
* RapidFuzz (Inteligência de Texto)
* Openpyxl (Estilização de Relatórios)
