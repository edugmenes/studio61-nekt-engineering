# Nekt Data Engineering — ClickUp & Conta Azul → BigQuery → Power BI

End-to-end data pipeline built on the [Nekt](https://nekt.com.br) platform that extracts data from **ClickUp** (project management) and **Conta Azul** (ERP/financial management), transforms it using **PySpark**, and loads it into **Google BigQuery** for consumption in **Power BI**.

---

## Table of Contents

- [Context](#-context)
- [Problem Statement](#️-problem-statement)
- [Objectives](#-objectives)
- [Architecture Overview](#️-architecture-overview)
  - [Bronze — Extraction](#-bronze--extraction)
  - [Silver — Transformation](#-silver--transformation)
  - [Gold — Load](#-gold--load)
- [Data Catalog](#-data-catalog)
  - [ClickUp Bronze Tables](#clickup--bronze)
  - [ClickUp Silver Tables](#clickup--silver)
  - [Conta Azul Bronze Tables](#conta-azul--bronze)
  - [Conta Azul Silver Tables](#conta-azul--silver)
- [Notebook: Transformation Logic](#-notebook-transformation-logic)
  - [Helper Functions](#helper-functions)
  - [ClickUp Transformations](#clickup-transformations)
  - [Conta Azul Transformations](#conta-azul-transformations)
- [Before / After Comparison](#-before--after-comparison)
- [Technical Results](#-technical-results)
- [Technology Stack](#️-technology-stack)
- [Repository Structure](#-repository-structure)
- [Setup & Configuration](#-setup--configuration)

---

## 📖 Context

One of our customers needed to:

- Extract operational data from **ClickUp** (time tracking, workspaces, users)
- Extract financial data from **Conta Azul ERP** (revenues, expenses, installments, DRE, sales, people)
- Consolidate everything in **Google BigQuery**
- Build analytical dashboards in **Power BI**

The original solution was a fully custom Python ETL API executed via a scheduled GCP job with direct API calls and manual BigQuery loads.

---

## ⚠️ Problem Statement

The previous architecture had:

| Issue | Impact |
|---|---|
| 100% hand-written ETL code | High maintenance burden |
| No platform governance | No execution auditability |
| Hard dependency on specific technical knowledge | Bus-factor risk |
| No observability | Silent failures |
| Limited scalability | Difficult to add new endpoints |
| Tight coupling to API contracts | Fragile against API changes |

---

## 🧠 Objectives

Restructure the solution using the **Nekt platform**, aiming to:

- Simplify the solution with less custom code
- Increase governance and auditability
- Improve scalability and maintainability
- Implement a clear **Medallion Architecture** (Bronze → Silver → Gold)

---

## 🏗️ Architecture Overview

```
┌──────────────────────────────────┐
│           DATA SOURCES           │
│   ClickUp API  |  Conta Azul API │
└──────────────┬───────────────────┘
               │
               ▼
┌──────────────────────────────────┐
│    BRONZE  (Raw Extraction)      │
│     Nekt Native REST Source      │
│  Authenticated API ingestion,    │
│  raw data stored as-is           │
└──────────────┬───────────────────┘
               │
               ▼
┌──────────────────────────────────┐
│   SILVER  (Transformation)       │
│   Nekt PySpark Notebook          │
│  Cleaning, typing, deduplication,│
│  flattening, joins               │
└──────────────┬───────────────────┘
               │
               ▼
┌──────────────────────────────────┐
│     GOLD  (Analytical Load)      │
│   Nekt Native Destination        │
│  Automated write to BigQuery     │
└──────────────┬───────────────────┘
               │
               ▼
┌──────────────────────────────────┐
│         Google BigQuery          │
│      Analytical Datasets         │
└──────────────┬───────────────────┘
               │
               ▼
┌──────────────────────────────────┐
│            Power BI              │
│         BI / Reporting           │
└──────────────────────────────────┘
```

<img width="3129" height="1112" alt="Architecture diagram" src="https://github.com/user-attachments/assets/668f0dce-a15f-4c4a-8eb7-0e39963ea1f8" />
<img width="1919" height="907" alt="Nekt pipeline view" src="https://github.com/user-attachments/assets/9d1a0c96-b785-4701-864d-55646671bd5a" />

---

### 🥉 Bronze — Extraction

- Nekt's **native REST source** connector (no custom HTTP code)
- Authenticated connections to the ClickUp and Conta Azul APIs
- Raw data stored without transformations
- Every execution is automatically traceable via the Nekt platform

### 🥈 Silver — Transformation

- PySpark notebook executed inside Nekt ([`src/nekt_notebook.py`](src/nekt_notebook.py))
- Data cleaning: null filtering, deduplication on primary keys
- Type casting to canonical types (string, integer, long, float, boolean)
- Field flattening: nested structs accessed via dot notation
- Array handling: `explode` for multi-value fields, `element_at` / `last_element` for positional access
- Timestamp normalization: Unix milliseconds → ISO timestamp via `ms_to_timestamp`
- Category hierarchy resolution: double-join to resolve parent category names
- Audit column `_loaded_at` (`current_timestamp`) appended to every table

### 🥇 Gold — Load

- Nekt's **native destination** connector writes directly to BigQuery
- No custom write code — fully managed by the platform
- Output tables are BI-ready, structured for direct Power BI consumption

---

## 📦 Data Catalog

### ClickUp — Bronze

| Table Name | Description |
|---|---|
| `studio61_clickup_bronze_users` | Raw user records from the ClickUp workspace |
| `studio61_clickup_bronze_spaces` | Raw space (project area) records |
| `studio61_clickup_bronze_time_entries` | Raw time tracking intervals per task/user |

### ClickUp — Silver

| Table Name | Key Fields | Notes |
|---|---|---|
| `studio61_clickup_silver_users` | `user_id`, `user_name` | Nulls filtered on `username`; deduplicated on `user_id` |
| `studio61_clickup_silver_spaces` | `space_id`, `space_name` | Nulls filtered on `name`; deduplicated on `space_id` |
| `studio61_clickup_silver_time_entries` | `interval_id`, `team_id`, `user_id`, `space_id`, `task_id`, `interval_date_start_ms`, `interval_date_start_iso`, `interval_date_end_ms`, `interval_date_end_iso`, `interval_date_added_ms`, `interval_date_added_iso` | Validated `start < end`; timestamps in both Unix ms and ISO string; deduplicated on `interval_id` |

### Conta Azul — Bronze

| Table Name | Description |
|---|---|
| `studio61_contaazul_bronze_despesas` | Raw accounts payable (expenses) |
| `studio61_contaazul_bronze_receitas` | Raw accounts receivable (revenues) |
| `studio61_contaazul_bronze_categorias` | Raw financial categories |
| `studio61_contaazul_bronze_contratos` | Raw contracts |
| `studio61_contaazul_bronze_categorias_dre` | Raw DRE (income statement) categories |
| `studio61_contaazul_bronze_contas_financeiras` | Raw financial accounts (bank accounts) |
| `studio61_contaazul_bronze_parcelas` | Raw installments and payment events |
| `studio61_contaazul_bronze_pessoas_lista` | Raw people list (customers/suppliers) |
| `studio61_contaazul_bronze_pessoas_detalhes` | Raw people details |
| `studio61_contaazul_bronze_vendas_lista` | Raw sales list |
| `studio61_contaazul_bronze_vendas_detalhes` | Raw sales details |

### Conta Azul — Silver

| Table Name | Key Fields | Notes |
|---|---|---|
| `studio61_contaazul_silver_accounts_payable` | `id`, `descricao`, `data_vencimento`, `status`, `total`, `nao_pago`, `pago`, `data_criacao`, `data_alteracao`, `categoria_principal_id/nome`, `fornecedor_id/nome` | Explodes `categorias` array; deduplicated on `id` |
| `studio61_contaazul_silver_accounts_receivable` | `id`, `descricao`, `data_vencimento`, `status`, `total`, `nao_pago`, `pago`, `data_criacao`, `data_alteracao`, `categoria_principal_id/nome`, `cliente_id/nome` | Explodes `categorias` array; deduplicated on `id` |
| `studio61_contaazul_silver_categories` | `id`, `nome`, `versao`, `categoria_pai`, `tipo`, `entrada_dre`, `considera_custo_dre` | All financial categories; deduplicated on `id` |
| `studio61_contaazul_silver_customers` | `id`, `cliente_id/nome`, `status`, `proximo_vencimento`, `data_inicio`, `numero` | Derived from contracts table; deduplicated on `id` |
| `studio61_contaazul_silver_combined_accounts` | All payable + receivable fields + `tipo` (D/R) + `categoria_pai_id/nome` | Union of payable (tipo=D) and receivable (tipo=R); double-join to resolve parent category hierarchy |
| `studio61_contaazul_silver_dre_items` | `id`, `descricao`, `codigo`, `posicao`, `indica_totalizador`, `representa_soma_custo_medio`, `tem_subitens`, `tem_categorias_financeiras`, `categorias_financeiras`, `tipo=item` | Top-level DRE income statement items |
| `studio61_contaazul_silver_dre_subitems` | `id`, `descricao`, `codigo`, `posicao`, `indica_totalizador`, `representa_soma_custo_medio`, `parent_item_id`, `tem_categorias_financeiras`, `categorias_financeiras`, `tipo=subitem` | Exploded from `subitens` array; references parent item |
| `studio61_contaazul_silver_dre_financial_categories` | `categoria_id`, `codigo`, `nome`, `ativo`, `origem_tipo`, `origem_id`, `origem_item_id`, `record_id` | Financial categories linked to DRE items/subitems; `record_id` = concat of `tipo-id-categoria_id` |
| `studio61_contaazul_silver_financial_accounts` | `id`, `banco`, `codigo_banco`, `nome`, `ativo`, `tipo`, `conta_padrao`, `possui_config_boleto_bancario`, `agencia`, `numero` | Only active accounts (`ativo = true`) |
| `studio61_contaazul_silver_installments` | `parcela_id`, `parcela_status`, `condicao_pagamento`, `referencia`, `agendado`, `tipo_evento`, `rateio`, `conciliado`, `valor_pago`, `nao_pago`, `data_vencimento`, `data_vencimento_previsto`, `descricao`, `id_conta_financeira`, `metodo_pagamento`, `parent_evento_id`, `rateio_id_categoria`, `rateio_nome_categoria`, `rateio_valor`, `rateio_centro_custo_id/nome/valor` | Uses `last_element` helper to safely extract last cost-center from nested rateio array |
| `studio61_contaazul_silver_installments_payments` | `parcela_id`, `baixa_id`, `baixa_versao`, `baixa_data_pagamento`, `baixa_metodo_pagamento`, `baixa_origem`, `baixa_valor_bruto`, `baixa_valor_liquido`, `baixa_desconto`, `baixa_juros`, `baixa_multa`, `baixa_taxa` and other baixa fields | Explodes `baixas` array from installments; one row per payment event |
| `studio61_contaazul_silver_people` | `id`, `id_legado`, `nome`, `documento`, `email`, `telefone`, `ativo`, `data_criacao`, `data_alteracao`, `tipo_pessoa`, `perfis`, `endereco_*` fields | Full address breakdown; first profile via `element_at` |
| `studio61_contaazul_silver_sales` | `id`, `total`, `id_legado`, `data`, `criado_em`, `data_alteracao`, `tipo`, `numero`, `cliente_*` fields, `situacao_nome/descricao`, `status_email_*` | Join between sales list and sales details on `id = sales_id` |

---

## 🔧 Notebook: Transformation Logic

**File:** [`src/nekt_notebook.py`](src/nekt_notebook.py)

The notebook is structured in three sections: **Imports**, **Extracting**, and **Transforming/Loading**.

### Helper Functions

```python
extract_nekt_table(layer_name, table_name) -> DataFrame
```
Thin wrapper over `nekt.load_table()` to reduce verbosity.

```python
save_nekt_table(df, layer_name, table_name, folder_name=None)
```
Thin wrapper over `nekt.save_table()` to reduce verbosity.

```python
ms_to_timestamp(col_name) -> Column
```
Converts a Unix millisecond epoch column to `TimestampType` by casting to long and dividing by 1000.

```python
last_element(array_col, field) -> Column
```
Safely retrieves a named field from the last element of an array column. Returns `null` when the array is empty, avoiding index-out-of-bounds errors.

---

### ClickUp Transformations

**Users** — Filter nulls on `username`, select `id` (integer) and `username` (string), deduplicate on `user_id`.

**Spaces** — Filter nulls on `name`, select `id` (long) and `name` (string), deduplicate on `space_id`.

**Time Entries** — Validates `id`, `user.id`, `start`, `end` are non-null and `end > start`. Extracts nested struct fields (`user.id`, `user.username`, `task_location.space_id`, `task.id`). Stores timestamps in both Unix milliseconds and ISO string format using `ms_to_timestamp`. Deduplicates on `interval_id`.

---

### Conta Azul Transformations

**Accounts Payable / Receivable** — Explodes the `categorias` array (one row per category), extracts category and counterpart (fornecedor/cliente) info, adds `_loaded_at` audit column. Payable uses `fornecedor_*` fields; Receivable uses `cliente_*` fields.

**Categories** — Flat selection with full DRE metadata fields. Used as a lookup table in other joins.

**Customers** — Derived from the contracts table. Extracts customer info and contract lifecycle fields.

**Combined Accounts** — Unions payable (tipo=`D`) and receivable (tipo=`R`) using `unionByName(allowMissingColumns=True)`. Performs two left-joins against the categories table: first to resolve `categoria_pai_id` from the child category, then to resolve `categoria_pai_nome` from the parent category.

**DRE Items / Subitems / Financial Categories** — DRE items and their subitems are processed separately then recombined via `unionByName`. Financial categories are exploded from the combined structure and flattened, with a composite `record_id` key.

**Installments** — Extracts the last element of the nested `evento.rateio` array to get cost-center and category attribution. Uses the `last_element` helper to safely handle variable-length arrays.

**Installment Payments (Baixas)** — Explodes the `baixas` array from the installments source. Each payment event becomes its own row with full financial composition (gross, net, discount, interest, fine, fee).

**Financial Accounts** — Filters to active accounts only (`ativo = true`). Extracts bank, agency, account number, and configuration flags.

**People** — Full contact and address breakdown. Profiles extracted via `element_at(..., 1)` (first profile).

**Sales** — Left-joins the sales list with the sales details on `sl.id = sd.sales_id` to enrich with situation and status information.

---

## 🔥 Before / After Comparison

| Before | After |
|---|---|
| 100% manual Python code | Low-code solution (Nekt + PySpark) |
| No governance | Auditable executions via Nekt |
| High operational complexity | Simplified maintenance |
| Limited scalability | Native platform scalability |
| No traceability | Built-in observability |
| Fragile to API changes | Nekt source abstracts API details |

---

## 📈 Technical Results

- ✅ Reduced operational complexity
- ✅ Reduced operational risk (bus-factor, silent failures)
- ✅ Significant improvement in traceability and auditability
- ✅ Easy to expand to new API endpoints
- ✅ Clear separation of responsibilities (Medallion Architecture)
- ✅ Structured, type-safe Silver tables ready for BI consumption

---

## 🛠️ Technology Stack

| Layer | Technology |
|---|---|
| Source systems | ClickUp REST API, Conta Azul REST API |
| Orchestration & connectors | [Nekt](https://nekt.com.br) Data Engineering Platform |
| Transformation runtime | PySpark (via Nekt Notebook) |
| Infrastructure | Docker, GCP |
| Analytical storage | Google BigQuery |
| BI layer | Power BI |

---

## 📁 Repository Structure

```
nekt-data-engineering/
├── src/
│   ├── nekt_notebook.py        # Main PySpark transformation notebook (Bronze → Silver)
│   └── local_notebook.ipynb    # Local Jupyter notebook for development/testing
├── docs/
│   └── architecture-excalidraw.excalidraw  # Architecture diagram source
├── .env.example                # Environment variable template
└── README.md
```

---

## ⚙️ Setup & Configuration

### 1. Environment Variables

Copy `.env.example` and fill in your Nekt credentials:

```bash
cp .env.example .env
```

```env
# .env
NEKT_DATA_ACCESS_TOKEN=<your Nekt data access token>
```

The token is a Nekt SDK access token created in the Nekt platform for repository/notebook access.

### 2. Running the Notebook Locally

The `src/local_notebook.ipynb` can be used for local development. You will need a Python environment with PySpark and the Nekt SDK installed.

### 3. Running in Nekt

The main transformation step (`src/nekt_notebook.py`) is executed directly inside the Nekt platform as a scheduled notebook. No local execution is needed for production — configure the Nekt pipeline to point to this file.

**Pipeline execution order:**

1. **Nekt Source** → runs REST connectors against ClickUp and Conta Azul APIs → writes Bronze tables
2. **Nekt Notebook** → executes `nekt_notebook.py` → reads Bronze, writes Silver tables
3. **Nekt Destination** → reads Silver tables → writes to Google BigQuery Gold datasets
