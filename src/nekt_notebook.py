## IMPORTS
import nekt 
from typing         import Optional 
from pyspark.sql    import Window
from pyspark.sql    import DataFrame, Column 
from pyspark.sql    import functions as F

## HELPER FUNCTIONS
def extract_nekt_table(layer_name: str, table_name: str) -> DataFrame:
    """Simplify nekt table extraction syntax."""
    return nekt.load_table(layer_name=layer_name, table_name=table_name)

def save_nekt_table(
    df: DataFrame, 
    layer_name: str, 
    table_name: str,
    folder_name: Optional[str] = None 
):
    """Simplify nekt table saving syntax."""
    nekt.save_table(
        df=df,
        layer_name=layer_name,
        table_name=table_name,
        folder_name=folder_name
    )

def ms_to_timestamp(col_name: str) -> Column:
    """Converts a Unix millisecond epoch column to a TimestampType."""
    return F.to_timestamp(F.col(col_name).cast("long") / 1000)

def last_element(array_col: str, field: str) -> F.Column:
    """Safely retrieves a field from the last element of an array column."""
    return (
        F.when(
            F.size(F.col(array_col)) > 0,
            F.element_at(F.col(array_col), F.size(F.col(array_col)))[field]
        ).otherwise(F.lit(None))
    )

## EXTRACTING TABLES
# clickup - bronze tables
df_bronze_clickup_users                 = extract_nekt_table("Bronze", "studio61_clickup_bronze_users")
df_bronze_clickup_spaces                = extract_nekt_table("Bronze", "studio61_clickup_bronze_spaces" )
df_bronze_clickup_time_entries          = extract_nekt_table("Bronze", "studio61_clickup_bronze_time_entries")

# conta azul - bronze tables
df_bronze_contaazul_accounts_payable    = extract_nekt_table("Bronze", "studio61_contaazul_bronze_despesas")
df_bronze_contaazul_accounts_receivable = extract_nekt_table("Bronze", "studio61_contaazul_bronze_receitas")
df_bronze_contaazul_categories          = extract_nekt_table("Bronze", "studio61_contaazul_bronze_categorias")
df_bronze_contaazul_contracts           = extract_nekt_table("Bronze", "studio61_contaazul_bronze_contratos")
df_bronze_contaazul_dre_categories      = extract_nekt_table("Bronze", "studio61_contaazul_bronze_categorias_dre")
df_bronze_contaazul_financial_accounts  = extract_nekt_table("Bronze", "studio61_contaazul_bronze_contas_financeiras")
df_bronze_contaazul_installments        = extract_nekt_table("Bronze", "studio61_contaazul_bronze_parcelas")
df_bronze_contaazul_people_list         = extract_nekt_table("Bronze", "studio61_contaazul_bronze_pessoas_lista")
df_bronze_contaazul_people_details      = extract_nekt_table("Bronze", "studio61_contaazul_bronze_pessoas_detalhes")   
df_bronze_contaazul_sales_list          = extract_nekt_table("Bronze", "studio61_contaazul_bronze_vendas_lista")
df_bronze_contaazul_sales_details       = extract_nekt_table("Bronze", "studio61_contaazul_bronze_vendas_detalhes")

## TRANSFORMING TABLES
# clickup - users
df_silver_clickup_users = (
    df_bronze_clickup_users
    .filter(
        F.col("username").isNotNull()
    )
    .select(
        F.col("id")         .cast("integer").alias("user_id"),
        F.col("username")   .cast("string") .alias("user_name")
    )
    
    .dropDuplicates(
        ["user_id"]
    )
)

# clickup - spaces
df_silver_clickup_spaces = (
    df_bronze_clickup_spaces
    .filter(
        F.col("name").isNotNull()
    )
    .select(
        F.col("id")     .cast("long")   .alias("space_id"),
        F.col("name")   .cast("string") .alias("space_name")
    )
    .dropDuplicates(
        ["space_id"]
    )
)

# clickup - time entries
df_silver_clickup_time_entries = (
    df_bronze_clickup_time_entries
    .filter(
        F.col("id").isNotNull() &
        F.col("user.id").isNotNull() &
        F.col("start").isNotNull() &
        F.col("end").isNotNull() &
        (F.col("end") > F.col("start"))
    )
    .select(
        F.col("id")                     .cast("long")   .alias("interval_id"),
        F.col("wid")                    .cast("integer").alias("team_id"),
        F.col("user.id")                .cast("integer").alias("user_id"),
        F.col("user.username")          .cast("string") .alias("user_name"),
        F.col("task_location.space_id") .cast("long")   .alias("space_id"),
        F.col("task.id")                .cast("string") .alias("task_id"),
        F.col("start")                  .cast("long")   .alias("interval_date_start_ms"),
        ms_to_timestamp("start")        .cast("string") .alias("interval_date_start_iso"),
        F.col("end")                    .cast("long")   .alias("interval_date_end_ms"),
        ms_to_timestamp("end")          .cast("string") .alias("interval_date_end_iso"),
        F.col("at")                     .cast("long")   .alias("interval_date_added_ms"),
        ms_to_timestamp("at")           .cast("string") .alias("interval_date_added_iso"),
    )
    .dropDuplicates(
        ["interval_id"]
    )
)

# conta azul - accounts payable (expenses)
df_silver_contaazul_accounts_payable = (
    df_bronze_contaazul_accounts_payable
    .filter(
        F.col("id").isNotNull() &
        F.col("data_criacao").isNotNull()
    )
    .withColumn(
        "categoria", F.explode("categorias")
    )
    .select(
        F.col("id")             .cast("string") .alias("id"),
        F.col("descricao")      .cast("string") .alias("descricao"),
        F.col("data_vencimento").cast("string") .alias("data_vencimento"),
        F.col("status")         .cast("string") .alias("status"),
        F.col("total")          .cast("float")  .alias("total"),
        F.col("nao_pago")       .cast("float")  .alias("nao_pago"),
        F.col("pago")           .cast("float")  .alias("pago"),
        F.col("data_criacao")   .cast("string") .alias("data_criacao"),
        F.col("data_alteracao") .cast("string") .alias("data_alteracao"),
        F.col("categoria.id")   .cast("string") .alias("categoria_principal_id"),
        F.col("categoria.nome") .cast("string") .alias("categoria_principal_nome"),
        F.col("fornecedor.id")  .cast("string") .alias("fornecedor_id"),
        F.col("fornecedor.nome").cast("string") .alias("fornecedor_nome"),
        F.current_timestamp()   .cast("string") .alias("_loaded_at"),
    )
    .dropDuplicates(
        ["id"]
    )
)

# conta azul - accounts receivable (revenues)
df_silver_contaazul_accounts_receivable = (
    df_bronze_contaazul_accounts_receivable
    .filter(
        F.col("id").isNotNull() &
        F.col("data_criacao").isNotNull()
    )
    .withColumn(
        "categoria", F.explode("categorias")
    )
    .select(
        F.col("id")             .cast("string") .alias("id"),
        F.col("descricao")      .cast("string") .alias("descricao"),
        F.col("data_vencimento").cast("string") .alias("data_vencimento"),
        F.col("status")         .cast("string") .alias("status"),
        F.col("total")          .cast("float")  .alias("total"),
        F.col("nao_pago")       .cast("float")  .alias("nao_pago"),
        F.col("pago")           .cast("float")  .alias("pago"),
        F.col("data_criacao")   .cast("string") .alias("data_criacao"),
        F.col("data_alteracao") .cast("string") .alias("data_alteracao"),
        F.col("categoria.id")   .cast("string") .alias("categoria_principal_id"),
        F.col("categoria.nome") .cast("string") .alias("categoria_principal_nome"),
        F.col("cliente.id")     .cast("string") .alias("cliente_id"),
        F.col("cliente.nome")   .cast("string") .alias("cliente_nome"),
        F.current_timestamp()   .cast("string") .alias("_loaded_at"),
    )
    .dropDuplicates(
        ["id"]
    )
)

# conta azul - categories   
df_silver_contaazul_categories = (
    df_bronze_contaazul_categories
    .select(
        F.col("id")                 .cast("string") .alias("id"),
        F.col("nome")               .cast("string") .alias("nome"),
        F.col("versao")             .cast("integer").alias("versao"),
        F.col("categoria_pai")      .cast("string") .alias("categoria_pai"),
        F.col("tipo")               .cast("string") .alias("tipo"),
        F.col("entrada_dre")        .cast("string") .alias("entrada_dre"),
        F.col("considera_custo_dre").cast("boolean").alias("considera_custo_dre"),
        F.current_timestamp()       .cast("string") .alias("_loaded_at"),
    )
    .dropDuplicates(
        ["id"]
    )
)

# conta azul - customers
df_silver_contaazul_customers = (
    df_bronze_contaazul_contracts
    .filter(
        F.col("id").isNotNull()
    )
    .select(
        F.col("id")                  .cast("string") .alias("id"),
        F.col("cliente.id")          .cast("string") .alias("cliente_id"),
        F.col("cliente.nome")        .cast("string") .alias("cliente_nome"),
        F.col("status")              .cast("string") .alias("status"),
        F.col("proximo_vencimento")  .cast("string") .alias("proximo_vencimento"),
        F.col("data_inicio")         .cast("string") .alias("data_inicio"),
        F.col("numero")              .cast("integer").alias("numero"),
        F.current_timestamp()        .cast("string") .alias("_loaded_at"),
    )
    .dropDuplicates(
        ["id"]
    )
)

# conta azul - parent categories
df_parent_categories = (
    df_bronze_contaazul_categories
    .join(
        df_bronze_contaazul_categories
        .filter(
            F.col("categoria_pai").isNotNull()
        )
        .select(
            F.col("categoria_pai").alias("id")
        )
        .distinct(),
        on="id",
        how="inner",
    )
)

# conta azul - combined accounts
df_silver_contaazul_combined_accounts = (
    df_silver_contaazul_accounts_payable
    .withColumn(
        "tipo", F.lit("D").cast("string")
    )
    .unionByName(
        df_silver_contaazul_accounts_receivable
        .withColumn(
            "tipo", F.lit("R").cast("string")
        ),
        allowMissingColumns=True,
    )
    .join(
        # first join — resolve categoria_pai_id from child category
        df_silver_contaazul_categories
        .select(
            F.col("id")             .cast("string").alias("categoria_principal_id"),
            F.col("categoria_pai")  .cast("string").alias("categoria_pai_id"),
        ),
        on="categoria_principal_id",
        how="left",
    )
    .join(
        # second join — resolve categoria_pai_nome from parent category
        df_parent_categories
        .select(
            F.col("id")             .cast("string").alias("categoria_pai_id"),
            F.col("nome")           .cast("string").alias("categoria_pai_nome"),
        ),
        on="categoria_pai_id",
        how="left",
    )
)

# conta azul - dre_items
df_silver_contaazul_dre_items = (
    df_bronze_contaazul_dre_categories
    .filter(
        F.col("id").isNotNull()
    )
    .select(                      
        F.col("id")                                                         .cast("string") .alias("id"),
        F.col("descricao")                                                  .cast("string") .alias("descricao"),
        F.coalesce(F.col("codigo"), F.lit("0"))                             .cast("string") .alias("codigo"),
        F.col("posicao")                                                    .cast("integer").alias("posicao"),
        F.col("indica_totalizador")                                         .cast("boolean").alias("indica_totalizador"),
        F.col("representa_soma_custo_medio")                                .cast("boolean").alias("representa_soma_custo_medio"), 
        F.when(F.size("subitens")               > 0, True).otherwise(False) .cast("boolean").alias("tem_subitens"),
        F.when(F.size("categorias_financeiras") > 0, True).otherwise(False) .cast("boolean").alias("tem_categorias_financeiras"),
        F.col("categorias_financeiras")                                                     .alias("categorias_financeiras"),
        F.lit("item")                                                       .cast("string") .alias("tipo"),
        F.current_timestamp()                                               .cast("string") .alias("_loaded_at"),
    )
    .dropDuplicates(
        ["id"]
    )
)

# conta azul - dre_subitems
df_silver_contaazul_dre_subitems = (
    df_bronze_contaazul_dre_categories
    .filter(
        F.col("subitens").isNotNull() &
        (F.size("subitens") > 0)
    )
    .select(
        F.col("id"),                                 
        F.explode("subitens").alias("subitem")
    )
    .select(                      
        F.col("subitem.id")                                                         .cast("string") .alias("id"),
        F.col("subitem.descricao")                                                  .cast("string") .alias("descricao"),
        F.col("subitem.codigo")                                                     .cast("string") .alias("codigo"),
        F.col("subitem.posicao")                                                    .cast("integer").alias("posicao"),
        F.col("subitem.indica_totalizador")                                         .cast("boolean").alias("indica_totalizador"),
        F.col("subitem.representa_soma_custo_medio")                                .cast("boolean").alias("representa_soma_custo_medio"),
        F.col("id")                                                                 .cast("string") .alias("parent_item_id"),   
        F.when(F.size("subitem.categorias_financeiras") > 0, True).otherwise(False) .cast("boolean").alias("tem_categorias_financeiras"),
        F.col("subitem.categorias_financeiras")                                                     .alias("categorias_financeiras"),
        F.lit("subitem")                                                            .cast("string") .alias("tipo"),
        F.current_timestamp()                                                       .cast("string") .alias("_loaded_at"),
    )
    .dropDuplicates(
        ["id"]
    )
)

# conta azul - combined dre items/subitems 
df_dre_combined_items = (
    df_silver_contaazul_dre_items
    .select(
        F.col("id"),
        F.col("id").alias("parent_item_id"),
        F.col("descricao"),
        F.col("codigo"),
        F.col("posicao"),
        F.col("indica_totalizador"),
        F.col("representa_soma_custo_medio"),
        F.col("categorias_financeiras"),
        F.col("tipo" ),
    )
    .unionByName(
        df_silver_contaazul_dre_subitems
        .select(
            F.col("id"),
            F.col("parent_item_id"),
            F.col("descricao"),     
            F.col("codigo"),
            F.col("posicao"),
            F.col("indica_totalizador"),
            F.col("representa_soma_custo_medio"),
            F.col("categorias_financeiras"),
            F.col("tipo" ),
        ),
        allowMissingColumns=False
    )
)

# conta azul - dre_financial_categories
df_silver_contaazul_dre_financial_categories = (
    df_dre_combined_items
    .select(
        F.col("id"),   
        F.col("parent_item_id"),
        F.col("tipo"),                           
        F.explode("categorias_financeiras").alias("categoria_financeria")
    )
    .select(
        F.col("categoria_financeria.id")    .cast("string") .alias("categoria_id"),
        F.col("categoria_financeria.codigo").cast("string") .alias("codigo"),
        F.col("categoria_financeria.nome")  .cast("string") .alias("nome"),
        F.col("categoria_financeria.ativo") .cast("boolean").alias("ativo"),
        F.col("tipo")                       .cast("string") .alias("origem_tipo"),
        F.col("id")                         .cast("string") .alias("origem_id"), 
        F.col("parent_item_id")             .cast("string") .alias("origem_item_id"),
        F.current_timestamp()               .cast("string") .alias("_loaded_at"),
    )
    .withColumn(
        "record_id", F.concat_ws("-", F.col("origem_tipo"), F.col("origem_id"), F.col("categoria_id"))
    )
)

# conta azul - installments
df_silver_contaazul_installments = (
    df_bronze_contaazul_installments
    .withColumn(
        "last_rateio",
        F.when(
            F.size(F.col("evento.rateio")) > 0,
            F.element_at(
                F.col("evento.rateio"), 
                F.size(F.col("evento.rateio"))
            )
        ).otherwise(F.lit(None))
    )
    .select(
        F.col("id")                                                         .cast("string") .alias("parcela_id"),
        F.col("status")                                                     .cast("string") .alias("parcela_status"),
        F.col("evento.condicao_pagamento")                                  .cast("string") .alias("condicao_pagamento"),
        F.col("referencia")                                                 .cast("string") .alias("referencia"),
        F.col("evento.agendado")                                            .cast("boolean").alias("agendado"),
        F.col("evento.tipo")                                                .cast("string") .alias("tipo_evento"),
        F.col("evento.rateio")                                              .cast("string") .alias("rateio"),
        F.col("conciliado")                                                 .cast("boolean").alias("conciliado"),
        F.col("valor_pago")                                                 .cast("float")  .alias("valor_pago"),
        F.col("perda")                                                      .cast("string") .alias("perda"),
        F.col("nao_pago")                                                   .cast("float")  .alias("nao_pago"),
        F.col("data_vencimento")                                            .cast("string") .alias("data_vencimento"),
        F.col("data_pagamento_previsto")                                    .cast("string") .alias("data_vencimento_previsto"),
        F.col("descricao")                                                  .cast("string") .alias("descricao"),
        F.col("conta_financeira.id")                                        .cast("string") .alias("id_conta_financeira"),
        F.col("metodo_pagamento")                                           .cast("string") .alias("metodo_pagamento"),
        F.col("evento.id")                                                  .cast("string") .alias("parent_evento_id"),
        F.col("last_rateio.id_categoria")                                   .cast("string") .alias("rateio_id_categoria"),
        F.col("last_rateio.nome_categoria")                                 .cast("string") .alias("rateio_nome_categoria"),
        F.col("last_rateio.valor")                                          .cast("float")  .alias("rateio_valor"),
        last_element("last_rateio.rateio_centro_custo", "id_centro_custo")  .cast("string") .alias("rateio_centro_custo_id"),
        last_element("last_rateio.rateio_centro_custo", "nome_centro_custo").cast("string") .alias("rateio_centro_custo_nome"),
        last_element("last_rateio.rateio_centro_custo", "valor")            .cast("float")  .alias("rateio_centro_custo_valor"),
        F.current_timestamp()                                               .cast("string") .alias("_loaded_at"),
        F.col("evento.data_competencia")                                    .cast("string") .alias("parcela_loaded_at"),
    )
    .dropDuplicates(
        ["parcela_id"]
    )
)

# conta azul - installments payments
df_silver_contaazul_installments_payments = (
    df_bronze_contaazul_installments
    .filter(
        F.col("baixas").isNotNull() &
        (F.size("baixas") > 0)
    )
    .select(
        F.col("id"),
        F.col("evento.data_competencia").alias("parcela_loaded_at"),
        F.explode("baixas").alias("baixa"),
    )
    .select(
        F.col("id")                                     .cast("string") .alias("parcela_id"),
        F.col("baixa.id")                               .cast("string") .alias("baixa_id"),
        F.col("baixa.versao")                           .cast("integer").alias("baixa_versao"),
        F.col("baixa.data_pagamento")                   .cast("string") .alias("baixa_data_pagamento"),
        F.col("baixa.id_reconciliacao")                 .cast("string") .alias("baixa_id_reconciliacao"),
        F.col("baixa.id_parcela")                       .cast("string") .alias("baixa_id_parcela"),
        F.col("baixa.id_solicitacao_cobranca")          .cast("string") .alias("baixa_id_solicitacao_cobranca"),
        F.col("baixa.observacao")                       .cast("string") .alias("baixa_observacao"),
        F.col("baixa.metodo_pagamento")                 .cast("string") .alias("baixa_metodo_pagamento"),
        F.col("baixa.origem")                           .cast("string") .alias("baixa_origem"),
        F.col("baixa.id_recibo_digital")                .cast("string") .alias("baixa_id_recibo_digital"),
        F.col("baixa.tipo_evento_financeiro")           .cast("string") .alias("baixa_tipo_evento_financeiro"),
        F.col("baixa.nsu")                              .cast("string") .alias("baixa_nsu"),
        F.col("baixa.id_referencia")                    .cast("string") .alias("baixa_id_referencia"),
        F.col("baixa.atualizado_em")                    .cast("string") .alias("baixa_atualizado_em"),
        F.col("baixa.valor_composicao.desconto")        .cast("float")  .alias("baixa_desconto"),
        F.col("baixa.valor_composicao.juros")           .cast("float")  .alias("baixa_juros"),
        F.col("baixa.valor_composicao.multa")           .cast("float")  .alias("baixa_multa"),
        F.col("baixa.valor_composicao.taxa")            .cast("float")  .alias("baixa_taxa"),
        F.col("baixa.valor_composicao.valor_bruto")     .cast("float")  .alias("baixa_valor_bruto"),
        F.col("baixa.valor_composicao.valor_liquido")   .cast("float")  .alias("baixa_valor_liquido"),
        F.current_timestamp()                           .cast("string") .alias("_loaded_at"),
        F.current_timestamp()                           .cast("string") .alias("baixa_loaded_at"),
        F.col("parcela_loaded_at")                      .cast("string"),
    )
    .dropDuplicates(
        ["baixa_id"]
    )
)

# conta azul - financial accounts
df_silver_contaazul_financial_accounts = (
    df_bronze_contaazul_financial_accounts
    .filter(
        F.col("id").isNotNull()
        & (F.col("ativo") == True)
    )
    .select(
        F.col("id")                            .cast("string") .alias("id"),
        F.col("banco")                         .cast("string") .alias("banco"),
        F.col("codigo_banco")                  .cast("integer").alias("codigo_banco"),
        F.col("nome")                          .cast("string") .alias("nome"),
        F.col("ativo")                         .cast("boolean").alias("ativo"),
        F.col("tipo")                          .cast("string") .alias("tipo"),
        F.col("conta_padrao")                  .cast("boolean").alias("conta_padrao"),
        F.col("possui_config_boleto_bancario") .cast("boolean").alias("possui_config_boleto_bancario"),
        F.col("agencia")                       .cast("string") .alias("agencia"),
        F.col("numero")                        .cast("string") .alias("numero"),
        F.current_timestamp()                  .cast("string") .alias("_loaded_at"),
    )
    .dropDuplicates(
        ["id"]
    )
)

# conta azul - people
df_silver_contaazul_people = (
    df_bronze_contaazul_people_list
    .filter(
        F.col("id").isNotNull()
    )
    .select(
        F.col("id")                     .cast("string") .alias("id"),
        F.col("id_legado")              .cast("integer").alias("id_legado"),
        F.col("uuid_legado")            .cast("string") .alias("uuid_legado"),
        F.col("nome")                   .cast("string") .alias("nome"),
        F.col("documento")              .cast("string") .alias("documento"),
        F.col("email")                  .cast("string") .alias("email"),
        F.col("telefone")               .cast("string") .alias("telefone"),
        F.col("ativo")                  .cast("boolean").alias("ativo"),
        F.col("data_criacao")           .cast("string") .alias("data_criacao"),
        F.col("data_alteracao")         .cast("string") .alias("data_alteracao"),
        F.col("tipo_pessoa")            .cast("string") .alias("tipo_pessoa"),
        F.col("observacoes_gerais")     .cast("string") .alias("observacoes_gerais"),
        F.element_at(F.col("perfis"), 1).cast("string") .alias("perfis"),
        F.col("endereco.logradouro")    .cast("string") .alias("endereco_logradouro"),
        F.col("endereco.numero")        .cast("string") .alias("endereco_numero"),
        F.col("endereco.complemento")   .cast("string") .alias("endereco_complemento"),
        F.col("endereco.bairro")        .cast("string") .alias("endereco_bairro"),
        F.col("endereco.cidade")        .cast("string") .alias("endereco_cidade"),
        F.col("endereco.estado")        .cast("string") .alias("endereco_uf"),
        F.col("endereco.pais")          .cast("string") .alias("endereco_pais"),
        F.col("endereco.cep")           .cast("string") .alias("endereco_cep"),
        F.current_timestamp()           .cast("string") .alias("_loaded_at"),
    )
    .dropDuplicates(
        ["id"]
    )
)

# conta azul - sales
df_silver_contaazul_sales = (
    df_bronze_contaazul_sales_list.alias("sl")
    .join(
        df_bronze_contaazul_sales_details.alias("sd"),
        on=F.col("sl.id") == F.col("sd.sales_id"),
        how="left",
    )
    .select(
        F.col("sl.id")                      .cast("string") .alias("id"),
        F.col("sl.total")                   .cast("float")  .alias("total"),
        F.col("sl.id_legado")               .cast("integer").alias("id_legado"),
        F.col("sl.data")                    .cast("string") .alias("data"),
        F.col("sl.criado_em")               .cast("string") .alias("criado_em"),
        F.col("sl.data_alteracao")          .cast("string") .alias("data_alteracao"),
        F.col("sl.tipo")                    .cast("string") .alias("tipo"),
        F.col("sl.itens")                   .cast("string") .alias("itens_tipo"),
        F.col("sl.condicao_pagamento")      .cast("boolean").alias("condicao_pagamento"),
        F.col("sl.numero")                  .cast("integer").alias("numero"),
        F.col("sl.cliente.id")              .cast("string") .alias("cliente_id"),
        F.col("sl.cliente.nome")            .cast("string") .alias("cliente_nome"),
        F.col("sl.cliente.email")           .cast("string") .alias("cliente_email"),
        F.col("sl.cliente.telefone")        .cast("string") .alias("cliente_telefone"),
        F.col("sl.cliente.endereco")        .cast("string") .alias("cliente_endereco"),
        F.col("sl.cliente.cidade")          .cast("string") .alias("cliente_cidade"),
        F.col("sl.cliente.estado")          .cast("string") .alias("cliente_estado"),
        F.col("sl.cliente.pais")            .cast("string") .alias("cliente_pais"),
        F.col("sl.cliente.cep")             .cast("string") .alias("cliente_cep"),
        F.col("sd.venda.situacao.nome")     .cast("string") .alias("situacao_nome"),
        F.col("sd.venda.situacao.descricao").cast("string") .alias("situacao_descricao"),
        F.col("sl.status_email.status")     .cast("string") .alias("status_email_status"),
        F.col("sl.status_email.enviado_em") .cast("string") .alias("status_email_enviado_em"),
        F.current_timestamp()               .cast("string") .alias("_loaded_at"),
    )
    .dropDuplicates(
        ["id"]
    )
)

## LOADING TABLES
# clickup - silver tables
save_nekt_table(df_silver_clickup_users,                        "Silver", "studio61_clickup_silver_users",                      "studio61_clickup_silver")
save_nekt_table(df_silver_clickup_spaces,                       "Silver", "studio61_clickup_silver_spaces",                     "studio61_clickup_silver")
save_nekt_table(df_silver_clickup_time_entries,                 "Silver", "studio61_clickup_silver_time_entries",               "studio61_clickup_silver")

# conta azul - silver tables
save_nekt_table(df_silver_contaazul_accounts_payable,           "Silver", "studio61_contaazul_silver_accounts_payable",         "studio61_contaazul_silver")
save_nekt_table(df_silver_contaazul_accounts_receivable,        "Silver", "studio61_contaazul_silver_accounts_receivable",      "studio61_contaazul_silver")
save_nekt_table(df_silver_contaazul_categories,                 "Silver", "studio61_contaazul_silver_categories",               "studio61_contaazul_silver")
save_nekt_table(df_silver_contaazul_customers,                  "Silver", "studio61_contaazul_silver_customers",                "studio61_contaazul_silver")
save_nekt_table(df_silver_contaazul_combined_accounts,          "Silver", "studio61_contaazul_silver_combined_accounts",        "studio61_contaazul_silver")
save_nekt_table(df_silver_contaazul_dre_items,                  "Silver", "studio61_contaazul_silver_dre_items",                "studio61_contaazul_silver")
save_nekt_table(df_silver_contaazul_dre_subitems,               "Silver", "studio61_contaazul_silver_dre_subitems",             "studio61_contaazul_silver")
save_nekt_table(df_silver_contaazul_dre_financial_categories,   "Silver", "studio61_contaazul_silver_dre_financial_categories", "studio61_contaazul_silver")
save_nekt_table(df_silver_contaazul_financial_accounts,         "Silver", "studio61_contaazul_silver_financial_accounts",       "studio61_contaazul_silver")
save_nekt_table(df_silver_contaazul_installments,               "Silver", "studio61_contaazul_silver_installments",             "studio61_contaazul_silver")
save_nekt_table(df_silver_contaazul_installments_payments,      "Silver", "studio61_contaazul_silver_installments_payments",    "studio61_contaazul_silver")
save_nekt_table(df_silver_contaazul_people,                     "Silver", "studio61_contaazul_silver_people",                   "studio61_contaazul_silver")
save_nekt_table(df_silver_contaazul_sales,                      "Silver", "studio61_contaazul_silver_sales",                    "studio61_contaazul_silver")