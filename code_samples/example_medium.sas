/* Criação de datasets simulados */
data clientes;
    input cliente_id $ nome $ sexo $ data_nascimento : yymmdd10.;
    format data_nascimento yymmdd10.;
    datalines;
001 João M 1990-03-15
002 Maria F 1985-11-25
003 Ana F 1992-07-04
004 Pedro M 1979-12-11
;

data transacoes;
    input transacao_id $ cliente_id $ valor data_compra : yymmdd10. categoria $;
    format data_compra yymmdd10.;
    datalines;
T001 001 200.50 2023-01-10 Eletrônicos
T002 001 150.00 2023-02-15 Roupas
T003 002 300.00 2023-02-20 Supermercado
T004 003 120.00 2023-03-01 Eletrônicos
T005 003 80.00 2023-03-05 Roupas
T006 004 250.00 2023-01-25 Viagem
T007 002 60.00 2023-03-10 Roupas
;

/* Criação de uma tabela de categorias com códigos */
data categorias;
    input categoria $ codigo_categoria $;
    datalines;
Eletrônicos E01
Roupas R02
Supermercado S03
Viagem V04
;

/* Join entre transações e categorias para enriquecer os dados */
proc sql;
    create table transacoes_enriquecidas as
    select 
        t.*, 
        c.codigo_categoria
    from 
        transacoes t
    left join 
        categorias c
    on 
        t.categoria = c.categoria;
quit;

/* Join com clientes e derivação de idade */
data transacoes_completas;
    merge transacoes_enriquecidas(in=a) clientes(in=b);
    by cliente_id;
    if a and b;

    /* Cálculo da idade no momento da compra */
    idade_no_momento = intck('year', data_nascimento, data_compra);
    
    /* Segmentação de clientes */
    if idade_no_momento < 30 then segmento = 'Jovem';
    else if idade_no_momento < 50 then segmento = 'Adulto';
    else segmento = 'Sênior';
run;

/* Agregação de valores por cliente e segmento */
proc sql;
    create table resumo_clientes as
    select 
        cliente_id,
        nome,
        segmento,
        sexo,
        count(distinct transacao_id) as qtd_transacoes,
        sum(valor) as total_gasto,
        avg(valor) as valor_medio
    from 
        transacoes_completas
    group by 
        cliente_id, nome, segmento, sexo
    order by 
        total_gasto desc;
quit;

/* Exportação do resultado final */
proc print data=resumo_clientes noobs label;
    title "Resumo de Gastos por Cliente";
    label 
        cliente_id = "ID do Cliente"
        nome = "Nome"
        segmento = "Segmento"
        sexo = "Sexo"
        qtd_transacoes = "Qtd de Transações"
        total_gasto = "Total Gasto (R$)"
        valor_medio = "Valor Médio (R$)";
run;
