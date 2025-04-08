/************************************
* 1. Criação de Macros Parametrizadas
************************************/
%macro carrega_dados_transacoes(data_input=);
    data transacoes;
        infile "&data_input." dlm=',' dsd firstobs=2;
        input transacao_id :$10. cliente_id :$10. produto_id :$10. 
              valor :comma8.2 data_compra :yymmdd10. canal $;
        format data_compra yymmdd10. valor comma8.2;
    run;
%mend;

%macro carrega_dados_clientes(data_input=);
    data clientes;
        infile "&data_input." dlm=',' dsd firstobs=2;
        input cliente_id :$10. nome $ sexo $ data_nascimento :yymmdd10.
              renda_mensal :comma10.2 uf $ tipo_cliente $;
        format data_nascimento yymmdd10. renda_mensal comma10.2;
    run;
%mend;

/*******************************************
* 2. Execução das macros com arquivos fictícios
*******************************************/
%carrega_dados_transacoes(data_input=/caminho/transacoes.csv);
%carrega_dados_clientes(data_input=/caminho/clientes.csv);

/************************************
* 3. Validação e tratamento dos dados
************************************/
data clientes_limpos;
    set clientes;
    
    /* Correção de UF inválida */
    if length(uf) ne 2 then uf = 'ND';

    /* Categorização de renda */
    if renda_mensal < 2000 then faixa_renda = 'Baixa';
    else if renda_mensal < 6000 then faixa_renda = 'Média';
    else faixa_renda = 'Alta';

    idade = intck('year', data_nascimento, today());

    /* Classificação etária */
    if idade < 25 then faixa_idade = 'Jovem';
    else if idade <= 60 then faixa_idade = 'Adulto';
    else faixa_idade = 'Sênior';
run;

/************************************
* 4. Join dos dados
************************************/
proc sql;
    create table vendas_detalhadas as
    select 
        t.*, 
        c.nome, 
        c.sexo, 
        c.uf,
        c.tipo_cliente,
        c.faixa_renda,
        c.faixa_idade
    from 
        transacoes t
    inner join 
        clientes_limpos c
    on 
        t.cliente_id = c.cliente_id;
quit;

/***********************************************
* 5. Agregações e análise por grupos
***********************************************/
proc sql;
    create table resumo_vendas as
    select
        uf,
        tipo_cliente,
        faixa_renda,
        faixa_idade,
        count(distinct cliente_id) as num_clientes,
        count(*) as num_transacoes,
        sum(valor) as total_vendas,
        avg(valor) as ticket_medio
    from
        vendas_detalhadas
    group by
        uf, tipo_cliente, faixa_renda, faixa_idade;
quit;

/***********************************************
* 6. Classificação com ranking e segmentação
***********************************************/
proc rank data=resumo_vendas out=ranking_vendas ties=low descending;
    by uf;
    var total_vendas;
    ranks posicao_uf;
run;

/***********************************************
* 7. Geração de relatório formatado
***********************************************/
proc report data=ranking_vendas nowd;
    columns uf tipo_cliente faixa_renda faixa_idade 
            num_clientes num_transacoes total_vendas ticket_medio posicao_uf;

    define uf / group 'UF';
    define tipo_cliente / group 'Tipo Cliente';
    define faixa_renda / group 'Faixa de Renda';
    define faixa_idade / group 'Faixa Etária';
    define num_clientes / analysis sum 'Nº Clientes';
    define num_transacoes / analysis sum 'Nº Transações';
    define total_vendas / analysis sum format=comma10.2 'Total Vendas (R$)';
    define ticket_medio / analysis mean format=comma10.2 'Ticket Médio (R$)';
    define posicao_uf / display 'Ranking por UF';
run;

/***********************************************
* 8. Exportação para CSV
***********************************************/
proc export data=ranking_vendas
    outfile="/caminho/relatorio_vendas.csv"
    dbms=csv
    replace;
run;
