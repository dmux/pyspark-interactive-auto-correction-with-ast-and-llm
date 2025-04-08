/* 1. Simulação de dados históricos de vendas mensais por produto */

data vendas_mensais;
    format data yymmn6.;
    input produto_id $ data :yymmn6. qtd_vendida;
    datalines;
P001 202201 120
P001 202202 150
P001 202203 180
P001 202204 210
P001 202205 200
P001 202206 250
P001 202207 230
P001 202208 220
P001 202209 190
P001 202210 210
P001 202211 240
P001 202212 270
P002 202201 80
P002 202202 100
P002 202203 120
P002 202204 110
P002 202205 150
P002 202206 160
P002 202207 170
P002 202208 140
P002 202209 130
P002 202210 160
P002 202211 190
P002 202212 200
;
run;

/* 2. Macro para preparar os dados por produto */

%macro separar_series();
    proc sql noprint;
        select distinct produto_id into :prod_list separated by ' ' 
        from vendas_mensais;
    quit;
%mend;

%separar_series()

/* 3. Macro para previsão com PROC ESM por produto */

%macro forecast_por_produto(produto_id);
    data _serie;
        set vendas_mensais;
        where produto_id = "&produto_id.";
    run;

    proc sort data=_serie;
        by data;
    run;

    /* Modelo ESM automático */
    proc esm data=_serie outfor=forecast_&produto_id. lead=3 print=all;
        id data interval=month;
        forecast qtd_vendida / model=best;
    run;
%mend;

/* 4. Macro loop para gerar forecasts de todos os produtos */

%macro rodar_forecast_para_todos;
    %let i=1;
    %do %while (%scan(&prod_list., &i) ne );
        %let produto = %scan(&prod_list., &i);
        %put ====> Executando Forecast para Produto: &produto.;
        %forecast_por_produto(&produto.)
        %let i = %eval(&i + 1);
    %end;
%mend;

%rodar_forecast_para_todos

/* 5. União de todos os forecasts */

data forecast_total;
    set 
    %let i=1;
    %do %while (%scan(&prod_list., &i) ne );
        forecast_%scan(&prod_list., &i)
        %let i = %eval(&i + 1);
    %end;
    ;
run;

/* 6. Enriquecimento com flags de previsão */

data forecast_final;
    set forecast_total;
    if actual = . then tipo = "Forecast";
    else tipo = "Histórico";
run;

/* 7. Relatório final */

proc sort data=forecast_final;
    by produto_id data;
run;

title "Previsão de Vendas por Produto (3 Meses à Frente)";
proc report data=forecast_final nowd;
    columns produto_id data tipo actual forecast;
    define produto_id / group 'Produto';
    define data / order 'Data';
    define tipo / display 'Tipo';
    define actual / analysis sum 'Real';
    define forecast / analysis sum 'Previsto';
run;
