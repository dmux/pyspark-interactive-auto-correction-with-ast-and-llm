/* 
----------------------------------------------------
 Exemplo de Código SAS para Teste de Transpilação
----------------------------------------------------
 Objetivo: Simular um processo de análise de vendas,
           juntando dados de clientes e vendas, 
           filtrando vendas de alto valor,
           categorizando-as e agregando por região.
----------------------------------------------------
*/

/* ----- 1. Criação de Dados de Exemplo ----- */

/* Dataset de Clientes */
DATA WORK.CUSTOMERS;
    INPUT CustomerID $ CustomerName $ Region $;
    DATALINES;
C1 Alice North
C2 Bob South
C3 Charlie East
C4 David North
C5 Eve West
;
RUN;

/* Dataset de Vendas */
DATA WORK.SALES;
    INPUT SaleID $ CustomerID $ Product $ Amount SaleDate;
    /* Informa ao SAS como ler a data (YYYYMMDD) */
    INFORMAT SaleDate yymmdd8.; 
    /* Define como exibir a data */
    FORMAT SaleDate yymmdd10.; 
    DATALINES;
S1 C1 Laptop 1200 20230115
S2 C2 Tablet 400 20230120
S3 C1 Monitor 300 20230210
S4 C3 Laptop 1150 20230215
S5 C4 Tablet 500 20230301
S6 C2 Laptop 1300 20230305
S7 C5 Monitor 250 20230312
S8 C1 Phone 800 20230401
;
RUN;

/* ----- 2. Junção de Dados de Clientes e Vendas ----- */
PROC SQL;
   CREATE TABLE WORK.CUSTOMER_SALES AS
   SELECT
      s.SaleID,
      s.Product,
      s.Amount,
      s.SaleDate,
      c.CustomerID,
      c.CustomerName,
      c.Region
   FROM WORK.SALES s
   LEFT JOIN WORK.CUSTOMERS c ON s.CustomerID = c.CustomerID;
QUIT;

/* ----- 3. Processamento e Análise das Vendas ----- */
DATA WORK.ANALYSIS_DATA;
    SET WORK.CUSTOMER_SALES;

    /* Filtra apenas vendas com valor acima de 500 */
    WHERE Amount > 500; 

    /* Cria uma categoria de venda baseada no valor */
    IF Amount > 1000 THEN SaleCategory = 'High Value';
    ELSE SaleCategory = 'Medium Value';

    /* Calcula uma coluna de bônus hipotético (5% do valor) */
    Bonus = Amount * 0.05;

    /* Mantém apenas as colunas relevantes para a próxima etapa */
    KEEP SaleID Product Amount SaleCategory Region CustomerName Bonus SaleDate; 
RUN;

/* ----- 4. Ordenação dos Dados Analisados ----- */
PROC SORT DATA=WORK.ANALYSIS_DATA OUT=WORK.SORTED_ANALYSIS;
    BY Region DESCENDING Amount; /* Ordena por Região (asc) e depois por Valor (desc) */
RUN;

/* ----- 5. Agregação de Vendas por Região ----- */
PROC SQL;
   CREATE TABLE WORK.REGION_SUMMARY AS
   SELECT
      Region,
      SUM(Amount) AS TotalSalesAmount FORMAT=DOLLAR12.2, /* Soma do valor total por região */
      COUNT(*) AS NumberOfHighValueSales, /* Contagem de vendas (já filtradas > 500) */
      AVG(Amount) AS AverageSaleAmount FORMAT=DOLLAR12.2 /* Média do valor por região */
   FROM WORK.SORTED_ANALYSIS
   GROUP BY Region
   ORDER BY TotalSalesAmount DESC; /* Ordena pelo total de vendas decrescente */
QUIT;

/* ----- 6. Impressão dos Resultados Finais ----- */

/* Imprime o sumário por região */
PROC PRINT DATA=WORK.REGION_SUMMARY NOOBS LABEL;
    TITLE 'Sumário de Vendas (> $500) por Região';
    LABEL TotalSalesAmount = "Valor Total das Vendas"
          NumberOfHighValueSales = "Número de Vendas"
          AverageSaleAmount = "Valor Médio por Venda";
RUN;

/* Imprime os detalhes das vendas analisadas e ordenadas */
PROC PRINT DATA=WORK.SORTED_ANALYSIS NOOBS LABEL;
    TITLE 'Detalhes das Vendas Analisadas (> $500) Ordenadas';
    LABEL SaleCategory = "Categoria da Venda"
          CustomerName = "Nome do Cliente";
RUN;

/* Limpa o título para não afetar próximos passos (boa prática) */
TITLE; 

/* ----- Fim do Código ----- */