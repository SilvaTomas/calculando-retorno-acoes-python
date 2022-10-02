# Databricks notebook source
# MAGIC %md
# MAGIC ## Calculando Retorno de ações com Python
# MAGIC 
# MAGIC Uma das tarefa importante no mercado financeiro é analisar os retornos históricos de investimentos.
# MAGIC 
# MAGIC Para realizar esta análise, precisaremos de dados históricos para os ativos.
# MAGIC 
# MAGIC Existem muitos fontes de dados, usaremos os dados do finance.yahoo.com.
# MAGIC 
# MAGIC Em python podemos fazer isso usando o módulo pandas-datareader.
# MAGIC 
# MAGIC Neste projeto iremos:
# MAGIC 
# MAGIC - Baixar preços;
# MAGIC - Calcular Devoluções;
# MAGIC - Calcular média e desvio padrão dos retornos;
# MAGIC 
# MAGIC O projeto será desenvolvido em um notebook DataBricks

# COMMAND ----------

#Instalando o módulo pandas_datareader no Data Bricks

pip install pandas_datareader

# COMMAND ----------

#Importando os módulos necessários:

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import pandas_datareader as web

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Analisando uma única ação
# MAGIC 
# MAGIC O Bank of America é uma das principais instituições financeiras do mundo, atendendo pessoas físicas, pequenas e médias empresas, grandes corporações e governos com uma gama de produtos e serviços bancários, de gestão de investimentos, serviços financeiros e de gestão de risco.

# COMMAND ----------

BOA = web.get_data_yahoo('BAC', start='2014-09-26', end='2022-09-26')

# COMMAND ----------

display(BOA.head())

# COMMAND ----------

plt.figure(figsize=(20, 10))
BOA['Adj Close'].plot()
plt.xlabel('Data')
plt.ylabel('Ajustado')
plt.title('Bank Of America Preço x Data')
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Calculando os retornos diários e mensais

# COMMAND ----------

BOA_RETORNO_DIÁRIO = BOA['Adj Close'].pct_change()
print(BOA_RETORNO_DIÁRIO.head())

# COMMAND ----------

BOA_RETORNO_MENSAL = BOA['Adj Close'].resample('M').ffill().pct_change()
print(BOA_RETORNO_MENSAL.head())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Plotando gráficos para os retornos diários e mensais

# COMMAND ----------

fig = plt.figure(figsize=(20, 10))
ax1 = fig.add_axes([0.1,0.1,0.8,0.8])
ax1.plot(BOA_RETORNO_DIÁRIO)
ax1.set_xlabel("Data")
ax1.set_ylabel("Percentual")
ax1.set_title("Retorno Diário do Bank Of America")
plt.show()

# COMMAND ----------

fig = plt.figure(figsize=(20, 10))
ax1 = fig.add_axes([0.1,0.1,0.8,0.8])
ax1.plot(BOA_RETORNO_MENSAL)
ax1.set_xlabel("Data")
ax1.set_ylabel("Percentual")
ax1.set_title("Retorno Mensal do Bank Of America")
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Com os gráficos de retornos diários, podemos concluir que os retornos são bastante voláteis e a ação pode movimentar +/- 5% em qualquer dia. 
# MAGIC 
# MAGIC Para ter uma noção de quão extremos os retornos podem ser, podemos plotar um histograma.

# COMMAND ----------

fig = plt.figure(figsize=(7, 11.32))
ax1 = fig.add_axes([0.1,0.1,0.8,0.8])
BOA_RETORNO_DIÁRIO.plot.hist(bins = 60)
ax1.set_xlabel("Retorno Diário %")
ax1.set_ylabel("Percentual")
ax1.set_title("Dados de Retorno Diário do Bank Of América")
ax1.text(-0.10,200,"Extreme Low\nreturns")
ax1.text(0.10,200,"Extreme High\nreturns")
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Calculando os retornos acumulativos da ação
# MAGIC 
# MAGIC Os gráficos de retornos diários e mensais são úteis para entender a volatilidade da ação.
# MAGIC 
# MAGIC Para saber crescimento do nosso investimento precisamos calcular o retorno total do investimento. 
# MAGIC 
# MAGIC Para isso vamos calcular o retorno acumulativo do investimento com a função cumprod().

# COMMAND ----------

BOA_RETORNO_CUM = (BOA_RETORNO_DIÁRIO + 1).cumprod()

# COMMAND ----------

fig = plt.figure(figsize=(10,7))
ax1 = fig.add_axes([0.1,0.1,0.8,0.8])
BOA_RETORNO_CUM.plot()
ax1.set_xlabel("Data")
ax1.set_ylabel("Crescimento para $1 investido")
ax1.set_title("Dados da Rentabilidade Diária Acumulada do Bank Of America")
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Este gráfico mostra o retorno acumulativo das ações do Bank of America desde 2015.
# MAGIC 
# MAGIC Alguém poderia ter feito US$ 2.3 em um investimento de US$ 1 desde 2015.

# COMMAND ----------

fig = plt.figure(figsize=(10,7))
ax1 = fig.add_axes([0.1,0.1,0.8,0.8])
BOA_RETORNO_MENSAL_CUM = (BOA_RETORNO_MENSAL + 1).cumprod()
BOA_RETORNO_MENSAL_CUM.plot()
ax1.set_xlabel('Data')
ax1.set_ylabel('Crescimento para $1 Investido')
ax1.set_title('Dados da Rentabilidade Mensal Acumulada do Bank Of America')
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Podemos visualizar que o gráfico de retornos mensais é muito mais suave que o gráfico diário.

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Múltiplas ações
# MAGIC ### Baixando os dados do mercado de ações para várias ações.
# MAGIC 
# MAGIC - 'AMZN': Amazon
# MAGIC - 'AAPL': Apple
# MAGIC - 'NFLX': Netflix
# MAGIC - 'GOOG': Google
# MAGIC - 'BAC': Bank Of America

# COMMAND ----------

labels = ['AMZN', 'AAPL', 'NFLX', 'GOOG', 'BAC']
mulplt_stocks = web.get_data_yahoo(labels, 
                                   start='2014-09-26',
                                   end='2022-09-26')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Plotando os preços das ações

# COMMAND ----------

fig = plt.figure(figsize=(15,10))
ax1 = fig.add_subplot(321)
ax2 = fig.add_subplot(322)
ax3 = fig.add_subplot(323)
ax4 = fig.add_subplot(324)
ax5 = fig.add_subplot(325)
#ax6 = fig.add_subplot(326)
ax1.plot(mulplt_stocks['Adj Close']['AMZN'])
ax1.set_title('Amazon')
ax2.plot(mulplt_stocks['Adj Close']['AAPL'])
ax2.set_title('Apple')
ax3.plot(mulplt_stocks['Adj Close']['NFLX'])
ax3.set_title('Netflix')
ax4.plot(mulplt_stocks['Adj Close']['GOOG'])
ax4.set_title('Google')
ax5.plot(mulplt_stocks['Adj Close']['BAC'])
ax5.set_title('Bank Of America')
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Calculando os retornos para várias ações

# COMMAND ----------

multpl_stocks_daily_returns = mulplt_stocks['Adj Close'].pct_change()
multpl_stock_monthly_returns = mulplt_stocks['Adj Close'].resample('M').ffill().pct_change()

# COMMAND ----------

multpl_stock_monthly_returns.columns

# COMMAND ----------

plt.figure(figsize=(17, 8))
plt.plot((multpl_stock_monthly_returns + 1).cumprod(), label=multpl_stock_monthly_returns.columns)
plt.legend()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dados estatísticos

# COMMAND ----------

multpl_stock_monthly_returns.describe()

# COMMAND ----------

multpl_stock_monthly_returns.corr()

# COMMAND ----------

multpl_stock_monthly_returns.cov()
