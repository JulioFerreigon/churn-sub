-- Databricks notebook source
-- DBTITLE 1,Tabela Players
select * from bronze_gc.tb_players

-- COMMAND ----------

-- DBTITLE 1,Tabela de Estatísticas de Partidas
select * from bronze_gc.tb_lobby_stats_players

-- COMMAND ----------

-- Para sabermos se o cliente ainda é ou não assinante, precisamos que a data de criação da medalha que informa se é plus ou premium foi criada e não foi removida ou expirada.
select * from bronze_gc.tb_players_medalha
where dtCreatedAt < dtExpiration
and dtCreatedAt < dtRemove

-- COMMAND ----------

-- DBTITLE 1,Tabela Dimensão de medalhas
select * from bronze_gc.tb_medalha

-- COMMAND ----------

-- Vamos checar qual período temos de dados para trabalharmos, para isso faremos o min e max de cada data que temos
select max(dtCreatedAt),
       min(dtCreatedAt),
       max(dtExpiration),
       min(dtExpiration),
       max(dtRemove),
       min(dtRemove)
from bronze_gc.tb_players_medalha

-- COMMAND ----------

-- Numero de assinantes ativos em um determinado período
select count(distinct idPlayer)
from bronze_gc.tb_players_medalha
where dtCreatedAt < '2020-02-20'
and dtRemove > '2020-02-20'
and dtCreatedAt < dtExpiration
and dtCreatedAt < dtRemove
and idMedal in (1,3)
