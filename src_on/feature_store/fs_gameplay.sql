-- Agora começamos a criar as variáveis de gameplay.

with tb_level as (
  select 
  idPlayer,
  vlLevel,
  row_number() over (partition by idPlayer order by dtCreatedAt desc) as rnPlayer

  from bronze_gc.tb_lobby_stats_player
  where dtCreatedAt < '[date]'
  and dtCreatedAt >= date_add('[date]', -30)

  order by idPlayer, dtCreatedAt
),

tb_level_final as (
  select *
  from tb_level
  where rnPlayer = 1
),

tb_gameplay_stats as (
  select 
    idPlayer,
    count(distinct idLobbyGame) as qtPartidas,
    count(distinct date(dtCreatedAt)) as qtDias,

    avg(qtKill) as avgKill,
    avg(qtAssist) as avgAssist,
    avg(qtDeath) as avgDeath,
    avg(qtHs) as avgHs,
    avg(qtBombeDefuse) as avgBombeDefuse,
    avg(qtBombePlant) as avgBombePlant,
    avg(qtTk) as avgTk,
    avg(qtTkAssist) as avgTkAssist,
    avg(qt1Kill) as avg1Kill,
    avg(qt2Kill) as avg2Kill,
    avg(qt3Kill) as avg3Kill,
    avg(qt4Kill) as avg4Kill,
    avg(qt5Kill) as avg5Kill,
    avg(qtPlusKill) as avgPlusKill,
    avg(qtFirstKill) as avgFirstKill,
    avg(vlDamage) as avgDamage,
    avg(qtHits) as avgHits,
    avg(qtShots) as avgShots,
    avg(qtLastAlive) as avgLastAlive,
    avg(qtClutchWon) as avgClutchWon,
    avg(qtRoundsPlayed) as avgRoundsPlayed,
    avg(qtSurvived) as avgSurvived,
    avg(qtTrade) as avgTrade,
    avg(qtFlashAssist) as avgFlashAssist,


     count (case when dayofweek(dtCreatedAt) = 1 then date(dtCreatedAt) end) / count(distinct date(dtCreatedAt)) as propDia1,
     count (case when dayofweek(dtCreatedAt) = 2 then date(dtCreatedAt) end) / count(distinct date(dtCreatedAt)) as propDia2,
     count (case when dayofweek(dtCreatedAt) = 3 then date(dtCreatedAt) end) / count(distinct date(dtCreatedAt)) as propDia3,
     count (case when dayofweek(dtCreatedAt) = 4 then date(dtCreatedAt) end) / count(distinct date(dtCreatedAt)) as propDia4,
     count (case when dayofweek(dtCreatedAt) = 5 then date(dtCreatedAt) end) / count(distinct date(dtCreatedAt)) as propDia5,
     count (case when dayofweek(dtCreatedAt) = 6 then date(dtCreatedAt) end) / count(distinct date(dtCreatedAt)) as propDia6,
     count (case when dayofweek(dtCreatedAt) = 7 then date(dtCreatedAt) end) / count(distinct date(dtCreatedAt)) as propDia7,

    min(datediff( '[date]', dtCreatedAt)) as qtRecencia,

    avg(flWinner) as winRate,

    avg(qtHs / qtKill) as avgHsRate,
    sum(qtHs) / sum(qtKill) as vlHsRate,

    avg((qtKill + qtAssists / coalesce(qtDeath,1))) as avgKDA,
    coalesce(sum(qtKill + qtAssist) / sum(coalesce(qtDeath,1)), 0) as KDAgeral,

    avg(coalesce(qtKill,0) / coalesce(qtDeath,1)) as avgKDR,
    sum(coalesce(qtKill,0)) / sum(coalesce(qtDeath,1)) as vlKDR,

    count (distinct case when descMapName = 'de_anciente' then idLobbyGame end) / count(distinct idLobbyGame) as propAnciente,
    count (distinct case when descMapName = 'de_overpass' then idLobbyGame end) / count(distinct idLobbyGame) as propOverpass,
    count (distinct case when descMapName = 'de_vertigo' then idLobbyGame end) / count(distinct idLobbyGame) as propVertigo,
    count (distinct case when descMapName = 'de_nuke' then idLobbyGame end) / count(distinct idLobbyGame) as propNuke,
    count (distinct case when descMapName = 'de_train' then idLobbyGame end) / count(distinct idLobbyGame) as propTrain,
    count (distinct case when descMapName = 'de_mirage' then idLobbyGame end) / count(distinct idLobbyGame) as propMirage,
    count (distinct case when descMapName = 'de_dust2' then idLobbyGame end) / count(distinct idLobbyGame) as propDust2,
    count (distinct case when descMapName = 'de_inferno' then idLobbyGame end) / count(distinct idLobbyGame) as propIferno


  from bronze_gc.tb_lobby_stats_player
  where dtCreatedAt < '[date]'
  and dtCreatedAt >= date_add('[date]', -30)

  group by idPlayer
)

select 
  '[date]' as dtRef,
  t1.*,
  t2.vlLevel
from tb_gameplay_stats as t1
left join tb_level_final as t2
on t1.idPlayer = t2.idPlayer
