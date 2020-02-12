from pyspark import SparkContext,SparkConf
from pyspark.sql import Row
from pyspark.sql import HiveContext
import pyspark.sql.functions as f

sc = SparkContext("local", "NBA Stats")
hive_context = HiveContext(sc)
data = hive_context.table("default.nbahive")


#1 Max Height
maxheight = data.groupBy().max('Height').withColumnRenamed("max(Height)","max_height")
#maxheight.show()
result1 = data.join(maxheight,data.height == maxheight.max_height,"inner").select(data.playername,data.height).distinct()
result1.show()
result1.write.mode("overwrite").saveAsTable("default.tallest")


#2 Min Weight
minweight = data.groupBy().min('Weight').withColumnRenamed("min(Weight)","min_weight")
#minweight.show()
result2 = data.join(minweight,data.weight == minweight.min_weight,"inner").select('playerName','Weight').distinct()
result2.show()
result2.write.mode("overwrite").saveAsTable("default.minWeight")


#3 Most Team Wins 

wins = data.select('gmDate','teamAbbr','Result').filter(data.result=='Win').distinct()
wincount = wins.groupby('teamAbbr').count()
wincount = wincount.withColumnRenamed('count','win_count')
sorted_win = wincount.select('teamAbbr','win_count').sort(wincount.win_count.desc())
sorted_win.show(n=30)

sorted_win.write.mode("overwrite").saveAsTable("default.mostWins")
maxwins = wincount.groupBy().max('win_count').withColumnRenamed("max(win_count)","max_wins")
result3 = wincount.join(maxwins,wincount.win_count == maxwins.max_wins,"inner").select(wincount.teamAbbr,wincount.win_count)
result3.show()


#4 LeBron Average
lbj = data.select('playerName','PTS').filter(data.playername=='LeBron James')
avg_lbj = lbj.groupby('playerName').avg('PTS')
avg_lbj = avg_lbj.select('playerName',f.round('avg(PTS)',2).alias('avg'))
print("Lebron average points:")
avg_lbj.show()
avg_lbj.write.mode("overwrite").saveAsTable("default.avgLeBron")

#5 Top 10 player averages
avg_all = data.select('playerName','PTS').groupby('playerName').avg('PTS')
avg_all = avg_all.withColumnRenamed("avg(PTS)","avg_pts")
avg_sorted = avg_all.sort(avg_all.avg_pts.desc())
avg_sorted.show(n=10)
avg_sorted.write.mode("overwrite").saveAsTable("default.averages")


#6 Top 10 3pt shooters
threept = data.select('playerName','3PTM','3PTA','3PTPCT').groupby('playerName').agg({'3PTM':'sum','3PTA':'sum','3PTPCT':'avg'})
threept = threept.withColumnRenamed('sum(3PTM)','threePTM')
threept = threept.withColumnRenamed('sum(3PTA)','threePTA')
threept = threept.withColumnRenamed('avg(3PTPCT)','threePT%')
top_threept = threept.sort(threept.threePTM.desc())
top_threept.select('playerName','threePTM','threePTA','threePT%').show(n=10)
top_threept.write.mode("overwrite").saveAsTable("default.topThreePT")


#7 LeBron vs Durant
lbj = data.select('gmDate','playerName','PTS','TRB','AST','STL','BLK','TOV','PF','FGM','FGA','FGPCT','3PTM','3PTA','3PTPCT','FTM','FTA','FTPCT').filter(data.playername=='LeBron James')
lbj = lbj.withColumn('gmYear',f.year(f.to_date('gmDate')))
kd = data.select('gmDate','playerName','PTS','TRB','AST','STL','BLK','TOV','PF','FGM','FGA','FGPCT','3PTM','3PTA','3PTPCT','FTM','FTA','FTPCT').filter(data.playername=='Kevin Durant')
kd = kd.withColumn('gmYear' , f.year(f.to_date('gmDate')))
lbjavg=lbj.groupby('playerName','gmYear').agg({'PTS':'avg','TRB':'avg','AST':'avg','STL':'avg','BLK':'avg','TOV':'avg','PF':'avg','FGM':'avg','FGA':'avg','FGPCT':'avg','3PTM':'avg','3PTA':'avg','3PTPCT':'avg','FTM':'avg','FTA':'avg','FTPCT':'avg'})
lbjavg = (lbjavg.withColumnRenamed('avg(PTS)','PTS')
 .withColumnRenamed('avg(TRB)','TRB')
 .withColumnRenamed('avg(AST)','AST')
 .withColumnRenamed('avg(TOV)','TOV')
 .withColumnRenamed('avg(PF)','PF')
 .withColumnRenamed('avg(STL)','STL')
 .withColumnRenamed('avg(BLK)','BLK')
 .withColumnRenamed('avg(FGM)','FGM')
 .withColumnRenamed('avg(FGA)','FGA')
 .withColumnRenamed('avg(FGPCT)','FGPCT')
 .withColumnRenamed('avg(3PTM)','3PTM')
 .withColumnRenamed('avg(3PTA)','3PTA')
 .withColumnRenamed('avg(3PTPCT)','3PTPCT')
 .withColumnRenamed('avg(FTM)','FTM')
 .withColumnRenamed('avg(FTA)','FTA')
 .withColumnRenamed('avg(FTPCT)','FTPCT') )

result4 = lbjavg.select('gmYear','playerName',f.round('PTS',2).alias('PTS'),f.round('TRB',2).alias('TRB'),
 f.round('AST',2).alias('AST'),
 f.round('STL',2).alias('STL'),
 f.round('BLK',2).alias('BLK'),
 f.round('TOV',2).alias('TOV'),
 f.round('PF',2).alias('PF'),
 f.round('FGM',2).alias('FGM'),
 f.round('FGA',2).alias('FGA'),
 f.round('FGPCT').alias('FGPCT'),
 f.round('FTM',2).alias('FTM'),
 f.round('FTA',2).alias('FTA'),
 f.round('FTPCT',2).alias('FTPCT'))

kdavg=kd.groupby('playerName','gmYear').agg({'PTS':'avg','TRB':'avg','AST':'avg','STL':'avg','BLK':'avg','TOV':'avg','PF':'avg','FGM':'avg','FGA':'avg','FGPCT':'avg','3PTM':'avg','3PTA':'avg','3PTPCT':'avg','FTM':'avg','FTA':'avg','FTPCT':'avg'})
kdavg = (kdavg.withColumnRenamed('avg(PTS)','PTS')
 .withColumnRenamed('avg(TRB)','TRB')
 .withColumnRenamed('avg(AST)','AST')
 .withColumnRenamed('avg(TOV)','TOV')
 .withColumnRenamed('avg(PF)','PF')
 .withColumnRenamed('avg(STL)','STL')
 .withColumnRenamed('avg(BLK)','BLK')
 .withColumnRenamed('avg(FGM)','FGM')
 .withColumnRenamed('avg(FGA)','FGA')
 .withColumnRenamed('avg(FGPCT)','FGPCT')
 .withColumnRenamed('avg(3PTM)','3PTM')
 .withColumnRenamed('avg(3PTA)','3PTA')
 .withColumnRenamed('avg(3PTPCT)','3PTPCT')
 .withColumnRenamed('avg(FTM)','FTM')
 .withColumnRenamed('avg(FTA)','FTA')
 .withColumnRenamed('avg(FTPCT)','FTPCT') )

result5 = kdavg.select('gmYear','playerName',f.round('PTS',2).alias('PTS'),f.round('TRB',2).alias('TRB'),
 f.round('AST',2).alias('AST'),
 f.round('STL',2).alias('STL'),
 f.round('BLK',2).alias('BLK'),
 f.round('TOV',2).alias('TOV'),
 f.round('PF',2).alias('PF'),
 f.round('FGM',2).alias('FGM'),
 f.round('FGA',2).alias('FGA'),
 f.round('FGPCT',2).alias('FGPCT'),
 f.round('FTM',2).alias('FTM'),
 f.round('FTA',2).alias('FTA'),
 f.round('FTPCT',2).alias('FTPCT'))

lbjkd=result4.unionAll(result5)
lbjkd=lbjkd.sort(f.asc('gmYear'),f.asc('playerName'))
lbjkd.show()
lbjkd.write.mode("overwrite").saveAsTable("default.lbjkd")


#8 Best Bench Scorers
bench=data.select('playerName','PTS','FGPCT','3PTPCT','FTPCT','playerStat').filter(data.playerstat=="Bench")
benchavg=bench.groupby('playerName').agg({'PTS':'avg','FGPCT':'avg','3PTPCT':'avg','FTPCT':'avg','playerName':'count'})
benchavg=benchavg.withColumnRenamed('count(playerName)','no_of_games')
bestbench=benchavg.filter(benchavg.no_of_games>82)
bestbench=bestbench.select('playerName',f.round('avg(PTS)',2).alias('PTS'),
 f.round('avg(FGPCT)',2).alias('FGPCT'),
 f.round('avg(3PTPCT)',2).alias('3PTPCT'),
 f.round('avg(FTPCT)',2).alias('FTPCT'))

sortedbench=bestbench.sort(bestbench.PTS.desc())
sortedbench.show(n=10)
sortedbench.write.mode("overwrite").saveAsTable("default.bench")


#9 Most Turnovers(Player)
turnover= data.select('playerName','TOV')
total_turnover = turnover.groupby('playerName').agg(f.sum('TOV').alias('TOV'))
sorted_turnover = total_turnover.sort(f.desc('TOV'))
result6=sorted_turnover.select('playerName','TOV')
result6.show(n=10)
result6.write.mode("overwrite").saveAsTable("default.turnovers")


#10 AST-TOV Ratio
ast_tov=data.select('playerName','AST','TOV')
avg_ast_tov = ast_tov.groupby('playerName').agg(f.avg('AST').alias('AST'),f.avg('TOV').alias('TOV'))
ratio = avg_ast_tov.withColumn("AST_TOV_Ratio",f.col('AST')/f.col('TOV'))
ratio=ratio.select('playerName',f.round('AST',2).alias('AST'),
 f.round('TOV',2).alias('TOV'),
 f.round('AST_TOV_Ratio',2).alias('AST_TOV_Ratio'))
sorted_ratio=ratio.sort(ratio.AST_TOV_Ratio.desc())
best_pgs=sorted_ratio.filter(sorted_ratio.AST>7)
best_pgs.show()
best_pgs.write.mode("overwrite").saveAsTable("default.bestpgs")

