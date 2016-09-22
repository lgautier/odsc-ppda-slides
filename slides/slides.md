
<!-- label:data-from-scratch_1_2 -->

```python
! git clone https://github.com/lgautier/project-tycho-utilities.git
```

<!-- label:data-from-scratch_2_2 -->

```python
! cd project-tycho-utilities/ && DBNAME=../tycho.db make all
```
---

<!-- label:sqlite -->

Opening a connection to a database (here an SQLite database)
and getting a cursor is straightforward.

```python
import sqlite3
dbfilename = "tycho.db"
dbcon = sqlite3.connect(dbfilename)
cursor = dbcon.cursor()
```

<!-- label:sqlite_firstquery -->

Our first query is simple: we want to fetch the cities
in states with a name starting with "M"

```python
sql = """
SELECT state, city
FROM location
WHERE state LIKE 'M%'
"""
cursor.execute(sql)
```

<!-- label:sqlite_firstresults -->
Results can then be pulled from the database, and further
computation done with Python.

If what we want is to count the number of cities in each state
matching our predicate, this can be written:

```python
from collections import Counter
ct = Counter()
for row_n, (state, city) in enumerate(cursor, 1):
    ct[state] += 1
print(ct.most_common(n=5))
```

<!-- label:sqlite_secondquery -->
Some of the post-processing done in Python can be pushed
back to the database

```python
cursor = dbcon.cursor()
sql = """
SELECT state, count(city) AS ct
FROM location
WHERE state LIKE 'M%'
GROUP BY state
ORDER BY ct DESC
LIMIT 5
"""
cursor.execute(sql)
print(cursor.fetchall())
```

<!-- label:sqlite_complexquery -->

Our database schema has an other table `casecount` that contains
the count of cases for several diseases broken down by city and date.

Now we want the answer to a slightly more complex question: for each state,
count the number of cities for which we have case counts for more than 5
different diseases. Oh, and sort the list of states in decreasing count order.
In fact, only report the first 5 states.

```python
sql = """
SELECT state, count(city) city_count
FROM (select D.location_id
      FROM (select location_id, COUNT(DISTINCT(disease_id)) AS disease_count
            FROM casecount
            GROUP BY location_id) AS D
      WHERE D.disease_count > 5) AS HD
INNER JOIN location
ON HD.location_id = location.id
GROUP BY state
ORDER BY city_count DESC
LIMIT 5
"""
cursor.execute(sql)

```

<!-- label:sqlalchemy_open -->
Opening the same database using an ORM (SQLalchemy).

```python
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine

Base = automap_base()

# engine, suppose it has two tables 'user' and 'address' set up
engine = create_engine("sqlite:///tycho.db")
```

<!-- label:sqlalchemy_reflect -->
Use reflection on the SQL side to create the objects from the database.
```python
Base.prepare(engine, reflect=True)
location = Base.classes.location
```

---

<!-- label:sqlalchemy_query -->
Make a query using SQLalchemy's methods.
```python
session = Session(engine)
from sqlalchemy import func # SQL functions

query = (session
         .query(location.state,
                func.count(location.city))
         .filter(location.state.like('M%'))
         .group_by(location.state)
         .order_by(func.count(location.city).desc())
         .limit(5))
res = query.all()

```

Note that SQL and ORM mapping are technology predating StackOverflow.

---

<!-- label:sqlalchemy_sql -->
Function composition is generating SQL code.
```python
from sqlalchemy.dialects import sqlite
print(str(query.statement.compile(dialect=session.bind.dialect))
      .replace('GROUP BY', '\nGROUP BY'))
```

---

<!-- label:dplyr_rpy2_string -->
<!-- config:two-columns -->

With dplyr, an SQL table is a data table.

```python
from rpy2.robjects import r

r_code = """
suppressMessages(require("dplyr"))
dbfilename <- '""" + dbfilename + """'
datasrc <- src_sqlite(dbfilename)
location_tbl <- tbl(datasrc, "location")

res <- filter(location_tbl,
              state %like% 'M%') %>%
       group_by(state) %>%
       count(state) %>%
       arrange(desc(n))
head(res, 5)
"""

res = r(r_code)
print(res)
```

---

We traded the knowledge of SQL for the knownledge of R.

---

<!-- label:dplyr_table -->
dplyr is not trying to map objects. It is focusing on databases
as sources of tables.
```{python}
from rpy2.robjects.lib import dplyr


datasrc  = dplyr.src_sqlite(dbfilename)
location_tbl = datasrc.get_table("location")
```
<!-- label:dplyr_query -->
The table can be queried using the dplyr interface.
```python
res =  (location_tbl
        .filter('state %like% "M%"')
        .group_by('state')
        .count('state')
        .arrange('desc(n)'))

print(res)

```

Strings are snippets of R code for dplyr.

R can be considered a domain-specific language (DSL) in the Python code.

---

<!-- label:dplyr_advanced -->

Let's implement our complex SQL query from early with dplyr.

```python
casecount_tbl = datasrc.get_table("casecount")

##
disease_per_city = (casecount_tbl
                    .group_by('location_id')
                    .summarize(n='count(distinct(disease_id))'))
##
high_disease = (disease_per_city
                .filter('n > 5'))
##
inner_join = dplyr.dplyr.inner_join
join_location = inner_join((location_tbl
                            .mutate(location_id="id")),
                           high_disease,
                           by="location_id")
res = (dplyr.DataFrame(join_location)
       .group_by('state')
       .summarize(n='count(city)')
       .arrange('desc(n)')
       .collect())

```
---

<!-- label:ggplot2_figure -->
The R package ggplot2 can also be used.
```python
from rpy2.robjects import r, globalenv
import rpy2.robjects.lib.ggplot2 as gg

p = (gg.ggplot(res.head(20)) +
     gg.geom_bar(gg.aes_string(x='factor(state, levels=as.character(state))', 
                               y='n'),
                 stat='identity') +
     gg.scale_x_discrete("State") +
     gg.scale_y_sqrt("# cities w/ >5 diseases"))
```

<!-- label:ggplot2_plot -->
<!-- config:split-output -->
Sending the resulting figure to a jupyter notebook output.

```python
from rpy2.ipython.ggplot import image_png
from IPython.display import display

display(image_png(p))
```

---
<!-- label:ggplot2_plot_map -->
<!-- config:split-output -->

```python
from rpy2.robjects import baseenv
state_abb = (dplyr.DataFrame({'state': baseenv.get('state.abb'),
                             'region': baseenv.get('state.name')})
             .mutate(region = 'tolower(region)'))
from rpy2.robjects.packages import importr
maps = importr('maps')
states = gg.map_data('state')

merge = baseenv.get('merge')
states_map = merge(states, state_abb, sort=False, by="region")
dataf_plot = merge(states_map, res, sort=False, by="state")
dataf_plot = dplyr.DataFrame(dataf_plot).arrange('order')

p = (gg.ggplot(dataf_plot) +
     gg.geom_polygon(gg.aes_string(x='long', y='lat', group='group', fill='n')) +
     gg.scale_fill_continuous(trans="sqrt") +
     gg.coord_map("albers",  at0 = 45.5, lat1 = 29.5))
display(image_png(p))
```

---

<!-- label:ggplot2_plot_pneumonia_prepare_1_2 -->

```python
sql_disease = """
SELECT date_from, count, city
FROM casecount
INNER JOIN disease
ON casecount.disease_id=disease.id
INNER JOIN location
ON casecount.location_id=location.id
WHERE disease.name='%s'
AND state='%s'
AND city IS NOT NULL
"""
sql = sql_disease % ('PNEUMONIA', 'MA')
```

<!-- label:ggplot2_plot_pneumonia_prepare_2_2 -->

```python
dataf = dplyr.DataFrame(dplyr.tbl(datasrc, dplyr.dplyr.sql(sql))).collect()
dataf = dataf.mutate(date='as.POSIXct(strptime(date_from, format="%Y-%m-%d"))')
dataf = dataf.mutate(month = 'format(date, "%m")',
                     year = 'format(date, "%Y")')
# sum per month
dataf_plot = (dataf
              .group_by('city', 'month','year')
              .summarize(count='sum(count)'))
# 
yearmonth_to_date = """
as.POSIXct(
    strptime(
        paste(year, month, "15", sep="-"),
        format="%Y-%m-%d")
    )
"""

dataf_plot = dataf_plot.mutate(date=yearmonth_to_date)
```

Initial plot:

```python
p = (gg.ggplot(dataf_plot) +
     gg.geom_line(gg.aes_string(x='date', y='count',
                                group='city')) +
     gg.scale_y_sqrt())
display(image_png(p))
```

Color Boston vs Other

```python
p = (gg.ggplot(dataf_plot
               .mutate(city_label='ifelse(city=="BOSTON", "Boston", "Other")')) +
     gg.geom_line(gg.aes_string(x='date', y='count',
                                group='city', color='city_label')) +
     gg.scale_y_sqrt())
display(image_png(p))
```

Something is strange before approx. 1925. Let's focus on the most recent
data.

```python
p = (gg.ggplot(dataf_plot
               .mutate(city_label='ifelse(city=="BOSTON", "Boston", "Other")')
               .filter('year > 1925')) +
     gg.geom_line(gg.aes_string(x='date', y='count',
                                group='city', color='city_label')) +
     gg.scale_y_sqrt())
display(image_png(p))
```

Data for relatively few city. We can color them individually.

```python
p = (gg.ggplot(dataf_plot
               .filter('year > 1925')) +
     gg.geom_line(gg.aes_string(x='date', y='count',
                                group='city', color='city')) +
     gg.scale_y_sqrt())
display(image_png(p))
```

Decreasing for Boston, increasing for Worcester.
```python
p = (gg.ggplot(dataf_plot
               .filter('year > 1925')) +
     gg.geom_line(gg.aes_string(x='date', y='count',
                                group='city', color='city'),
                  alpha=0.4) +
     gg.geom_smooth(gg.aes_string(x='date', y='count',
                                group='city', color='city')) +
     gg.scale_y_sqrt())
display(image_png(p))
```

<!-- label:ggplot2_plot_pneumonia -->
<!-- config:split-output -->

```python
p = (gg.ggplot(dataf_plot
               .filter('year > 1925')) +
     gg.geom_line(gg.aes_string(x='month', y='count',
     group='year', color='city')) +
     gg.facet_grid('city~.', scales="free_y") +
     gg.scale_y_sqrt())
display(image_png(p))
```

Which years correspond to largest number of cases in Boston ?

```python
set((dataf_plot
     .filter('count>200', 'year>1925')
     .rx2('year')))
```

Corresponds to largest number of cases in Fall Rivers.
Springfield and Worcester have different bad years.

```python
p = (gg.ggplot(dataf_plot
               .filter('year > 1925')) +
     gg.geom_line(gg.aes_string(x='month', y='count', 
                                group='year', 
                                color='year %in% c(1926,1929,1931,1933,1937)')) +
     gg.facet_grid('city~.', scales="free_y"))
display(image_png(p))
```

---

Function to plot monthly aggregates.

<!-- label:function_make_ggplot -->

```python
def foo(disease, state):
    sql = sql_disease % (disease, state)
    dataf = dplyr.DataFrame(dplyr.tbl(datasrc, dplyr.dplyr.sql(sql))).collect()
    dataf = dataf.mutate(date='as.POSIXct(strptime(date_from, format="%Y-%m-%d"))')
    dataf = dataf.mutate(month = 'format(date, "%m")',
                         year = 'format(date, "%Y")')

    dataf_plot = (dataf
                  .group_by('city', 'month','year')
                  .summarize(count='sum(count)'))
    
    dataf_plot = dataf_plot.mutate(date=yearmonth_to_date)
    p = (gg.ggplot(dataf_plot
               .filter('year > 1925')) +
         gg.geom_line(gg.aes_string(x='month', y='count+1',
	                            group='year', color='city')) +
         gg.facet_grid('city~.', scales="free_y") +
         gg.scale_y_sqrt() +
         gg.ggtitle(disease))
    display(image_png(p, height=600))
```

<!-- label:widget_ggplot -->

```python
from ipywidgets import interact
interact(foo,
         disease=('PNEUMONIA','INFLUENZA','MEASLES'),
         state=('MA','NH','CA'))
```

---

<!-- label:bokeh -->
<!-- config:split-output -->

```{.python #bokeh_scatter}
from bokeh.plotting import (figure, show,
                            ColumnDataSource,
                            output_notebook)
##from bokeh.resources import INLINE
output_notebook()

res = res.head(20)
plot = figure(x_range=list(res.rx2('state')))
source = ColumnDataSource(dict((x, tuple(res.rx2(x))) for x in res.colnames))
plot.vbar(x='state',
          bottom=0, top='n',
          width=0.5,
          color='STEELBLUE',
          source=source)
```

<!-- label:bokeh_show -->

```{.python}
show(plot)

```

---

<!-- label:spark_setup -->
Spark can be started from regular Python code.
```{.python}
import findspark
findspark.init()

import pyspark

conf = pyspark.conf.SparkConf()
(conf.setMaster('local[2]')
 .setAppName('ipython-notebook')
 .set("spark.executor.memory", "2g"))

sc = pyspark.SparkContext(conf=conf)
```

<!-- label:spark_dataframe -->
```python
from pyspark.sql import SQLContext, Row
sqlcontext = SQLContext(sc)
cursor.execute("SELECT * FROM location")
location = \
    sqlcontext.createDataFrame(cursor,
                               tuple(x[0] for x in cursor.description))
location.registerTempTable("location")

sql = """
SELECT * 
FROM (SELECT * FROM disease WHERE name='PNEUMONIA') AS disease
INNER JOIN casecount
ON disease.id=casecount.disease_id"""

cursor.execute(sql)
casecount = \
    sqlcontext.createDataFrame(cursor,
                               tuple(x[0] for x in cursor.description))
casecount.registerTempTable("casecount")
```

---

<!-- label:spark_query -->
SQL can be used to query the data.
```python
sql = """
SELECT state, count(city) AS ct
FROM location
GROUP BY state
ORDER BY ct DESC
"""
counts = sqlcontext.sql(sql)
```

<!-- label:spark_query_collect -->
The evaluation is only performed when the results are needed.
```python
res = counts.collect()
res[:3]
```

<!-- label:spark_mapreduce -->

Spark is particularly comfortable with map-reduce tasks.
The input data can be our table (stored in a RDBM).
Here we count the number of times suffixes are found in city names:

```python
names = (location
         .filter(location.city.isNotNull())
         .rdd
         .flatMap(lambda rec: [x[-5:] for x in rec.city.split()])
         .map(lambda word: (word.lower(), 1))
         .reduceByKey(lambda a, b: a+b))
names.takeOrdered(10, key = lambda x: -x[1])
```

---

<!-- label:spark_sqlmapreduce -->

We can also seamlessly use result table obtained from an SQL query
to perform map/reduce tasks:

```python
sql = """
SELECT city
FROM (SELECT * FROM casecount WHERE epiweek LIKE '1912%') AS sub
INNER JOIN location
ON location.id=sub.location_id
GROUP BY city
"""
y_1912 = sqlcontext.sql(sql)
names = (y_1912
         .filter(y_1912.city.isNotNull())
         .rdd
         .flatMap(lambda rec: [x[-5:] for x in rec.city.split()])
         .map(lambda word: (word.lower(), 1))
         .reduceByKey(lambda a,b: a+b))
names.takeOrdered(5, key = lambda x: -x[1])
```
---

<!-- label:spark_sqlmapreduceggplot_1_2 -->
```python
## --- SQL ---
sql = """
SELECT state, city, date_from, count AS ct
FROM (SELECT * FROM casecount WHERE epiweek LIKE '1912%') AS sub
INNER JOIN location
ON location.id=sub.location_id
"""
y_1912 = sqlcontext.sql(sql)

## --- Spark ---
cases = (y_1912
         .rdd
         .map(lambda rec: ((rec.state, int(rec.date_from.split('-')[1])),
                           rec.ct))
         .reduceByKey(lambda a,b: a+b)).collect()
```

<!-- label:spark_sqlmapreduceggplot_2_2 -->
<!-- config:split-output -->
```python
## --- R (from Python) ---
from rpy2.robjects import StrVector, IntVector, FactorVector, Formula
months = StrVector([str(x) for x in range(1,13)])
res = dplyr.DataFrame({'state': StrVector([x[0][0] for x in cases]),
                       'month': FactorVector([x[0][1] for x in cases],
                                             levels = months),
                       'count': IntVector([x[1] for x in cases])})
dataf_plot = merge(states_map, res, all_x=True, sort=False, by="state")
dataf_plot = dplyr.DataFrame(dataf_plot).arrange('order')

jetcols = StrVector(("#00007F", "#007FFF", "#7FFF7F", "#FF7F00", "#7F0000"))
p = (gg.ggplot(dataf_plot) +
     gg.geom_polygon(gg.aes_string(x='long', y='lat',
                                   group='group', fill='count')) +
     gg.coord_map("albers",  at0 = 45.5, lat1 = 29.5) +
     gg.scale_fill_gradientn(colors=jetcols, trans='sqrt') +
     gg.facet_wrap(facets=Formula("~month")) +
     gg.ggtitle("Cases of Pneumonia in 1912"))

display(image_png(p))
```

<!-- label:spark_sqlmapreduceggplot_3_2 -->
<!-- config:split-output -->
```python
dataf_now = dataf_plot.filter('month %in% c(9,10)')
p = (gg.ggplot(dataf_now) +
     gg.geom_polygon(gg.aes_string(x='long', y='lat',
                                   group='group', fill='count')) +
     gg.coord_map("albers",  at0 = 45.5, lat1 = 29.5) +
     gg.scale_fill_gradientn(colors=jetcols, trans='sqrt') +
     gg.facet_wrap(facets=Formula("~month")) +
     gg.ggtitle("Cases of Pneumonia in 1912"))

display(image_png(p))
```

<!-- label:spark_sqlmapreduceggplot_4_2 -->
<!-- config:split-output -->
```python
dataf_now = (dataf_plot
             .filter('month %in% c(9,10)',
                     'state %in% c("CT", "NY", "MA", "NJ", "NH", "VM")'))
p = (gg.ggplot(dataf_now) +
     gg.geom_polygon(gg.aes_string(x='long', y='lat',
                                   group='group', fill='count')) +
     gg.coord_map("albers",  at0 = 45.5, lat1 = 29.5) +
     gg.scale_fill_gradientn(colors=jetcols, trans='sqrt') +
     gg.facet_wrap(facets=Formula("~month")) +
     gg.ggtitle("Cases of Pneumonia in 1912"))

display(image_png(p))
```

