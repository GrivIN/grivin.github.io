---
layout: post
title:  "Update data table from CSV in PostgreSQL"
---

Lets quickly prepare out test enviromnent like we did in last post:

```bash
# Download the data file
wget https://population.un.org/wpp/Download/Files/1_Indicators%20\(Standard\)/CSV_FILES/WPP2019_TotalPopulationBySex.csv

# run the database in docker (will run in background)
docker run -d -p 5432:5432 --name blog-test-database -e POSTGRES_PASSWORD=mysecretpassword postgres

# create python virtual env (lets try bpython instead ipython this time)
pipenv --python 3.7
pipenv install psycopg2-binary bpython
pipenv shell
bpython
```
and create DB table in the PostgreSQL database (WARNING! there is an additional column in source file since we were using it last time):
```python
import psycopg2
conn = psycopg2.connect("host=localhost dbname=postgres user=postgres password=mysecretpassword")
cur = conn.cursor()
cur.execute(
    '''CREATE TABLE public.population_by_sex (
        locid int NOT NULL,
        location varchar(255) NOT NULL,
        varid int NOT NULL,
        variant varchar(20) NOT NULL,
        time int NOT NULL,
        midperiod numeric NOT NULL,
        popmale numeric,
        popfemale numeric,
        poptotal numeric,
        popdensity numeric
    );
    ALTER TABLE public.population_by_sex ADD CONSTRAINT population_by_sex_pk PRIMARY KEY (locid,varid,time);
    '''
)
conn.commit()
```

and let's add every 3rd line from csv (this tile we will include header line as well):

```python
import io
import psycopg2

conn = psycopg2.connect("host=localhost dbname=postgres user=postgres password=mysecretpassword")
cur = conn.cursor()

tmp_file = io.StringIO()
with open('WPP2019_TotalPopulationBySex.csv', 'r') as input_file:
    for num, line in enumerate(input_file):
        if num % 3 == 0:
            tmp_file.write(line)
    tmp_file.seek(0)
    cur.copy_expert(
        "COPY public.population_by_sex FROM STDIN WITH (FORMAT CSV, HEADER)", 
        tmp_file
    )
    conn.commit()
```

Notice that if we will try to run this code twice, on second attempt we will get an exception in return

```python
UniqueViolation                         Traceback (most recent call last)
<ipython-input-2-a6067173e443> in <module>
     13     cur.copy_expert(
     14         "COPY public.population_by_sex FROM STDIN WITH (FORMAT CSV, HEADER)",
---> 15         tmp_file
     16     )
     17     conn.commit()

UniqueViolation: duplicate key value violates unique constraint "population_by_sex_pk"
DETAIL:  Key (locid, varid, "time")=(4, 2, 1952) already exists.
CONTEXT:  COPY population_by_sex, line 2
```

So let's modify our code to do something to account that. Firstly let's create a temporary table in PostgreSQL and copy data there:

```python
    cur.execute(f"""
        CREATE TEMP TABLE 
            population_by_sex_tmp 
        ON COMMIT DROP 
        AS SELECT * FROM 
            population_by_sex 
        LIMIT 0;
    """)
```
put the data into temorary table
```python
    cur.copy_expert(
        """COPY population_by_sex_tmp FROM STDIN WITH (FORMAT CSV, HEADER)""", 
        tmp_file
    )
```

then let's copy data from temporary table to the main one, but let skip rows that are there already.
```python
    cur.execute(f"""
        INSERT INTO 
            population_by_sex 
            (
                locid,
               location,
                varid,
                variant,
                time,
                midperiod,
                popmale,
                popfemale,
                poptotal,
                popdensity
            ) 
        (
            SELECT 
                locid,
                location,
                varid,
                variant,
                time,
                midperiod,
                popmale,
                popfemale,
                poptotal,
                popdensity 
            FROM 
                population_by_sex_tmp
        )
        ON CONFLICT (
            locid,
            varid,
            time) 
        DO NOTHING;
    ;""")

```

That is it, simple as that. Of course we can change the how act on rows that already are in DB

```sql
INSERT INTO 
    population_by_sex 
    (
        locid,
        location,
        varid,
        variant,
        time,
        midperiod,
        popmale,
        popfemale,
        poptotal,
        popdensity
    ) 
    (
        SELECT 
            locid,
            location,
            varid,
            variant,
            time,
            midperiod,
            popmale,
            popfemale,
            poptotal,
            popdensity 
        FROM 
            population_by_sex_tmp
    )
ON CONFLICT (
    locid,
    varid,
    time) 
DO UPDATE SET 
    locid=excluded.locid,
    location=excluded.location,
    varid=excluded.varid,
    variant=excluded.variant,
    time=excluded.time,
    midperiod=excluded.midperiod,
    popmale=excluded.popmale,
    popfemale=excluded.popfemale,
    poptotal=excluded.poptotal,
    popdensity=excluded.popdensity
```

All together would look like this (now we can inpul all rows - prevoiusly to was approx 94k of rows):

```python
import io
import psycopg2

conn = psycopg2.connect("host=localhost dbname=postgres user=postgres password=mysecretpassword")
cur = conn.cursor()

with open('WPP2019_TotalPopulationBySex.csv', 'r') as input_file:
    cur.execute(f"""
        CREATE TEMP TABLE 
            population_by_sex_tmp 
        ON COMMIT DROP 
        AS SELECT * FROM 
            population_by_sex 
        LIMIT 0;
    """)
    cur.copy_expert(
        """COPY population_by_sex_tmp FROM STDIN WITH (FORMAT CSV, HEADER)""", 
        input_file
    )
    cur.execute(f"""
        INSERT INTO 
            population_by_sex 
            (
                locid,
               location,
                varid,
                variant,
                time,
                midperiod,
                popmale,
                popfemale,
                poptotal,
                popdensity
            ) 
        (
            SELECT 
                locid,
                location,
                varid,
                variant,
                time,
                midperiod,
                popmale,
                popfemale,
                poptotal,
                popdensity 
            FROM 
                population_by_sex_tmp
        )
        ON CONFLICT (
            locid,
            varid,
            time) 
        DO NOTHING;
    ;""")
    conn.commit()
```

Sucess, we got ~281k rows wihout any issues!