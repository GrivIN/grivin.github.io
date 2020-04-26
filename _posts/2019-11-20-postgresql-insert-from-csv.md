---
layout: post
title:  "Insert data table from CSV in PostgreSQL"
---

How to speed up uploading 20.3 MB CSV file into PostgreSQL tables with Python from `16.1s` to `134ms`(!)


Lets start with some basic environment preparations for our testing purposes. I use pipenv to setup an virtualenv
```bash
pipenv --python 3.7
pipenv shell
pipenv install psycopg2 ipython
```

also active PostgreSQL database is needed, let's use docker version just for convenience, remember to change the port if you have

```bash
docker run -p 5432:5432 --name some-postgres -e POSTGRES_PASSWORD=mysecretpassword postgres
```

lets get an example csv file from https://population.un.org (it is approx. 21,35MB)
```bash
wget https://population.un.org/wpp/Download/Files/1_Indicators%20\(Standard\)/CSV_FILES/WPP2019_TotalPopulationBySex.csv
```

and create corresponding table in the PostgreSQL database:
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
        poptotal numeric
    );
    ALTER TABLE public.population_by_sex ADD CONSTRAINT population_by_sex_pk PRIMARY KEY (locid,varid,time);
    '''
)
conn.commit()
```

Now we are ready to save the scv file into our database, first solution would be read file and insert its contents line by line.

```python
import csv
import psycopg2


def insert_1():
    conn = psycopg2.connect("host=localhost dbname=postgres user=postgres password=mysecretpassword")
    cur = conn.cursor()
    with open('WPP2019_TotalPopulationBySex.csv', 'r') as input_file:
        reader = csv.reader(input_file)
        next(reader)  # Skip the header row.
        for row in reader:
            cur.execute(
                "INSERT INTO public.population_by_sex VALUES (%s, %s, %s, %s, %s, %s, %f, %f, %f)",
                row
            )
    conn.commit()
```

Oh, we have a problem here... empty cells in csv are not interpreted as NULLs, let's fix that quickly with new version

```python
import csv
import psycopg2


def insert_1():
    conn = psycopg2.connect("host=localhost dbname=postgres user=postgres password=mysecretpassword")
    cur = conn.cursor()
    with open('WPP2019_TotalPopulationBySex.csv', 'r') as input_file:
        reader = csv.reader(input_file)
        next(reader)  # Skip the header row.
        for row in reader:
            for field_idx in range(9):
                row[field_idx] = row[field_idx] or None  # change empty strings into None's
            cur.execute(
                "INSERT INTO public.population_by_sex VALUES (%s, %s, %s, %s, %s, %s, %f, %f, %f)",
                row
            )
    conn.commit()
```

using `%time insert_1()` function in ipython I have got times around
```
CPU times: user 4.92 s, sys: 11.2 s, total: 16.1 s
Wall time: 1min 22s
```
that is a bit long for me, so lets crank the speed a little with direct csv upload, `copy_from` should do the trick.

```python
import psycopg2


def insert_2():
    conn = psycopg2.connect("host=localhost dbname=postgres user=postgres password=mysecretpassword")
    cur = conn.cursor()
    with open('WPP2019_TotalPopulationBySex.csv', 'r') as input_file:
        next(input_file)  # Skip the header row.
        cur.copy_from(input_file, 'public.population_by_sex', sep=',')
    conn.commit()
```
And another miss... there is a country with name `"African, Caribbean and Pacific (ACP) Group of States"`, comma inside the string is taken by PostgreSQL as field separator, so the last func we can use is `copy_expert`. With the small change we got:

```python
import psycopg2


def insert_2():
    conn = psycopg2.connect("host=localhost dbname=postgres user=postgres password=mysecretpassword")
    cur = conn.cursor()
    with open('WPP2019_TotalPopulationBySex.csv', 'r') as input_file:
        next(input_file)  # Skip the header row.
        cur.copy_expert("copy public.population_by_sex from stdin (format csv)", input_file)
    conn.commit()
```

using `%time insert_2()` function in ipython I have got times around
```
CPU times: user 54.1 ms, sys: 79.6 ms, total: 134 ms
Wall time: 1.07 s
```.
this is a lot better than `insert_1()`, great!


but now we have a problem... how to update data already inserted into the table?
I will do it in the next part
