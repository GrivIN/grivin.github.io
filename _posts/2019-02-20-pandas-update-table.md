---
layout: post
title:  "Pandas update DB table contents (WIP)"
date:   2019-02-20 16:16:01 +0100
categories: [pandas]
tags: [pandas, postgresql, mysql]
---

I use Pandas in multiple projects to process data. In all of them at some point I faced problem with updating existing data in target tables. With Pandas internals you can only replace whole table or add new rows to existing.

There are 2 solutions: implement update query in sqlalchemy from pandas DataFrame or update target data using PostgreSQL CSV upload

## Update with sqlalchemy

### Split data to insert or update

```python
def calculate_changes(
        session, data, sqla_model,
        columns=None, exclude_columns=None,
        extra_update_filter_func=None):
    if exclude_columns is None:
        exclude_columns = []
    if columns is None:
        columns = [
            column.key
            for column in sqla_model.__table__.columns
            if column.key in data.columns
        ]
    new = pd.DataFrame(columns=columns)
    toupdate = pd.DataFrame(columns=columns)
    uptodate = pd.DataFrame(columns=columns)

    index_columns = list(data.index.names)
    if len(index_columns) > 1:
        records = [dict(zip(index_columns, x)) for x in data.index]
    else:
        records = [{index_columns[0]:x} for x in data.index]

    filters = []
    for record in records:
        filters.append(
            and_(*[
                getattr(sqla_model, name) == value
                for name, value in record.items()
            ])
        )
    db_data = pd.DataFrame()
    for chunk in as_chunks(filters, CHUNKSIZE):
        query = session.query(sqla_model).filter(
            or_(*chunk)
        ).statement
        chunk_data = pd.read_sql(
            query,
            session.connection()
        )
        db_data = pd.concat([db_data, chunk_data])
    data = data.reset_index()

    if not len(db_data):
        columns = list(filter(lambda x: x not in index_columns, columns))
        new = data.set_index(index_columns)
        return new[columns], toupdate[columns], uptodate

    db_data = db_data.astype(
        {
            column: data[column].dtype
            for column in (index_columns + columns)
            if column in data.columns
        },
        errors='raise'
    )

    for column in index_columns:
        try:
            check_types(data, db_data, column)
        except AssertionError as e:
            log.error(
                "new:\n{}\ndb:\n{}".format(
                    data[[column]], db_data[[column]]
                )
            )
            raise

    columns = list(filter(lambda x: x not in index_columns, columns))
    calc = pd.merge(
        db_data, data,
        on=index_columns,
        how='right', suffixes=('_db', '_new')
    )
    calc = calc.set_index(index_columns)

    calc['_new'] = False
    calc['_toupdate'] = False
    calc['_uptodate'] = False

    calc['_new'] = calc.apply(
        is_new(columns),
        axis=1
    )
    calc['_toupdate'] = calc.apply(
        is_toupdate(columns, extra_update_filter_func),
        axis=1
    )
    calc['_uptodate'] = calc.apply(
        is_uptodate(columns),
        axis=1
    )

    new = calc[calc['_new'] == True]
    toupdate = calc[calc['_toupdate'] == True]
    uptodate = calc[calc['_uptodate'] == True]

    new = new.rename(columns={
        x + '_new': x
        for x in columns
    })[columns]
    toupdate = toupdate.rename(columns={
        x + '_new': x
        for x in columns
    })[columns]
    uptodate = uptodate[[
        x + '_db'
        for x in columns] + [x + '_new' for x in columns]
    ]

    return new, toupdate, uptodate
```

```python
def is_new(columns):
    columns = [x + '_db' for x in columns]

    def inner(row):
        return all([pd.isnull(row[column]) for column in columns])

    return inner
```

```python
def is_toupdate(columns, extra_update_filter_func=None):
    """ Returns True if row should to be updated

    extra_update_filter_func must return True if row should be kept
    """
    columns_db = [x + '_db' for x in columns]
    columns_new = [x + '_new' for x in columns]

    if extra_update_filter_func is None:
        def extra_update_filter_func(row):
            return True

    def inner(row):
        log.debug('is_toupdate:', extra={'data': row})
        if row['_new'] is True:
            log.debug('is_toupdate: is new')
            return False
        if not extra_update_filter_func(row):
            log.debug('is_toupdate: extra_update_filter_func is false')
            return False
        for column_db, column_new in zip(columns_db, columns_new):
            if isinstance(row[column_db], numbers.Number):
                if not np.isclose(
                        [row[column_db]], [row[column_new]],
                        rtol=1e-03, atol=1e-05, equal_nan=True):
                    log.debug('is_toupdate: isclose is false')
                    return True
            else:
                if row[column_db] != row[column_new]:
                    log.debug('is_toupdate: != is true')
                    return True
        return False

    return inner
```

```python
def is_uptodate(columns):
    def inner(row):
        return row['_new'] is False and row['_toupdate'] is False
    return inner
```

```python
def check_types(left_df, right_df, field_left, field_right=None):
    if not field_right:
        field_right = field_left

    assert field_left in left_df.columns, '{} not found in left_df'.format(
        field_left
    )
    assert field_right in right_df.columns, '{} not found in right_df'.format(
        field_right
    )
    assert (
        left_df[field_left].dtype == right_df[field_right].dtype
    ), (
        'Different column types on left-right dataframe, merge/join will fail.'
        ' ({}: {} == {}: {})'.format(
            field_left,
            left_df[field_left].dtype,
            field_right,
            right_df[field_right].dtype
        )
    )
```


### Insert rows

```python
def insert(session, data, sqla_model, index=True):
    data.to_sql(
        sqla_model.__table__.name,
        session.connection(),
        index=index,
        if_exists='append'
    )
```

### Update rows
```python
def update(session, data, sqla_model):
    index_columns = data.index.names
    renames = {x: 'b_' + x for x in index_columns}
    data = data.reset_index()
    data = data.rename(columns=renames)
    columns_to_update = {
        key: bindparam(key)
        for key in data.columns
        if key not in ['b_' + x for x in index_columns]
    }
    statement = sqla_model.__table__.update().where(
        and_(*[
            getattr(sqla_model, x) == bindparam(y)
            for x, y in renames.items()
        ])
    ).values(
        **columns_to_update
    )
    records = data.to_dict('records')
    for chunk in as_chunks(records, CHUNKSIZE):
        session.execute(
            statement,
            records
        )
        session.commit()
```


## Update with csv upload

```python
def publish(source_df, table_name, sql_host):
    tmp_table_name = f'{table_name}_temp'

    wheres = [f'{field}' for field in source_df.index.names]
    wheres_str = ', '.join(wheres)

    insert_str = ', '.join(source_df.index.names + source_df.columns)

    sets = [f'{field}=excluded.{field}' for field in source_df.columns]
    sets_str = ', '.join(sets)

    query = f"""INSERT INTO {table_name} ({insert_str}) (
        SELECT {insert_str} FROM {tmp_table_name})
        ON CONFLICT ({wheres_str}) DO UPDATE
        SET {sets_str};
    ;"""
    with StringIO() as f:
        source_df.to_csv(f, header=False)
        f.seek(0)
        conn = psycopg2.connect(sql_host)
        cur = conn.cursor()
        cur.execute(f"""CREATE TEMP TABLE {tmp_table_name} ON COMMIT DROP AS SELECT * FROM {table_name} LIMIT 0;""")
        # Run additional modifications here, for ex:
        # cur.execute(f"""ALTER TABLE {tmp_table_name} DROP COLUMN id;""")
        cur.copy_expert(f"""COPY {tmp_table_name} FROM STDIN WITH (FORMAT CSV)""", f)
        cur.execute(query)
        conn.commit()
```