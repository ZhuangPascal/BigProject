from airflow.providers.postgres.hooks.postgres import PostgresHook

def insert_into_postgres(job_data: tuple):
    pg_hook = PostgresHook(postgres_conn_id="postgres_default")
    connection = pg_hook.get_conn()
    cursor = connection.cursor()

    sql = """
    INSERT INTO job_posts (date_posted, title, company, location, job_description, profile_searched, link)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (link) DO UPDATE 
    SET 
        date_posted = EXCLUDED.date_posted,
        title = EXCLUDED.title,
        company = EXCLUDED.company,
        location = EXCLUDED.location,
        job_description = EXCLUDED.job_description,
        profile_searched = EXCLUDED.profile_searched
    WHERE job_posts.date_posted <> EXCLUDED.date_posted;
    """

    cursor.execute(sql, job_data)
    connection.commit()
    cursor.close()
    connection.close()

    print("The data has been imported into the database")