import logging
from pgsqldata import pgsql_query_params, safe_alphanum

syslog = logging.getLogger(f"root.{__name__}")

async def create_journals_table(pgpool):

    return await pgpool.execute("""
                         
        CREATE TABLE IF NOT EXISTS journals (
            journal_id UUID NOT NULL,
            journal_name TEXT NOT NULL,
            player_id TEXT
                         
                    
        );
        CREATE UNIQUE INDEX IF NOT EXISTS journals_journal_id_unique ON journals (journal_id);
        CREATE INDEX IF NOT EXISTS journals_journal_name_unique ON journals (journal_name);

                         
    """)

async def upsert_journal(pgpool, journal_id, **kwargs):
    
    params = pgsql_query_params()
    qry_text = f""" 

        INSERT INTO journals
            (journal_id, {', '.join([safe_alphanum(k) for k in kwargs])})
        VALUES
            ({params.append_param(journal_id)}, {', '.join([params.append_param(v) for k,v in kwargs.items() ])})
        ON CONFLICT (journal_id) DO UPDATE SET
            {', '.join([f'{safe_alphanum(k)} = {params.append_param(v)}' for k,v in kwargs.items()])}
            journal_name = %(journal_name)s,
            player_id = %(player_id)s

        """
    try:

        return await pgpool.execute(qry_text, *params.get_params())
    
    except Exception as e:
        syslog.exception("Exception: %s", e, exc_info=True, stack_info=True)



async def create_events_table(pgpool):
    await pgpool.execute(f"""
        CREATE TABLE IF NOT EXISTS journal_events (
            journal_id UUID NOT NULL,
            event_time TIMESTAMPTZ NOT NULL,
            event_name TEXT NOT NULL
        );
    """)


# EOF ==============================================