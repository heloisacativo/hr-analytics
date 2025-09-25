from airflow.models import Connection
from airflow.settings import Session
import os

ADB_HOST = os.getenv("ADB_HOST")
ADB_USER = os.getenv("ADB_USER")
ADB_PASSWORD = os.getenv("ADB_PASSWORD")
WALLET_PATH = os.getenv("WALLET_PATH")

CONN_ID = "oracle_autonomous_conn"

def create_or_update_connection():
    session = Session

    existing_conn = session.query(Connection).filter(Connection.conn_id == CONN_ID).first()
    if existing_conn:
        print(f"Connection '{CONN_ID}' já existe. Atualizando...")
        existing_conn.conn_type = "oracle"
        existing_conn.host = ADB_HOST
        existing_conn.login = ADB_USER
        existing_conn.password = ADB_PASSWORD
        existing_conn.extra = f'{{"wallet_location":"{WALLET_PATH}"}}'

    else:
        print(f"Criando conexão '{CONN_ID}'...")
        new_conn = Connection(
            conn_id=CONN_ID,
            conn_type="oracle",
            host=ADB_HOST,
            login=ADB_USER,
            password=ADB_PASSWORD,
            extra=f'{{"wallet_location":"{WALLET_PATH}"}}'
        )
        session.add(new_conn)
    session.commit()
    session.close()
    print(f"Conexão '{CONN_ID}' criada ou atualizada com sucesso.")

if __name__ == "__main__":
    create_or_update_connection()