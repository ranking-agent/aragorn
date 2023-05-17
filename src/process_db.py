import os
import sqlite3
from datetime import datetime

dir_path: str = os.environ.get("PROCESS_DB_PATH") or os.path.dirname(os.path.realpath(__file__))
process_filename = os.path.join(dir_path, "process.db")


def init_db():
    if os.path.exists(process_filename):
        os.remove(process_filename)

    conn = sqlite3.connect(process_filename)
    c = conn.cursor()
    c.execute("CREATE TABLE process (job_id TEXT, status_time timestamp, status_msg TEXT, status_code INT)")
    c.execute("CREATE INDEX job_id_index ON process(job_id);")
    conn.commit()
    conn.close()

def add_item(job_id, status_msg, status_code):
    conn = sqlite3.connect(process_filename)
    status_time = datetime.now()
    c = conn.cursor()
    c.execute("INSERT INTO process (job_id, status_time, status_msg, status_code) VALUES (?, ?, ?, ?)",
                    (job_id, status_time, status_msg, status_code))
    conn.commit()
    conn.close()

def get_status(job_id):
    conn = sqlite3.connect(process_filename)
    c = conn.cursor()
    c.execute("SELECT job_id, status_time, status_msg, status_code FROM process "
              "WHERE job_id = ? ORDER BY status_time ASC", (job_id,))
    rows = c.fetchall()
    conn.close()
    names = ["job_id", "status_time", "status_msg", "status_code"]
    return [{name: item for name,item in zip(names,row)} for row in rows]

if __name__ == "__main__":
    init_db()
    print("Database initialized.")

