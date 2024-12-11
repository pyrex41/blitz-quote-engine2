import json
from datetime import datetime
from typing import Any, List, Tuple

class DBOperationsLogger:
    def __init__(self, log_file_path: str):
        self.log_file_path = log_file_path

    def log_operation(self, operation: str, query: str, params: Any = None):
        log_entry = {
            'timestamp': datetime.now().isoformat(),
            'operation': operation,
            'query': query,
            'params': params
        }
        with open(self.log_file_path, 'a') as f:
            f.write(json.dumps(log_entry) + '\n')

    def replay_operations(self, db_connection):
        cursor = db_connection.cursor()
        with open(self.log_file_path, 'r') as f:
            for line in f:
                operation = json.loads(line)
                if operation['params']:
                    cursor.execute(operation['query'], operation['params'])
                else:
                    cursor.execute(operation['query'])
        db_connection.commit()