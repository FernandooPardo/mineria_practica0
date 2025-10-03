import jaydebeapi
import os

class Conexion:
    def __init__(self, host='localhost', database='IBEX35', user='root', password='changeme'):
        self._host = host
        self._database = database
        self._user = user
        self._password = password
        self.conexion = self.createConnection()

    def createConnection(self):
        try:
            jdbc_driver = "com.mysql.cj.jdbc.Driver"
            jar_path = os.path.abspath("lib/mysql-connector-j-9.2.0.jar")

            # URL sin base de datos para poder crearla
            url_no_db = f"jdbc:mysql://{self._host}:3306/?serverTimezone=UTC"
            
            # 1. Conectar sin DB
            conn_tmp = jaydebeapi.connect(
                jdbc_driver,
                url_no_db,
                [self._user, self._password],
                jar_path
            )
            curs_tmp = conn_tmp.cursor()
            curs_tmp.execute(f"CREATE DATABASE IF NOT EXISTS {self._database}")
            curs_tmp.close()
            conn_tmp.close()

            # 2. Conectar ya a la DB
            url = f"jdbc:mysql://{self._host}:3306/{self._database}?serverTimezone=UTC"
            conn = jaydebeapi.connect(
                jdbc_driver,
                url,
                [self._user, self._password],
                jar_path
            )
            print(f"✅ Conexión establecida a la base de datos {self._database}.")
            return conn

        except Exception as e:
            print("❌ Error creando conexión:", e)
            return None

    def getCursor(self):
        if self.conexion is None:
            self.conexion = self.createConnection()
        return self.conexion.cursor()

    def closeConnection(self):
        try:
            if self.conexion:
                self.conexion.close()
                self.conexion = None
        except Exception as e:
            print("❌ Error cerrando conexión:", e)

# Prueba
if __name__ == "__main__":
    Conexion()
