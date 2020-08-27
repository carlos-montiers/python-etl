#!/usr/bin/env python3
# coding: utf-8

# Autor: Carlos Montiers A.

# Ejemplo de tomar datos desde una base de datos no relacional MongoDB
# y usarlos para realizar actualización de datos en una base de datos MariaDB
# con campos de tipo JSON
# Inserta o reemplaza ingredientGroup en el extended data de Recipes

import os
from dotenv import load_dotenv
import pymongo
import mysql.connector
from mysql.connector import errorcode
from mysql.connector.cursor import MySQLCursor
from datetime import datetime

# para desarrollo local: carga archivo .env
# load_dotenv()

mariadb_config = {
    "host": os.getenv("mariadb_host", "127.0.0.1"),
    "port": os.getenv("mariadb_port", "3306"),
    "user": os.getenv("mariadb_user"),
    "password": os.getenv("mariadb_password"),
    "database": os.getenv("mariadb_database"),
    "charset": "utf8",
    "autocommit": False
}

mongodb_config = {
    "host": os.getenv("mongodb_host", "127.0.0.1"),
    "port": os.getenv("mongodb_port", "27017"),
    "database": os.getenv("mongodb_database"),
    "collection": os.getenv("mongodb_collection"),
}


def crear_sql_actualizacion(lista_ingredientes):
    # En el update se repite dos veces el set de extended_data
    # porque aunque la documentación de JSON_SET indica que se puede repetir path val
    # en la práctica arrojaba errores si en el segundo path val se usaban tildes
    # Referencia: MDEV-16750 JSON_SET mishandles unicode every second pair of arguments.
    # http://lists.askmonty.org/pipermail/commits/2018-August/012775.html

    mariadb_update_stmt = ""
    if len(lista_ingredientes) > 0:
        mariadb_update_stmt += """UPDATE recipes rec SET
    """
        mariadb_update_stmt += """
    rec.extended_data = CASE
    """
        for ingredient in lista_ingredientes:
            ingredient_id = str(ingredient["id"])
            ingredient_description = ingredient["description"]
            mariadb_update_stmt += "WHEN CAST(IFNULL(JSON_UNQUOTE(JSON_EXTRACT(rec.extended_data, '$.ingredientId')), 0) AS INT) = "
            mariadb_update_stmt += ingredient_id + " "
            mariadb_update_stmt += "THEN JSON_SET(rec.extended_data"
            mariadb_update_stmt += ", '$.ingredientDescription', '" + ingredient_description + "'"
            mariadb_update_stmt += ")"
            mariadb_update_stmt += "\n"
        mariadb_update_stmt += """ELSE rec.extended_data END
    """

        mariadb_update_stmt += """
    , rec.extended_data = CASE
    """
        for ingredient in lista_ingredientes:
            ingredient_id = str(ingredient["id"])
            ingredient_group = ingredient["ingredientGroup"]
            mariadb_update_stmt += "WHEN CAST(IFNULL(JSON_UNQUOTE(JSON_EXTRACT(rec.extended_data, '$.ingredientId')), 0) AS INT) = "
            mariadb_update_stmt += ingredient_id + " "
            mariadb_update_stmt += "THEN JSON_SET(rec.extended_data"
            mariadb_update_stmt += ", '$.ingredientGroup', '" + ingredient_group + "'"
            mariadb_update_stmt += ")"
            mariadb_update_stmt += "\n"
        mariadb_update_stmt += """ELSE rec.extended_data END
    """

        mariadb_update_stmt += "WHERE rect_id = 1;"

    return mariadb_update_stmt


try:

    print(str(datetime.now()) + " INICIO")

    mariadb_connection = mysql.connector.connect(**mariadb_config)
    mongo_client = pymongo.MongoClient(mongodb_config["host"], int(mongodb_config["port"]))
    mongo_database = mongo_client[mongodb_config["database"]]
    mongo_collection = mongo_database[mongodb_config["collection"]]

    mongo_query = {"name": "ingredient"}
    term_ingredient = mongo_collection.find_one(mongo_query)
    lista_ingredientes = term_ingredient["value"]

    if len(lista_ingredientes) == 0:
        print("Lista de ingredientes sin items")
    else:
        total_filas_actualizadas = 0
        print("Construyendo sqls de actualización ...")

        mariadb_update_stmt = crear_sql_actualizacion(lista_ingredientes)

        mariadb_update_cursor = mariadb_connection.cursor()
        print("Actualizando ...")
        mariadb_update_cursor.execute(mariadb_update_stmt)
        total_filas_actualizadas = mariadb_update_cursor.rowcount
        mariadb_connection.commit()

        print("Total de filas actualizadas", total_filas_actualizadas)

except mysql.connector.Error as err:
    print(err)
else:
    mariadb_connection.close()
    mongo_client.close()
finally:
    print(str(datetime.now()) + " FIN")
