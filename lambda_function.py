import json
import sys
import logging
import redis
import pymysql


DB_HOST = "database-1.cluster-cy8v87ey4i9x.us-east-1.rds.amazonaws.com"
DB_USER = "admin"
DB_PASS = "Need4speed"
DB_NAME = "db"
DB_TABLE = "heroes"
REDIS_URL = "redis://redis4rdscluster.hxxj2s.ng.0001.use1.cache.amazonaws.com:6379"

TTL = 10

class DB:
    def __init__(self, **params):
        params.setdefault("charset", "utf8mb4")
        params.setdefault("cursorclass", pymysql.cursors.DictCursor)

        self.mysql = pymysql.connect(**params)

    def query(self, sql):
        with self.mysql.cursor() as cursor:
            cursor.execute(sql)
            return cursor.fetchall()

    def get_idx(self, table_name):
        with self.mysql.cursor() as cursor:
            cursor.execute(f"SELECT MAX(id) as id FROM {table_name}")
            idx = str((int)(cursor.fetchone()['id']) + 1)
            print('using index ', idx)
            return idx

    def insert(self, idx, data):
        with self.mysql.cursor() as cursor:
            hero = data["hero"]
            power = data["power"]
            name = data["name"]
            xp = data["xp"]
            color = data["color"]
            
            sql = f"INSERT INTO heroes (`id`, `hero`, `power`, `name`, `xp`, `color`) VALUES ('{idx}', '{hero}', '{power}', '{name}', '{xp}', '{color}')"

            cursor.execute(sql)
            self.mysql.commit()

def read(use_cache, indices, Database, Cache):
    if use_cache:
        print("Using cache to read")
        """Retrieve records from the cache, or else from the database.""" 
        heroes = []
        for indice in indices:
            print("reading cache index", indice)
            #print("Whu")
            res = Cache.get(indice)
            #print("Wut")
            if res:
                heroes.append(json.loads(res))
            else:
                row = Database.query(f"SELECT  `id`, `hero`, `power`, `name`, `xp`, `color` FROM {DB_TABLE} where id='{indice}'")
                hero = row[0]
                hero["id"] = int(hero["id"])
                hero["xp"] = int(hero["xp"])
                Cache.setex(indice, TTL, json.dumps(hero))
                heroes.append(hero)
        return heroes
    else:
        
        heroes = []
        for indice in indices:
            print('reading cache index', indice)
            row = Database.query(f"SELECT  `id`, `hero`, `power`, `name`, `xp`, `color` FROM {DB_TABLE} where id='{indice}'")
            hero = row[0]
            hero["id"] = int(hero["id"])
            hero["xp"] = int(hero["xp"])
            Cache.setex(indice, TTL, json.dumps(hero))
            heroes.append(hero)
        return heroes

    
    
def write(use_cache, sqls, Database, Cache):
    if use_cache:
        # write through strategy
        for data in sqls:
            idx = Database.get_idx(DB_TABLE)
            Database.insert(idx, data)
            hero = data
            hero["id"] = idx
            print("Writing data", data)
            Cache.setex(idx, TTL, json.dumps(hero))
    else:
        for data in sqls:
            idx = Database.get_idx(DB_TABLE)
            print("Writing  w/o cache data ", data)
            Database.insert(idx, data)



def lambda_handler(event, context):
    
    USE_CACHE = (event['USE_CACHE'] == "True")
    REQUEST = event['REQUEST']
    
    # initialize database and cache
    try:
        Database = DB(host=DB_HOST, user=DB_USER, password=DB_PASS, db=DB_NAME)
    except pymysql.MySQLError as e:
        print("ERROR: Unexpected error: Could not connect to MySQL instance.")
        print(e)
        sys.exit()
        
    Cache = redis.Redis.from_url(REDIS_URL)
    result = []
    if REQUEST == "read":
        # event["SQLS"] should be a list of integers
        print("Reading:", event["SQLS"])
        result = read(USE_CACHE, event["SQLS"], Database, Cache)
        print("rd result is", json.dumps(result))
    elif REQUEST == "write":
        # event["SQLS"] should be a list of jsons
        print("Writing:", event["SQLS"])
        write(USE_CACHE, event["SQLS"], Database, Cache)
        result = "write success"
    
    
    print("Finally returning", json.dumps(result))
    return {
        'statusCode': 200,
        'body': result
    }
