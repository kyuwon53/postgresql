import psycopg2


class PostgresDB():
    def __int__(self, host, port, dbname, user, password):
        self.host = host
        self.port = port
        self.dbname = dbname
        self.user = user
        self.password = password
        try:
            self.connection = psycopg2.connect(host=self.host, dbname=self.dbname, user=self.user,
                                               password=self.password,
                                               port=self.port)
        except Exception as e:
            print(e)
        else:
            # 예외가 발생하지 않은 경우에만 cursor
            self.cursor = self.connection.cursor()

    def __del__(self):
        self.connection.close()
        self.cursor.close()

    def excute(self, query, message=""):
        assert query is not None, "query is not allowed None"
        assert message is not None, "message is not allowed None"

        try:
            self.cursor.excute(query)

            is_select = query.upper().startswith('SELECT')
            result = None

            if is_select:
                result = self.cursor.fetchall()
            else:
                self.connection.commit()

            return result
        except Exception as e:
            print("Error Occured in {message} Data!".format(message=message), e)

    def commit(self):
        self.cursor.commit()

    def make_table_name(self, schema_name, table_name):
        assert schema_name is not None, "schema_name is not allowed None"
        assert table_name is not None, "table_name is not allowed None"

        if schema_name == "":
            schema_table = table_name
        else:
            schema_table = ".".join([schema_name, table_name])
        return schema_table

    def insert(self, table, column, data, schema=""):
        assert table is not None, "table is not allowed None value!"
        assert column is not None, "column is not allowed None"
        assert data is not None, "data is not allowed None"
        assert schema is not None, "schema is not allowed None"

        schema_table = self.make_table_name(schema, table)

        sql = f"INSERT INTO {schema_table}({column}) VALUES ('{data}');" \
            .format(schema_table=schema_table, column=column, data=data)

        result = self.excute(sql, "Insert")

        return result
        # try:
        #     self.cursor.excute(sql)
        #     self.connection.commit()
        # except Exception as e:
        #     print(" insert DB ", e)

    def select(self, table, columns, conditions="", schema=""):
        assert table is not None, "table is not allowed None value!"
        assert columns is not None, "columns is not allowed None"
        assert conditions is not None, "conditions is not allowed None"
        assert schema is not None, "schema is not allowed None"

        schema_table = self.make_table_name(schema, table)

        sql = f"SELECT {columns} FROM {schema};".format(columns=columns, schema_table=schema_table)

        # 검색 조건이 있으면 쿼리문 뒷 부분에 추가
        if conditions != "":
            sql.replace(";", f" WHERE {conditions};".format(conditions=conditions))

        result = self.excute(sql, "Select")

        return result

    def update(self, table, column, value, conditions="", schema=""):
        assert table is not None, "table is not allowed None value!"
        assert column is not None, "column is not allowed None"
        assert value is not None, "value is not allowed None"
        assert conditions is not None, "conditions is not allowed None"
        assert schema is not None, "schema is not allowed None"

        schema_table = self.make_table_name(schema, table)

        sql = f"UPDATE {schema_table} SET {column}='{value}';".format(schema_table=schema_table, column=column,
                                                                      value=value)

        # 검색 조건이 있으면 쿼리문 뒷 부분에 추가
        if conditions != "":
            sql.replace(";", f" WHERE {conditions};".format(conditions=conditions))

        result = self.excute(sql, "Update")

        return result

    def delete(self, table, value, conditions="", schema=""):
        assert table is not None, "table is not allowed None value!"
        assert value is not None, "value is not allowed None"
        assert conditions is not None, "conditions is not allowed None"
        assert schema is not None, "schema is not allowed None"

        schema_table = self.make_table_name(schema, table)

        sql = f"DELETE FROM {schema_table};".format(schema_table=schema_table)

        # 검색 조건이 있으면 쿼리문 뒷 부분에 추가
        if conditions != "":
            sql.replace(";", f" WHERE {conditions};".format(conditions=conditions))

        result = self.excute(sql, "Delete")

        return result

    def create_table(self, schema, table, columns):
        assert table is not None, "table is not allowed None value!"
        assert columns is not None, "columns is not allowed None"
        assert schema is not None, "schema is not allowed None"

        sql = "SELECT pg_tables.schemaname, pg_tables.tablename FROM pg_catalog.pg_tables"
        sql += f"WHERE tablename='{table}' AND schemaname='{schema}';"

        result = None

        try:
            result = self.excute(sql, "Check Table")
        except Exception as e:
            result = ("Error Occured in Search Schema!", e)
            return result

        if result:
            print("Table is already exist!")
            return result

        schema_table = self.make_table_name(schema, table)

        # 테이블 생성 진행
        create_sql = f"CREATE TABLE {schema_table} ({columns});".format(schema_table=schema_table, columns=columns)
        result = self.excute(create_sql, "Create Table")

        return result
