import { databaseConnectionConfig } from "./main.interface";
import { delete_data, parse_delete_data } from "./single-table/delete_data";
import {
  check_the_data_against_table,
  insert_data,
  parse_insert_data,
} from "./single-table/insert_data";
import { single_select } from "./single-table/select_clause";
import { parse_update_data, update_data } from "./single-table/update_data";
import { create_table } from "./table-modification/create_table";
import {
  convert_sql_data_to_normal_data,
  detect_query_type,
  setup_initial_config_tables,
} from "./utils";
import { single_table_where_clause } from "./where_clause";

export class ISQLConnection {
  constructor(private config: databaseConnectionConfig) {
    if (!config.database_name) {
      throw new Error("missing database name");
    }
  }

  public async open() {
    // set up the initial config tables
    await setup_initial_config_tables();
    // return the object to query
    return new ISQLQuery(this.config);
  }
}

export class ISQLQuery {
  constructor(private config: databaseConnectionConfig) {}

  public async query(SQLQuery: string) {
    const query_list: string[] = SQLQuery.trim()
      .split(";")
      .filter((p) => p !== "")
      .map((p) => p.trim());

    if (query_list.length !== 0) {
      if (this.config.multi_query) {
        const result = await this.manage_query(query_list);
        return result;
      } else {
        if (query_list.length > 1) {
          throw new Error(" multi query is not enabled ");
        } else {
          // query is single
          const result = await this.manage_query(query_list);
          return result[0];
        }
      }
    } else {
      throw new Error("no query is provided");
    }
  }

  private async manage_query(query_list: string[]) {
    // detect the query and add them as promise in array
    const query_promises = [];

    for (let query of query_list) {
      const query_type = await detect_query_type(query);

      switch (query_type) {
        case "CREATE":
          const create_promise = () =>
            new Promise<any>((resolve, reject) => {
              create_table(query, this.config.database_name)
                .then((data) => {
                  resolve(data);
                })
                .catch((err) => reject(err));
            });
          query_promises.push(create_promise);
          break;

        case "DELETE":
          const delete_promise = () =>
            new Promise<any>((resolve, reject) => {
              delete_data(query, this.config.database_name)
                .then((data) => {
                  resolve(data);
                })
                .catch((err) => reject(err));
            });
          query_promises.push(delete_promise);
          break;

        case "INSERT":
          const insert_promise = () =>
            new Promise<any>((resolve, reject) => {
              insert_data(query, this.config.database_name)
                .then((data) => {
                  resolve(data);
                })
                .catch((err) => reject(err));
            });
          query_promises.push(insert_promise);
          break;

        case "SELECT":
          const select_promise = () =>
            new Promise<any>((resolve, reject) => {
              single_select(query, this.config.database_name)
                .then((data) => {
                  resolve(convert_sql_data_to_normal_data(data));
                })
                .catch((err) => reject(err));
            });
          query_promises.push(select_promise);
          break;

        case "UPDATE":
          const update_promise = () =>
            new Promise<any>((resolve, reject) => {
              update_data(query, this.config.database_name)
                .then((data) => {
                  resolve(data);
                })
                .catch((err) => reject(err));
            });
          query_promises.push(update_promise);
          break;
      }
    }

    const result = [];
    for (let promise_fun of query_promises) {
      result.push(await promise_fun());
    }
    return result;
  }

  public async parse_insert_statement(insert_statement: string) {
    const parsed_data = await parse_insert_data(
      insert_statement,
      this.config.database_name
    );
    return parsed_data;
  }

  /**
   *
   * @param data : object to ckeck against condition
   * @param where_statement : SQL consition whithout WHERE clause and ;
   */
  public async check_data_with_where(data: any, where_statement: string) {
    return await single_table_where_clause(data, where_statement);
  }

  public async parse_update_statement(update_statement: string) {
    const result = await parse_update_data(
      update_statement,
      this.config.database_name
    );
    return result;
  }

  public async parse_delete_statement(delete_statement: string) {
    const where_string = await parse_delete_data(
      delete_statement,
      this.config.database_name
    );
    return where_string;
  }

  public async check_the_data_against_table(table_name: string, data: any[]) {
    if (data.length === 0) {
      return [];
    } else {
      const result = await check_the_data_against_table(
        this.config.database_name,
        table_name,
        data
      );
      return result;
    }
  }
}
