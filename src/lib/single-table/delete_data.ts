import { openDB } from "indexed-pdb";
import {
  is_number,
  is_pure_sql_string,
  retrive_next_database_version,
} from "../utils";
import { single_table_where_clause } from "../where_clause";
import { make_select_literals } from "./select_clause";
import { extract_update_where_statement } from "./update_data";
/*
We need to parse the database name and table name as well as where statement.
There is already the where statement parser for the update clause use that to parse where statement
for this.

*/

export function parse_delete_database_table_info(delete_literals: string[]) {
  let database_name: any;
  let table_name: any;

  // find index of from keyword after that is database.tablename
  const index_of_from_keyword = delete_literals.findIndex(
    (val) => val.toUpperCase() === "FROM"
  );
  if (index_of_from_keyword > -1) {
    // found the from keyword
    const database_table = delete_literals[index_of_from_keyword + 1];
    const splited_value = database_table.trim().split(".");
    if (splited_value.length === 2) {
      // there is database name
      database_name = splited_value[0].trim();
      table_name = splited_value[1].trim();
    } else if (splited_value.length === 1) {
      // there is only table name
      database_name = null;
      table_name = splited_value[0].trim();
    }
  } else {
    // throw err missing from keyword in the statement
    throw new Error("missing from keyword in the delete statement.");
  }

  return { database_name, table_name };
}

export async function delete_data(
  delete_statement: string,
  default_database: string
) {
  const delete_literals = make_select_literals(delete_statement);
  // parse the table and database name
  const database_info = parse_delete_database_table_info(delete_literals);
  database_info.database_name = database_info.database_name
    ? database_info.database_name
    : default_database;

  const where_statement = extract_update_where_statement(delete_literals);
  // remove any period in column name and use last one
  const final_where_statement = where_statement
    ? where_statement
        .map((m: string) => {
          const literal = m as string;
          if (
            literal.includes(".") &&
            !is_number(literal) &&
            !is_pure_sql_string(literal)
          ) {
            const splited_value = literal.split(".");
            return splited_value[splited_value.length - 1];
          } else {
            return m;
          }
        })
        .join(" ")
        .trim()
    : null;

  const version_db = await retrive_next_database_version(
    database_info.database_name
  );

  // open the cursor and delete the row
  const database_delete = await openDB(
    database_info.database_name,
    version_db.current_version
  );
  const database_delete_tnx = database_delete.transaction(
    database_info.table_name,
    "readwrite"
  );
  const database_delete_objectstore = database_delete_tnx.objectStore(
    database_info.table_name
  );

  await database_delete_objectstore
    .openCursor()
    .then(function delete_item(cursor) {
      if (!cursor) {
        return;
      }

      if (final_where_statement) {
        const original_object = cursor.value;
        single_table_where_clause(original_object, final_where_statement).then(
          (data) => {
            if (data) {
              cursor.delete().then((data) => {
                cursor.continue().then(delete_item);
              });
            } else {
              // just move to next item
              cursor.continue().then(delete_item);
            }
          }
        );
      } else {
        // just delete every row
        cursor.delete().then((data) => {
          cursor.continue().then(delete_item);
        });
      }
    });

  return "OK";
}

export async function parse_delete_data(
  delete_statement: string,
  default_database: string
) {
  const delete_literals = make_select_literals(delete_statement);
  // parse the table and database name
  const database_info = parse_delete_database_table_info(delete_literals);
  database_info.database_name = database_info.database_name
    ? database_info.database_name
    : default_database;

  const where_statement = extract_update_where_statement(delete_literals);
  // remove any period in column name and use last one
  const final_where_statement = where_statement
    ? where_statement
        .map((m: string) => {
          const literal = m as string;
          if (
            literal.includes(".") &&
            !is_number(literal) &&
            !is_pure_sql_string(literal)
          ) {
            const splited_value = literal.split(".");
            return splited_value[splited_value.length - 1];
          } else {
            return m;
          }
        })
        .join(" ")
        .trim()
    : null;

  return final_where_statement;
}
