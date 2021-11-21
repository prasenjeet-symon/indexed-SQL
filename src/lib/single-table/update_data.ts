import { openDB } from "indexed-pdb";
import moment from "moment";
import {
  convert_to_number,
  find_bracket_close_index,
  indexed_sql_config_database,
  indexed_sql_config_schema_name,
  is_number,
  is_pure_sql_string,
  retrive_next_database_version,
  table_schema_unique_string,
} from "../utils";
import { single_table_where_clause } from "../where_clause";
import { check_columns_compatibility } from "./insert_data";
import { make_select_literals } from "./select_clause";

export function extract_table_info_update(update_literals: string[]) {
  // find the index of SET keyword
  // table and database info is at found_index - 1
  const index_of_SET = update_literals.findIndex(
    (val) => val.toUpperCase() === "SET"
  );
  let table_name: string;
  let database_name: any;
  table_name = update_literals[index_of_SET - 1];

  const splited_value = table_name.split(".");
  if (splited_value.length === 1) {
    // no database name provided
    table_name = splited_value[0].replace(new RegExp("`", "ig"), "");
  } else if (splited_value.length === 2) {
    database_name = splited_value[0].replace(new RegExp("`", "ig"), "");
    table_name = splited_value[1].replace(new RegExp("`", "ig"), "");
  }

  return {
    database_name,
    table_name,
  };
}

// update statement of SQL
export function extract_update_column_value_pair(update_literals: string[]) {
  // we assume that incoming sql statement is true
  let index_of_SET = -1;
  let index_of_WHERE = -1;

  for (let index = 0; index < update_literals.length; index++) {
    const literal = update_literals[index].toUpperCase();

    if (literal === "(") {
      // find the closing bracket
      const closing_bracket = find_bracket_close_index(
        update_literals,
        "right",
        index + 1
      );
      if (closing_bracket > -1) {
        index = closing_bracket;
        continue;
      } else {
        throw new Error("missing closing bracket");
      }
    }

    if (literal === "SET") {
      index_of_SET = index;
    }

    if (literal === "WHERE") {
      index_of_WHERE = index;
      break;
    }
  }

  let columns_key_val_info: string[] = [];

  if (index_of_SET > -1) {
    if (index_of_WHERE > -1) {
      // there is where clause
      columns_key_val_info = update_literals.slice(
        index_of_SET + 1,
        index_of_WHERE
      );
    } else {
      // no where clause
      columns_key_val_info = update_literals.slice(index_of_SET + 1);
    }
  } else {
    throw new Error("missing SET clause on update statement.");
  }

  const update_rows: string[][] = [];

  let found_comma_index = 0;
  for (let index = 0; index < columns_key_val_info.length; index++) {
    const literal = columns_key_val_info[index];
    if (literal === ",") {
      const sliced_row = columns_key_val_info.slice(found_comma_index, index);
      update_rows.push(sliced_row);
      found_comma_index = index + 1;
    }

    if (index === columns_key_val_info.length - 1) {
      // last index
      const sliced_row = columns_key_val_info.slice(found_comma_index);
      update_rows.push(sliced_row);
    }
  }

  let column_names_list: string[] = [];
  update_rows.forEach((p) => column_names_list.push(p[0]));

  let column_values: any[] = [];
  update_rows.forEach((p) =>
    column_values.push(convert_to_number(p.slice(2).join(" ").trim()))
  );

  return {
    column_names: column_names_list,
    column_values: column_values,
  };
}

export function extract_update_where_statement(update_literals: string[]) {
  let index_of_WHERE = -1;

  // find the index of where clause
  for (let index = 0; index < update_literals.length; index++) {
    const literal = update_literals[index].toUpperCase();

    if (literal === "(") {
      // find the closing bracket
      const closing_bracket = find_bracket_close_index(
        update_literals,
        "right",
        index + 1
      );
      if (closing_bracket > -1) {
        index = closing_bracket;
        continue;
      } else {
        throw new Error("missing closing bracket");
      }
    }

    if (literal === "WHERE") {
      index_of_WHERE = index;
      break;
    }
  }

  let where_statement: any = null;

  if (index_of_WHERE > -1) {
    // found the where statement
    where_statement = update_literals.slice(index_of_WHERE + 1);
  } else {
    where_statement = null;
  }

  return where_statement;
}

export async function update_data(
  update_statement: string,
  default_database: string
) {
  // make  update literals
  const update_literals = make_select_literals(update_statement);
  const database_info = extract_table_info_update(update_literals);
  database_info.database_name = database_info.database_name
    ? database_info.database_name
    : default_database;

  const column_name_value_list =
    extract_update_column_value_pair(update_literals);
  // remove  any period from the columns name
  column_name_value_list.column_names = column_name_value_list.column_names.map(
    (val) => {
      const splited_value = val.split(".");
      return splited_value[splited_value.length - 1];
    }
  );

  const where_statement = extract_update_where_statement(update_literals);
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

  // ectract the schema config
  const sql_schema_config_database = await openDB(
    indexed_sql_config_database,
    1
  );
  const sql_schema_config_transaction = sql_schema_config_database.transaction(
    indexed_sql_config_schema_name,
    "readonly"
  );
  const schema_config = await sql_schema_config_transaction
    .objectStore(indexed_sql_config_schema_name)
    .get(
      table_schema_unique_string(
        database_info.database_name,
        database_info.table_name
      )
    );
  await sql_schema_config_transaction.is_complete();
  sql_schema_config_database.close();

  const checked_data = check_columns_compatibility(
    column_name_value_list.column_values,
    column_name_value_list.column_names,
    schema_config
  );

  const final_checked_data: any = {};
  Object.keys(checked_data).forEach((val, i) => {
    if (column_name_value_list.column_names.includes(val)) {
      final_checked_data[val] = checked_data[val];
    }
  });

  // check if the there is any column with timestamp
  const timestamp_columns = (schema_config as any[]).filter(
    (val: any) => val.column_datatype.first_part.toUpperCase() === "TIMESTAMP"
  );
  timestamp_columns.forEach(
    (val, i) =>
      (final_checked_data[val.column_name] = moment(new Date()).format(
        "YYYY-MM-DD HH:mm:ss"
      ))
  );

  // open cursor and modify
  const version_db = await retrive_next_database_version(
    database_info.database_name
  );

  const database_update = await openDB(
    database_info.database_name,
    version_db.current_version
  );
  const database_update_tnx = database_update.transaction(
    database_info.table_name,
    "readwrite"
  );
  const database_update_objectstore = database_update_tnx.objectStore(
    database_info.table_name
  );

  await database_update_objectstore
    .openCursor()
    .then(function update_item(cursor) {
      if (!cursor) {
        return;
      }

      const original_object = cursor.value;
      // check is this target item
      if (final_where_statement) {
        single_table_where_clause(original_object, final_where_statement).then(
          (data) => {
            if (data) {
              // this is the item
              const final_object: any = {};
              Object.keys(original_object).forEach((val, i) => {
                if (final_checked_data.hasOwnProperty(val)) {
                  final_object[val] = final_checked_data[val];
                } else {
                  final_object[val] = original_object[val];
                }
              });
              cursor.update(final_object).then((data) => {
                cursor.continue().then(update_item);
              });
            } else {
              // move the cursor
              cursor.continue().then(update_item);
            }
          }
        );
      } else {
        // no where statement
        // update all
        // this is the item
        const final_object: any = {};
        Object.keys(original_object).forEach((val, i) => {
          if (final_checked_data.hasOwnProperty(val)) {
            final_object[val] = final_checked_data[val];
          } else {
            final_object[val] = original_object[val];
          }
        });
        cursor.update(final_object).then((data) => {
          cursor.continue().then(update_item);
        });
      }
    });

  await database_update_tnx.is_complete();
  database_update.close();

  return "OK";
}

export async function parse_update_data(
  update_statement: string,
  default_database: string
) {
  // make  update literals
  const update_literals = make_select_literals(update_statement);
  const database_info = extract_table_info_update(update_literals);
  database_info.database_name = database_info.database_name
    ? database_info.database_name
    : default_database;

  const column_name_value_list =
    extract_update_column_value_pair(update_literals);
  // remove  any period from the columns name
  column_name_value_list.column_names = column_name_value_list.column_names.map(
    (val) => {
      const splited_value = val.split(".");
      return splited_value[splited_value.length - 1];
    }
  );

  const where_statement = extract_update_where_statement(update_literals);
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

  // ectract the schema config
  const sql_schema_config_database = await openDB(
    indexed_sql_config_database,
    1
  );
  const sql_schema_config_transaction = sql_schema_config_database.transaction(
    indexed_sql_config_schema_name,
    "readonly"
  );
  const schema_config = await sql_schema_config_transaction
    .objectStore(indexed_sql_config_schema_name)
    .get(
      table_schema_unique_string(
        database_info.database_name,
        database_info.table_name
      )
    );
  await sql_schema_config_transaction.is_complete();
  sql_schema_config_database.close();

  const checked_data = check_columns_compatibility(
    column_name_value_list.column_values,
    column_name_value_list.column_names,
    schema_config
  );

  const final_checked_data: any = {};
  Object.keys(checked_data).forEach((val, i) => {
    if (column_name_value_list.column_names.includes(val)) {
      final_checked_data[val] = checked_data[val];
    }
  });

  // check if the there is any column with timestamp
  const timestamp_columns = (schema_config as any[]).filter(
    (val: any) => val.column_datatype.first_part.toUpperCase() === "TIMESTAMP"
  );
  timestamp_columns.forEach(
    (val, i) =>
      (final_checked_data[val.column_name] = moment(new Date()).format(
        "YYYY-MM-DD HH:mm:ss"
      ))
  );

  // open cursor and modify
  const version_db = await retrive_next_database_version(
    database_info.database_name
  );

  const database_update = await openDB(
    database_info.database_name,
    version_db.current_version
  );
  const database_update_tnx = database_update.transaction(
    database_info.table_name,
    "readonly"
  );
  const database_update_objectstore = database_update_tnx.objectStore(
    database_info.table_name
  );

  const update_data: any[] = [];

  await database_update_objectstore
    .openCursor()
    .then(function update_item(cursor) {
      if (!cursor) {
        return;
      }

      const original_object = cursor.value;
      // check is this target item
      if (final_where_statement) {
        single_table_where_clause(original_object, final_where_statement).then(
          (data) => {
            if (data) {
              // this is the item
              const final_object: any = {};
              Object.keys(original_object).forEach((val, i) => {
                if (final_checked_data.hasOwnProperty(val)) {
                  final_object[val] = final_checked_data[val];
                } else {
                  final_object[val] = original_object[val];
                }
              });
              update_data.push(final_object);
              cursor.continue().then(update_item);
            } else {
              // move the cursor
              cursor.continue().then(update_item);
            }
          }
        );
      } else {
        // no where statement
        // update all
        // this is the item
        const final_object: any = {};
        Object.keys(original_object).forEach((val, i) => {
          if (final_checked_data.hasOwnProperty(val)) {
            final_object[val] = final_checked_data[val];
          } else {
            final_object[val] = original_object[val];
          }
        });
        update_data.push(final_object);
        cursor.continue().then(update_item);
      }
    });

  await database_update_tnx.is_complete();
  database_update.close();

  return update_data;
}
