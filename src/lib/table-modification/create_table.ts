/**
 * Currently we are assuming that no subquery exit in process of table creation
 */

import { openDB } from "indexed-pdb";
import { make_select_literals } from "../single-table/select_clause";
import {
  find_bracket_close_index,
  indexed_sql_config_database,
  indexed_sql_config_schema_name,
  retrive_next_database_version,
  table_schema_unique_string,
  upgrade_database_version,
} from "../utils";

export function extract_table_name(create_literals: string[]) {
  // if there is AS find the first index of the AS
  // if there is no AS then find the first index of (
  // table name do exit at the index of (found index - 1)
  // note that table name can't contain space as well as any special_character other _
  const index_of_as_literal = create_literals.findIndex(
    (val, i) => val.toUpperCase() === "AS"
  );
  const index_of_starting_bracket = create_literals.findIndex(
    (val, i) => val.toUpperCase() === "("
  );

  let table_name: string = "";
  let database_name: string | null = null;

  if (index_of_as_literal > -1) {
    // AS literal exit
    // no need to check the starting bracket
    // note that table name may contain database_name and table name separated by .
    table_name = create_literals[index_of_as_literal - 1];
  } else if (index_of_starting_bracket > -1) {
    // no AS is in the create_literals
    table_name = create_literals[index_of_starting_bracket - 1];
  }

  const splited_value = table_name.split(".");
  if (splited_value.length === 1) {
    // no database name provided
    table_name = splited_value[0].replace(new RegExp("`", "ig"), "");
  } else if (splited_value.length === 2) {
    database_name = splited_value[0];
    table_name = splited_value[1].replace(new RegExp("`", "ig"), "");
  }

  return {
    database_name,
    table_name,
  };
}

export function group_bracket_together(create_literals: string[]) {
  const group_now = (
    create_literal: string[],
    cb: (result: string[]) => void
  ) => {
    for (let index = 0; index < create_literal.length; index++) {
      const literal = create_literal[index].toUpperCase();
      if (literal === "(") {
        // try to find the corresponding ending bracket
        const ending_bracket_index = find_bracket_close_index(
          create_literal,
          "right",
          index + 1
        );
        if (ending_bracket_index > -1) {
          // found the closing bracket
          const sliced_value = create_literal.slice(
            index,
            ending_bracket_index + 1
          );
          const joined_string = sliced_value.join(" ");
          create_literal.splice(
            index,
            ending_bracket_index - index + 1,
            joined_string
          );
          break;
        } else {
          throw new Error(`need closing bracket at`);
        }
      }
    }

    if (create_literal.includes("(")) {
      // call this function again
      group_now(create_literal, cb);
    } else {
      // end of the operation
      cb(create_literal);
    }
  };

  return new Promise<string[]>((resolve, reject) => {
    group_now(create_literals, (result) => resolve(result));
  });
}

export async function extract_column_info(create_literals: string[]) {
  create_literals = create_literals;

  let sliced_column_info: string[];

  // extract the column info part
  const initial_bracket_index = create_literals.findIndex(
    (val) => val.toUpperCase() === "("
  );
  if (initial_bracket_index > -1) {
    const matching_closing_bracket = find_bracket_close_index(
      create_literals,
      "right",
      initial_bracket_index + 1
    );
    sliced_column_info = create_literals.slice(
      initial_bracket_index + 1,
      matching_closing_bracket
    );
  } else {
    // thorw error
    throw new Error("need bracket in select stattement");
  }

  // replace the , with _,_
  let start_bracket_index = 0;
  for (let index = 0; index < sliced_column_info.length; index++) {
    const literal = sliced_column_info[index].toUpperCase();

    if (literal === "(") {
      start_bracket_index = index;
    }

    if (literal === ")") {
      start_bracket_index = 0;
    }

    if (literal === ",") {
      // replace with _,_
      if (start_bracket_index > 0) {
        sliced_column_info[index] = "_,_";
      }
    }
  }

  const comma_splited_value = sliced_column_info.join(" ").split(" , ");

  const columns_info: any[] = [];

  for (let index = 0; index < comma_splited_value.length; index++) {
    const val = comma_splited_value[index]; // this is string
    // replace the _,_ with ,
    const replaced_val = val.replace(new RegExp(/_,_/, "ig"), ",");
    // split the by space single
    const splited_value = replaced_val.split(" ");
    // name of the column of the table
    const column_name = splited_value[0];

    if (splited_value[1]) {
      const sliced_value = splited_value.slice(1);
      // group the bracket together
      const grouped_value = await group_bracket_together(sliced_value);

      // get the datatype
      let datatype: {
        first_part: string | null;
        second_part: string[] | null;
      } = {
        first_part: null,
        second_part: null,
      };

      if (grouped_value[1] && grouped_value[1].search(new RegExp(/\(/)) > -1) {
        // there is secod part of the datatype
        const second_part = grouped_value[1]
          .slice(1, grouped_value[1].length - 1)
          .trim()
          .split(" , ");
        datatype.second_part = second_part;
      }

      // column name of the table
      datatype.first_part = grouped_value[0];

      let extract_column_datatype_data: string[] = [];

      if (datatype.first_part && datatype.second_part) {
        extract_column_datatype_data = grouped_value.slice(2);
      } else {
        extract_column_datatype_data = grouped_value.slice(1);
      }

      const column_info = {
        column_name,
        column_datatype: datatype,
        can_null: true,
        is_unsigned: false,
        default_value: "null",
        is_primary_key: false,
        is_auto_increment: false,
      };

      for (
        let index = 0;
        index < extract_column_datatype_data.length;
        index++
      ) {
        const literal = extract_column_datatype_data[index].toUpperCase();
        switch (literal) {
          case "NOT":
            // assume next literal is NULL
            column_info.can_null = false;
            break;

          case "PRIMARY":
            column_info.is_primary_key = true;
            break;

          case "AUTO_INCREMENT":
            column_info.is_auto_increment = true;
            break;

          case "DEFAULT":
            column_info.default_value = extract_column_datatype_data[index + 1];
            break;

          case "UNSIGNED":
            column_info.is_unsigned = true;
            break;
        }
      }

      columns_info.push(JSON.parse(JSON.stringify(column_info)));
    } else {
      throw new Error("error we need datatype of column");
    }
  }

  return columns_info;
}

export async function create_table(
  create_statement: string,
  default_database_name: string = ""
) {
  const create_literals = make_select_literals(create_statement);
  // extract table name and database name
  let { database_name, table_name } = extract_table_name(create_literals);
  database_name = database_name ? database_name : default_database_name;

  // extract the column info
  const column_info = (await extract_column_info(create_literals)) as any[];
  const schema_key = table_schema_unique_string(database_name, table_name);

  // insert the schema
  const config_database = await openDB(indexed_sql_config_database, 1);
  const tnx = config_database.transaction(
    indexed_sql_config_schema_name,
    "readwrite"
  );
  await tnx
    .objectStore(indexed_sql_config_schema_name)
    .add(column_info, schema_key);
  await tnx.is_complete();
  config_database.close();

  const version_db = await retrive_next_database_version(database_name);

  // create the actual table
  const database = await openDB(
    database_name,
    version_db.next_version,
    (upgradeDB) => {
      if (!upgradeDB.objectStoreNames.contains(table_name)) {
        const pk = column_info.find((val) => val.is_primary_key);
        pk
          ? upgradeDB.createObjectStore(table_name, {
              keyPath: pk.column_name,
              autoIncrement: pk.is_auto_increment,
            })
          : upgradeDB.createObjectStore(table_name);
      }
    }
  );

  database.close();
  upgrade_database_version(database_name, version_db.next_version);
  return "OK";
}
