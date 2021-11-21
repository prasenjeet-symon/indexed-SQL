import { BigNumber } from "bignumber.js";
import { openDB } from "indexed-pdb";
import moment from "moment";
import { table_column_info } from "../main.interface";
import { extract_table_name } from "../table-modification/create_table";
import {
  convert_sql_string_to_string,
  convert_the_normal_value_to_sql_value,
  convert_to_number,
  find_bracket_close_index,
  generalTrim,
  indexed_sql_config_database,
  indexed_sql_config_schema_name,
  is_pure_sql_string,
  is_string_date,
  is_time_string,
  is_year_string,
  retrive_next_database_version,
  table_schema_unique_string,
} from "../utils";
import { make_select_literals } from "./select_clause";

export function extract_insertion_data_column_info(insert_literals: string[]) {
  // we assume that there is no () appear in the table name as well as sql reserved keyword as well as column_name
  const value_keyword_index = insert_literals.findIndex(
    (val) => val.toUpperCase() === "VALUES"
  );
  if (value_keyword_index > -1) {
    // we found the VALUES keyword
    // slice the part from 0 to this index
    const sliced_part = insert_literals.slice(0, value_keyword_index);
    // find the (  index and also closing ) index
    const bracket_index = sliced_part.findIndex(
      (val) => val.toUpperCase() === "("
    );
    if (bracket_index > -1) {
      const corresponding_end_bracket = find_bracket_close_index(
        sliced_part,
        "right",
        bracket_index + 1
      );
      // todo -- if missing closing bracket then throw the error
      const column_lists = sliced_part.slice(
        bracket_index + 1,
        corresponding_end_bracket
      );
      const list_of_columns = column_lists.join(" ").trim().split(" , ");
      return list_of_columns;
    } else {
      throw new Error(
        " you need to provide the list of the column names in the insert statement"
      );
    }
  } else {
    throw new Error(
      "there is missing `VALUES` keyword in the insert statement."
    );
  }
}

export function dismental_the_comma_inside_bracket(literals: string[]) {
  const literals_arr = JSON.parse(JSON.stringify(literals)) as string[];
  let start_bracket_index = -1;
  for (let index = 0; index < literals_arr.length; index++) {
    const literal = literals_arr[index].toUpperCase();

    if (literal === "(") {
      start_bracket_index = index;
    }

    if (literal === ")") {
      start_bracket_index = -1;
    }

    if (literal === ",") {
      // replace with _,_
      if (start_bracket_index > -1) {
        literals_arr[index] = "_,_";
      }
    }
  }

  return literals_arr;
}

export async function extract_insertion_data(insert_literals: string[]) {
  const index_of_values_keyword = insert_literals.indexOf("VALUES");
  if (index_of_values_keyword > -1) {
    const literals_after_values_keyword = insert_literals.slice(
      index_of_values_keyword + 1
    );
    const dismentals_comma = dismental_the_comma_inside_bracket(
      literals_after_values_keyword
    );
    const all_values_rows = dismentals_comma.join(" ").split(" , ");
    const data: any[] = [];

    for (let index = 0; index < all_values_rows.length; index++) {
      const single_row = await generalTrim(all_values_rows[index].trim(), "(");

      // replace all the _,_ with ,
      const replaced_values_with_comma = single_row
        .replace(new RegExp("_,_", "ig"), ",")
        .trim();
      const final_row_data = replaced_values_with_comma.split(" , ");
      data.push(final_row_data.map((val) => convert_to_number(val)));
    }

    return data;
  } else {
    throw new Error("there is missing VALUES keyword in the insert statement");
  }
}

// check if the given array of data is compitable with columns in the table
// todo - test pending
export function check_columns_compatibility(
  data_to_insert: any[],
  columns_list: any[],
  table_column_info: table_column_info[]
) {
  /*
    Note that data_to_insert is the array of the value to insert into given table,
    In the array of the value number is Bignumber Object , and string is SQL string and NUll is 'NULL'->  null string
    columns_list - This is the list of the column to according to given value , position and order matters,
    Date is also string but pure sql string
    */

  const object_to_return: any = {};

  if (columns_list.length === data_to_insert.length) {
    for (let index = 0; index < columns_list.length; index++) {
      // column name
      const column_name_incoming = columns_list[index] as string;
      const data_incoming = data_to_insert[index]; // Data for the column 'column_name_incoming'
      const column_info_table = table_column_info.find(
        (val) =>
          val.column_name.toLowerCase() === column_name_incoming.toLowerCase()
      );
      if (data_incoming === "NULL") {
        object_to_return[column_name_incoming] = "NULL";
      } else if (column_info_table) {
        switch (column_info_table.column_datatype.first_part.toUpperCase()) {
          case "CHAR":
            const char_length =
              +column_info_table.column_datatype.second_part[0];
            let char_data: any;
            if (data_incoming instanceof BigNumber) {
              char_data = `'${data_incoming.toString()}'`;
            } else {
              if (is_pure_sql_string(data_incoming)) {
                char_data = data_incoming;
              } else {
                throw new Error("need sql string type");
              }
            }
            if (convert_sql_string_to_string(char_data).length <= char_length) {
              const final_char_data = char_data;
              object_to_return[column_name_incoming] = final_char_data;
            } else {
              throw new Error(
                `${column_name_incoming} is of type ${column_info_table.column_datatype.first_part} with max length - ${char_length}`
              );
            }
            break;

          case "VARCHAR":
            const varchar_length =
              +column_info_table.column_datatype.second_part[0];
            let varchar_data: any;
            if (data_incoming instanceof BigNumber) {
              varchar_data = `'${data_incoming.toString()}'`;
            } else {
              if (is_pure_sql_string(data_incoming)) {
                varchar_data = data_incoming;
              } else {
                throw new Error("need sql string type");
              }
            }

            if (
              convert_sql_string_to_string(varchar_data).length <=
              varchar_length
            ) {
              const final_varchar_data = varchar_data;
              object_to_return[column_name_incoming] = final_varchar_data;
            } else {
              throw new Error(
                `${column_name_incoming} is of type ${column_info_table.column_datatype.first_part} with max length - ${varchar_length}`
              );
            }

            break;

          case "TINYTEXT":
            let tinytext_data: any;
            if (data_incoming instanceof BigNumber) {
              tinytext_data = `'${data_incoming.toString()}'`;
            } else {
              if (is_pure_sql_string(data_incoming)) {
                tinytext_data = data_incoming;
              } else {
                throw new Error("need sql string type");
              }
            }

            if (convert_sql_string_to_string(tinytext_data).length <= 255) {
              const final_tinytext_data = tinytext_data;
              object_to_return[column_name_incoming] = final_tinytext_data;
            } else {
              throw new Error(
                `${column_name_incoming} is of type ${
                  column_info_table.column_datatype.first_part
                } with max length - ${255}`
              );
            }

            break;

          case "TEXT":
            let text_data: any;
            if (data_incoming instanceof BigNumber) {
              text_data = `'${data_incoming.toString()}'`;
            } else {
              if (is_pure_sql_string(data_incoming)) {
                text_data = data_incoming;
              } else {
                throw new Error("need sql string type");
              }
            }

            if (convert_sql_string_to_string(text_data).length <= 65535) {
              const final_text_data = text_data;
              object_to_return[column_name_incoming] = final_text_data;
            } else {
              throw new Error(
                `${column_name_incoming} is of type ${column_info_table.column_datatype.first_part} with max length 65,535`
              );
            }

            break;

          case "MEDIUMTEXT":
            let MEDIUMTEXT_data: any;
            if (data_incoming instanceof BigNumber) {
              MEDIUMTEXT_data = `'${data_incoming.toString()}'`;
            } else {
              if (is_pure_sql_string(data_incoming)) {
                MEDIUMTEXT_data = data_incoming;
              } else {
                throw new Error("need sql string type");
              }
            }

            if (
              convert_sql_string_to_string(MEDIUMTEXT_data).length <= 16777215
            ) {
              const final_MEDIUMTEXT_data = MEDIUMTEXT_data;
              object_to_return[column_name_incoming] = final_MEDIUMTEXT_data;
            } else {
              throw new Error(
                `${column_name_incoming} is of type ${column_info_table.column_datatype.first_part} with max length 16777215`
              );
            }
            break;

          case "LONGTEXT":
            let LONGTEXT_data: any;
            if (data_incoming instanceof BigNumber) {
              LONGTEXT_data = `'${data_incoming.toString()}'`;
            } else {
              if (is_pure_sql_string(data_incoming)) {
                LONGTEXT_data = data_incoming;
              } else {
                throw new Error("need sql string type");
              }
            }

            if (
              convert_sql_string_to_string(LONGTEXT_data).length <= 4294967295
            ) {
              const final_LONGTEXT_data = LONGTEXT_data;
              object_to_return[column_name_incoming] = final_LONGTEXT_data;
            } else {
              throw new Error(
                `${column_name_incoming} is of type ${column_info_table.column_datatype.first_part} with max length 4294967295`
              );
            }
            break;

          case "ENUM":
            const enum_values = column_info_table.column_datatype.second_part;

            let enum_data: any;
            if (data_incoming instanceof BigNumber) {
              enum_data = `'${data_incoming.toString()}'`;
            } else {
              if (is_pure_sql_string(data_incoming)) {
                enum_data = data_incoming;
              } else {
                throw new Error("need sql string type");
              }
            }

            if (enum_values.includes(enum_data)) {
              const final_enum_data = enum_data;
              object_to_return[column_name_incoming] = final_enum_data;
            } else {
              throw new Error(
                `data mismatch error, ${column_name_incoming} is of type ${column_info_table.column_datatype.first_part} with values enum_values`
              );
            }

            break;

          case "TINYINT":
            if (data_incoming instanceof BigNumber) {
              const is_TINYINT_unsigned = column_info_table.is_unsigned;
              if (is_TINYINT_unsigned) {
                if (
                  data_incoming.isLessThanOrEqualTo(new BigNumber("255")) &&
                  data_incoming.isGreaterThanOrEqualTo(new BigNumber(0))
                ) {
                  const final_TINYINT_data = data_incoming.toString();
                  object_to_return[column_name_incoming] = final_TINYINT_data;
                } else {
                  throw new Error(
                    `${column_name_incoming} is of type ${column_info_table.column_datatype.first_part} with max length [0, 255]`
                  );
                }
              } else {
                if (
                  data_incoming.isLessThanOrEqualTo(new BigNumber(127)) &&
                  data_incoming.isGreaterThanOrEqualTo(new BigNumber(-128))
                ) {
                  const final_TINYINT_data = data_incoming.toString();
                  object_to_return[column_name_incoming] = final_TINYINT_data;
                } else {
                  throw new Error(
                    `${column_name_incoming} is of type ${column_info_table.column_datatype.first_part} with max length [-128, 127]`
                  );
                }
              }
            } else {
              throw new Error(
                `data mismatch error, ${column_name_incoming} is of type ${column_info_table.column_datatype.first_part}`
              );
            }

            break;

          case "BOOL":
            if ((data_incoming as BigNumber).eq(new BigNumber(0))) {
              const final_BOOL_data = 0;
              object_to_return[column_name_incoming] = final_BOOL_data;
            } else {
              const final_BOOL_data = 1;
              object_to_return[column_name_incoming] = final_BOOL_data;
            }

            break;

          case "BOOLEAN":
            if ((data_incoming as BigNumber).eq(new BigNumber(0))) {
              const final_BOOL_data = 0;
              object_to_return[column_name_incoming] = final_BOOL_data;
            } else {
              const final_BOOL_data = 1;
              object_to_return[column_name_incoming] = final_BOOL_data;
            }

            break;

          case "SMALLINT":
            if (data_incoming instanceof BigNumber) {
              const is_SMALLINT_unsigned = column_info_table.is_unsigned;
              if (is_SMALLINT_unsigned) {
                if (
                  data_incoming.isLessThanOrEqualTo(new BigNumber("65535")) &&
                  data_incoming.isGreaterThanOrEqualTo(new BigNumber(0))
                ) {
                  const final_SMALLINT_data = data_incoming.toString();
                  object_to_return[column_name_incoming] = final_SMALLINT_data;
                } else {
                  throw new Error(
                    `${column_name_incoming} is of type ${column_info_table.column_datatype.first_part} with max length [0, 65535]`
                  );
                }
              } else {
                if (
                  data_incoming.isLessThanOrEqualTo(new BigNumber("32767")) &&
                  data_incoming.isGreaterThanOrEqualTo(new BigNumber("-32768"))
                ) {
                  const final_SMALLINT_data = data_incoming.toString();
                  object_to_return[column_name_incoming] = final_SMALLINT_data;
                } else {
                  throw new Error(
                    `${column_name_incoming} is of type ${column_info_table.column_datatype.first_part} with max length [-32768, 32767]`
                  );
                }
              }
            } else {
              throw new Error(
                `data mismatch error, ${column_name_incoming} is of type ${column_info_table.column_datatype.first_part}`
              );
            }

            break;

          case "MEDIUMINT":
            if (data_incoming instanceof BigNumber) {
              const is_MEDIUMINT_unsigned = column_info_table.is_unsigned;
              if (is_MEDIUMINT_unsigned) {
                if (
                  data_incoming.isLessThanOrEqualTo(
                    new BigNumber("16777215")
                  ) &&
                  data_incoming.isGreaterThanOrEqualTo(new BigNumber(0))
                ) {
                  const final_MEDIUMINT_data = data_incoming.toString();
                  object_to_return[column_name_incoming] = final_MEDIUMINT_data;
                } else {
                  throw new Error(
                    `${column_name_incoming} is of type ${column_info_table.column_datatype.first_part} with max length [0, 16777215]`
                  );
                }
              } else {
                if (
                  data_incoming.isLessThanOrEqualTo(new BigNumber("8388607")) &&
                  data_incoming.isGreaterThanOrEqualTo(
                    new BigNumber("-8388608")
                  )
                ) {
                  const final_MEDIUMINT_data = data_incoming.toString();
                  object_to_return[column_name_incoming] = final_MEDIUMINT_data;
                } else {
                  throw new Error(
                    `${column_name_incoming} is of type ${column_info_table.column_datatype.first_part} with max length [-8388608, 8388607]`
                  );
                }
              }
            } else {
              throw new Error(
                `data mismatch error, ${column_name_incoming} is of type ${column_info_table.column_datatype.first_part}`
              );
            }

            break;

          case "INT":
            if (data_incoming instanceof BigNumber) {
              const is_INT_unsigned = column_info_table.is_unsigned;
              if (is_INT_unsigned) {
                if (
                  data_incoming.isLessThanOrEqualTo(
                    new BigNumber("4294967295")
                  ) &&
                  data_incoming.isGreaterThanOrEqualTo(new BigNumber(0))
                ) {
                  const final_INT_data = data_incoming.toString();
                  object_to_return[column_name_incoming] = final_INT_data;
                } else {
                  throw new Error(
                    `${column_name_incoming} is of type ${column_info_table.column_datatype.first_part} with max length [0, 4294967295]`
                  );
                }
              } else {
                if (
                  data_incoming.isLessThanOrEqualTo(
                    new BigNumber("2147483647")
                  ) &&
                  data_incoming.isGreaterThanOrEqualTo(
                    new BigNumber("-2147483648")
                  )
                ) {
                  const final_INT_data = data_incoming.toString();
                  object_to_return[column_name_incoming] = final_INT_data;
                } else {
                  throw new Error(
                    `${column_name_incoming} is of type ${column_info_table.column_datatype.first_part} with max length [-2147483648, 2147483647]`
                  );
                }
              }
            } else {
              throw new Error(
                `data mismatch error, ${column_name_incoming} is of type ${column_info_table.column_datatype.first_part}`
              );
            }

            break;

          case "BIGINT":
            if (data_incoming instanceof BigNumber) {
              const is_BIGINT_unsigned = column_info_table.is_unsigned;
              if (is_BIGINT_unsigned) {
                if (
                  data_incoming.isLessThanOrEqualTo(
                    new BigNumber("18446744073709551615")
                  ) &&
                  data_incoming.isGreaterThanOrEqualTo(new BigNumber("0"))
                ) {
                  const final_BIGINT_data = data_incoming.toString();
                  object_to_return[column_name_incoming] = final_BIGINT_data;
                } else {
                  throw new Error(
                    `${column_name_incoming} is of type ${column_info_table.column_datatype.first_part} with max length [0, 18446744073709551615]`
                  );
                }
              } else {
                if (
                  data_incoming.isLessThanOrEqualTo(
                    new BigNumber("9223372036854775807")
                  ) &&
                  data_incoming.isGreaterThanOrEqualTo(
                    new BigNumber("-9223372036854775808")
                  )
                ) {
                  const final_BIGINT_data = data_incoming.toString();
                  object_to_return[column_name_incoming] = final_BIGINT_data;
                } else {
                  throw new Error(
                    `${column_name_incoming} is of type ${column_info_table.column_datatype.first_part} with max length [-9223372036854775808, 9223372036854775807]`
                  );
                }
              }
            } else {
              throw new Error(
                `data mismatch error, ${column_name_incoming} is of type ${column_info_table.column_datatype.first_part}`
              );
            }

            break;

          case "DOUBLE":
            if (data_incoming instanceof BigNumber) {
              const final_double_data = data_incoming.toString();
              object_to_return[column_name_incoming] = final_double_data;
            } else {
              throw new Error(
                `data mismatch error, ${column_name_incoming} is of type ${column_info_table.column_datatype.first_part}`
              );
            }

            break;

          case "DATE":
            const type_of_incoming_date_data = typeof data_incoming;
            if (
              type_of_incoming_date_data === "string" &&
              is_pure_sql_string(data_incoming)
            ) {
              const is_date = is_string_date(
                convert_sql_string_to_string(data_incoming)
              );
              if (is_date) {
                const date_final_data = data_incoming as string;
                object_to_return[column_name_incoming] = date_final_data;
              } else {
                throw new Error(
                  `${column_name_incoming} is of type ${column_info_table.column_datatype.first_part} provide the correct date string.`
                );
              }
            } else {
              throw new Error(
                `data mismatch error, ${column_name_incoming} is of type ${column_info_table.column_datatype.first_part}`
              );
            }

            break;

          case "DATETIME":
            const type_of_incoming_datetime_data = typeof data_incoming;
            if (
              type_of_incoming_datetime_data === "string" &&
              is_pure_sql_string(data_incoming)
            ) {
              const is_date = is_string_date(
                convert_sql_string_to_string(data_incoming)
              );
              if (is_date) {
                const date_final_data = data_incoming as string;
                object_to_return[column_name_incoming] = date_final_data;
              } else {
                throw new Error(
                  `${column_name_incoming} is of type ${column_info_table.column_datatype.first_part} provide the correct date string.`
                );
              }
            } else {
              throw new Error(
                `data mismatch error, ${column_name_incoming} is of type ${column_info_table.column_datatype.first_part}`
              );
            }

            break;

          case "TIMESTAMP":
            const type_of_incoming_timestamp_data = typeof data_incoming;
            if (
              type_of_incoming_timestamp_data === "string" &&
              is_pure_sql_string(data_incoming)
            ) {
              const is_date = is_string_date(
                convert_sql_string_to_string(data_incoming)
              );
              if (is_date) {
                const date_final_data = data_incoming as string;
                object_to_return[column_name_incoming] = date_final_data;
              } else {
                throw new Error(
                  `${column_name_incoming} is of type ${column_info_table.column_datatype.first_part} provide the correct date string.`
                );
              }
            } else {
              throw new Error(
                `data mismatch error, ${column_name_incoming} is of type ${column_info_table.column_datatype.first_part}`
              );
            }

            break;

          case "TIME":
            const type_of_time_data = typeof data_incoming;
            if (
              type_of_time_data === "string" &&
              is_pure_sql_string(data_incoming)
            ) {
              const is_time = is_time_string(
                convert_sql_string_to_string(data_incoming)
              );
              if (is_time) {
                const time_final_data = data_incoming as string;
                object_to_return[column_name_incoming] = time_final_data;
              } else {
                throw new Error(
                  `${column_name_incoming} is of type ${column_info_table.column_datatype.first_part} provide the correct time string.`
                );
              }
            } else {
              throw new Error(
                `data mismatch error, ${column_name_incoming} is of type ${column_info_table.column_datatype.first_part}`
              );
            }

            break;

          case "YEAR":
            const type_of_year_data = typeof data_incoming;
            if (
              type_of_year_data === "string" &&
              is_pure_sql_string(data_incoming)
            ) {
              const is_year = is_year_string(
                convert_sql_string_to_string(data_incoming)
              );
              if (is_year) {
                const year_final_data = data_incoming as string;
                object_to_return[column_name_incoming] = year_final_data;
              } else {
                throw new Error(
                  `${column_name_incoming} is of type ${column_info_table.column_datatype.first_part} provide the correct year string.`
                );
              }
            } else {
              throw new Error(
                `data mismatch error, ${column_name_incoming} is of type ${column_info_table.column_datatype.first_part}`
              );
            }
        }
      } else {
        throw new Error(`column - ${column_name_incoming} is not in the table`);
      }
    }

    const final_data_to_return: any = {};
    for (const item of table_column_info) {
      if (!item.is_primary_key) {
        if (object_to_return.hasOwnProperty(item.column_name)) {
          // there is data provided for this column
          final_data_to_return[item.column_name] =
            object_to_return[item.column_name];
        } else {
          if (item.column_datatype.first_part.toUpperCase() === "TIMESTAMP") {
            // add current timestamp
            const current_datetime = moment(new Date()).format(
              "YYYY-MM-DD HH:mm:ss"
            );
            final_data_to_return[item.column_name] = `'${current_datetime}'`;
          } else {
            // check if can be null
            if (item.can_null) {
              // check for the default value
              if (item.default_value) {
                final_data_to_return[item.column_name] = item.default_value;
              } else {
                final_data_to_return[item.column_name] = "NULL";
              }
            } else {
              // can be null need value
              throw new Error(`need value for the column ${item.column_name}`);
            }
          }
        }
      }
    }

    return final_data_to_return;
  } else {
    throw new Error(
      "problem with data insertion, required same number of data as columns list"
    );
  }
}

export async function insert_data(
  insert_statement: string,
  default_database_name: string
) {
  // make string literals
  const insert_literals = make_select_literals(insert_statement);
  // combine sql string together
  const database_info = extract_table_name(insert_literals);
  database_info.database_name = database_info.database_name
    ? database_info.database_name
    : default_database_name;

  const table_schema_unique_identifier = table_schema_unique_string(
    database_info.database_name,
    database_info.table_name
  );

  // access the table schema
  const sql_config_database = await openDB(indexed_sql_config_database, 1);
  const sql_config_database_tnx = sql_config_database.transaction(
    indexed_sql_config_schema_name,
    "readonly"
  );
  const sql_config_object_store = sql_config_database_tnx.objectStore(
    indexed_sql_config_schema_name
  );
  const table_column_info = await sql_config_object_store.get(
    table_schema_unique_identifier
  );
  await sql_config_database_tnx.is_complete();
  sql_config_database.close();

  // access the incoming_column_info
  const insertion_data_col_info =
    extract_insertion_data_column_info(insert_literals);
  const data_to_insert = await extract_insertion_data(insert_literals);
  const final_data_insertion = data_to_insert.map((p) =>
    check_columns_compatibility(p, insertion_data_col_info, table_column_info)
  );

  const version_db = await retrive_next_database_version(
    database_info.database_name
  );
  // add this to table
  const database = await openDB(
    database_info.database_name,
    version_db.current_version
  );
  const tnx = database.transaction(database_info.table_name, "readwrite");
  const primary_keys = await tnx
    .objectStore(database_info.table_name)
    .addAll(final_data_insertion);
  await tnx.is_complete();
  database.close();

  return primary_keys;
}

export async function check_the_data_against_table(
  database_name: string,
  table_name: string,
  data: any[]
) {
  const table_schema_unique_identifier = table_schema_unique_string(
    database_name,
    table_name
  );
  // access the table schema
  const sql_config_database = await openDB(indexed_sql_config_database, 1);
  const sql_config_database_tnx = sql_config_database.transaction(
    indexed_sql_config_schema_name,
    "readonly"
  );
  const sql_config_object_store = sql_config_database_tnx.objectStore(
    indexed_sql_config_schema_name
  );
  const table_column_info = await sql_config_object_store.get(
    table_schema_unique_identifier
  );
  await sql_config_database_tnx.is_complete();
  sql_config_database.close();

  // convert the data to proper data
  const column_names = Object.keys(data[0]);
  const column_values = data.map((p) =>
    Object.values(p).map((val) => convert_the_normal_value_to_sql_value(val))
  );
  const final_data_insertion = column_values.map((p) =>
    check_columns_compatibility(p, column_names, table_column_info)
  );
  return final_data_insertion;
}

export async function parse_insert_data(
  insert_statement: string,
  default_database_name: string
) {
  // make string literals
  const insert_literals = make_select_literals(insert_statement);
  // combine sql string together
  const database_info = extract_table_name(insert_literals);
  database_info.database_name = database_info.database_name
    ? database_info.database_name
    : default_database_name;

  const table_schema_unique_identifier = table_schema_unique_string(
    database_info.database_name,
    database_info.table_name
  );

  // access the table schema
  const sql_config_database = await openDB(indexed_sql_config_database, 1);
  const sql_config_database_tnx = sql_config_database.transaction(
    indexed_sql_config_schema_name,
    "readonly"
  );
  const sql_config_object_store = sql_config_database_tnx.objectStore(
    indexed_sql_config_schema_name
  );
  const table_column_info = await sql_config_object_store.get(
    table_schema_unique_identifier
  );
  await sql_config_database_tnx.is_complete();
  sql_config_database.close();

  // access the incoming_column_info
  const insertion_data_col_info =
    extract_insertion_data_column_info(insert_literals);
  const data_to_insert = await extract_insertion_data(insert_literals);
  const final_data_insertion = data_to_insert.map((p) =>
    check_columns_compatibility(p, insertion_data_col_info, table_column_info)
  );
  return final_data_insertion;
}
