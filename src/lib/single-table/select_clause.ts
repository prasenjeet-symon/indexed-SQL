import { openDB } from "indexed-pdb";
import { multiple_select } from "../multiple-table/multi_select";
import {
  breakpoints_literals,
  find_bracket_close_index,
  generalTrim,
  group_by,
  is_number,
  is_pure_sql_string,
  parse_order_by,
  retrive_next_database_version,
  sort_by,
  unite_sources,
} from "../utils";
import { single_table_where_clause } from "../where_clause";

export function replace_sql_string_with_marker(str: string) {
  const marker: string[] = [];
  const final_string_arr: string[] = [];

  let copy_initial_index = 0;
  let initial_single_quote_index = -1;
  const splited_string = str.split("");

  for (let i = 0; i < splited_string.length; i++) {
    const char = splited_string[i];
    if (char === "'" && initial_single_quote_index > -1) {
      if (splited_string[i + 1]) {
        const next_char = splited_string[i + 1];
        if (next_char === "'") {
          // continue
          i = i + 1;
          continue;
        } else {
          // there is a  string
          const found_string = splited_string
            .slice(initial_single_quote_index, i + 1)
            .join("");
          marker.push(found_string);
          const marker_name = `MS_${marker.length - 1}`;
          const prev_string = splited_string.slice(
            copy_initial_index,
            initial_single_quote_index
          );
          final_string_arr.push(...prev_string, ...[" ", marker_name, " "]);
          copy_initial_index = i + 1;
          initial_single_quote_index = -1;
        }
      } else {
        // there is a  string
        const found_string = splited_string
          .slice(initial_single_quote_index, i + 1)
          .join("");
        marker.push(found_string);
        const marker_name = `MS_${marker.length - 1}`;
        const prev_string = splited_string.slice(
          copy_initial_index,
          initial_single_quote_index
        );
        final_string_arr.push(...prev_string, ...[" ", marker_name, " "]);
        copy_initial_index = i + 1;
        initial_single_quote_index = -1;
      }
    } else if (char === "'" && initial_single_quote_index === -1) {
      initial_single_quote_index = i;
    } else {
      if (i === splited_string.length - 1) {
        // last index
        // just copy the string to final_string_arr
        final_string_arr.push(...splited_string.slice(copy_initial_index));
      }
    }
  }
  return {
    marker: marker.map((p) => p.replace("''", "'")),
    final_string_arr: final_string_arr,
    final_string: final_string_arr.join("").trim(),
  };
}

export function make_select_literals(select_cmd: string) {
  const marker_data = replace_sql_string_with_marker(select_cmd);

  let normalized_string_arr = marker_data.final_string
    // logical operators
    .replace(/\|/g, " | ")

    // comparision operators
    .replace(/=/g, " = ")
    .replace(/</g, " < ")
    .replace(/>/g, " > ")
    .replace(/<=/g, " <= ")
    .replace(/>=/g, " >= ")
    .replace(/!=/g, " != ")

    // arithmatics operators
    .replace(/\+/g, " + ")
    .replace(/\-/g, " - ")
    .replace(/\*/g, " * ")
    .replace(/\//g, " * ")
    .replace(/\%/g, " * ")

    // bracket ( group maker)
    .replace(/\(/g, " ( ")
    .replace(/\)/g, " ) ")

    // bracket BIG ( group maker)
    .replace(/\[/g, " [ ")
    .replace(/\]/g, " ] ")

    // ", "
    .replace(/\,/g, " , ")
    .replace(/\,/g, " , ")

    // string maker
    .replace(/\'/g, " ' ")
    .replace(/\'/g, " ' ")

    // string maker double quotes
    .replace(/\"/g, ' " ')
    .replace(/\"/g, ' " ')

    .trim()
    .replace(/\s+/g, " ")
    .split(/\s/g);

  // add the sql string back
  marker_data.marker.forEach((mrk, i) => {
    const marker_string = `MS_${i}`;
    normalized_string_arr = normalized_string_arr.map((p) =>
      p === marker_string ? mrk : p
    );
  });

  return normalized_string_arr;
}

export function parse_columns_to_select(select_literals: string[]) {
  // slice the select column part
  const select_columns_part = select_literals.slice(
    select_literals.findIndex((val, index) => val.toUpperCase() === "SELECT") +
      1,
    select_literals.findIndex((val, index) => val.toUpperCase() === "FROM")
  );

  // extract the single column part
  let initial_index = 0;
  const single_column_part: string[][] = [];

  for (let index = 0; index < select_columns_part.length; index++) {
    const literal = select_columns_part[index];
    if (literal === ",") {
      // found the single column part
      single_column_part.push(select_columns_part.slice(initial_index, index));
      initial_index = index + 1;
    } else if (index === select_columns_part.length - 1) {
      single_column_part.push(select_columns_part.slice(initial_index));
      initial_index = index + 1;
    }
  }

  // conver into object
  const final_data = single_column_part.map((val, index) => {
    const splited_about_dot = val[0].trim().split("."); // original column name
    const original_column_name =
      splited_about_dot[splited_about_dot.length - 1];

    let final_column_name = null;

    if (val[1]) {
      // there is alies
      if (val[1] === '"') {
        // find the index of closing "
        if (val[2]) {
          const index_of_closing_quote = val.indexOf('"', 2); // we assuming that " there is no double quotes inside "
          if (index_of_closing_quote > -1) {
            final_column_name = val.slice(2, index_of_closing_quote).join(" ");
          } else {
            throw new Error("missing double quote");
          }
        } else {
          throw new Error("need alies");
        }
      } else if (val[1] === "[") {
        // find the index of ]
        if (val[2]) {
          const index_of_closing_bracket = val.indexOf("]", 2);
          if (index_of_closing_bracket > -1) {
            final_column_name = val
              .slice(2, index_of_closing_bracket)
              .join(" ");
          } else {
            throw new Error("missing ] ");
          }
        } else {
          throw new Error("need alies");
        }
      } else if (val[1].toUpperCase() === "AS") {
        if (val[2]) {
          if (val[2] === '"') {
            if (val[3]) {
              const index_of_closing_quote = val.indexOf('"', 3);
              if (index_of_closing_quote > -1) {
                final_column_name = val
                  .slice(3, index_of_closing_quote)
                  .join(" ");
              } else {
                throw new Error("missing double quote");
              }
            } else {
              throw new Error("missing double quote");
            }
          } else if (val[2] === "[") {
            if (val[3]) {
              const index_of_closing_bracket = val.indexOf("]", 2);
              if (index_of_closing_bracket > -1) {
                final_column_name = val
                  .slice(3, index_of_closing_bracket)
                  .join(" ");
              } else {
                throw new Error("missing ] ");
              }
            } else {
              throw new Error("missing ] ");
            }
          } else {
            final_column_name = val[2];
          }
        } else {
          throw new Error("need alies");
        }
      } else {
        final_column_name = val[1];
      }
    } else {
      final_column_name = original_column_name;
    }

    return { original_column_name, final_column_name };
  });

  return final_data;
}

export function is_data_source_subquery(select_literals: string[]) {
  let found_from_index: any;
  const is_subquery = select_literals.findIndex((val, index) => {
    if (val.toUpperCase() === "FROM") {
      found_from_index = index;
    }

    if (found_from_index) {
      if (val === "SELECT") {
        return true;
      }
    }
  });

  return is_subquery > -1 ? true : false;
}

export function parse_union_datasource(datasource_literal: string[]) {
  const data: any[] = [];

  const parse = (
    datasource_literals: string[],
    cb: (result: any[]) => void
  ) => {
    let initial_index = 0;
    let final_index = null;

    if (datasource_literals[0] === "(") {
      // find the end )
      for (let index = 1; index < datasource_literals.length; index++) {
        const literal = datasource_literals[index];
        if (literal === "(") {
          initial_index = initial_index + 1;
        }
        if (literal === ")") {
          if (initial_index === 0) {
            final_index = index;

            const element = datasource_literals.splice(0, final_index + 1);

            data.push({ type: "source", data: element });
            // check the union type
            if (datasource_literals[0]) {
              if (datasource_literals[0].toUpperCase() === "UNION") {
                if (datasource_literals[1]) {
                  if (datasource_literals[1].toUpperCase() === "ALL") {
                    data.push({ type: "union", data: "UNION ALL" });
                    datasource_literals.splice(0, 2);
                  } else {
                    data.push({ type: "union", data: "UNION" });
                    datasource_literals.splice(0, 1);
                  }
                } else {
                  data.push({ type: "union", data: "UNION" });
                  datasource_literals.splice(0, 1);
                }
              } else {
                // no union
              }
            }
            break;
          } else {
            initial_index = initial_index - 1;
          }
        }
      }
    } else {
      // error throw the Error
      throw Error("Need ) in Union");
    }

    if (datasource_literals.length !== 0) {
      parse(datasource_literals, cb);
    } else {
      cb(data);
    }
  };

  return new Promise<any[]>((resolve, reject) => {
    parse(datasource_literal, (result) => resolve(result));
  });
}

export async function parse_data_source(select_literals: string[]) {
  let datasource_literals: string[] = [];
  // extract the data source
  // if there is WHERE literal then try to find the SELECT keyword after the WHERE keyword
  // if it found the SELECT keyword then you have subquery as data source
  // breaking point is ()

  // if no SELECT keyword after the WHERE keyword then there is no subquery as datasource
  // breaking point is one of -  [ORDER BY, GROUP BY, WHERE, OFFSET, LIMIT ]

  // try to find is data source is sub-query

  // prepare the from part
  const is_subquery = is_data_source_subquery(select_literals);

  for (let index = 0; index < select_literals.length; index++) {
    const literal = select_literals[index];
    if (literal.toUpperCase() === "FROM") {
      // remove the prev one
      select_literals = select_literals.slice(index + 1);
      break;
    }
  }

  if (is_subquery) {
    // data source using ()
    if (select_literals[0] === "(") {
      // slice the part
      let initial_index = 0;
      let final_index = null;

      // will find the first ( )
      for (let index = 1; index < select_literals.length; index++) {
        const literal = select_literals[index];
        if (literal === "(") {
          initial_index = initial_index + 1;
        }
        if (literal === ")") {
          if (initial_index === 0) {
            final_index = index;
            break;
          } else {
            initial_index = initial_index - 1;
          }
        }
      }

      if (final_index !== null) {
        // found the closing bracket
        datasource_literals = select_literals.slice(1, final_index);
        // trim the ()
        const trimed_value = (
          await generalTrim(datasource_literals.join(" "), "(")
        ).split(" ");

        if (trimed_value[0].toUpperCase() === "SELECT") {
          // single select source
          // just return the data
          const single_select_datasource = trimed_value;
          return {
            type: "single_select",
            source_literals: single_select_datasource,
          };
        } else {
          // union datasource
          const union_datasource = await parse_union_datasource(trimed_value); // will work both for multi and single query
          return { type: "union", source_literals: union_datasource };
        }
      } else {
        // err throw the Error
        throw Error(`Error - missing )`);
      }
    } else {
      throw Error(`Subquery should be inside ()`);
      // error throw Error
    }
  } else {
    // base table
    const table_base = select_literals[0]; // base table and database info
    const splited_value = table_base.trim().split(".");
    if (splited_value.length > 2) {
      // err thow the Error
      throw Error(`Error near the statement - ${table_base}`);
    } else if (splited_value.length === 2) {
      // contain database also
      const database_name = splited_value[0];
      const table_name = splited_value[1];
      return { type: "base", database_name, table_name };
    } else if (splited_value.length === 1) {
      const database_name: any = null;
      const table_name = splited_value[0];
      return { type: "base", database_name, table_name };
    }
  }
}

// take the select lterals and return  the select literals after combining all the SQL spaced reserved keyword together
// this mean ['GROUP', 'BY'] -> [ 'GROUP BY']
export function combine_sql_spaced_literals(
  full_name: string,
  select_literals: string[]
) {
  const splited_value = full_name.split(" ");

  for (let index = 0; index < select_literals.length; index++) {
    const literal = select_literals[index];
    if (literal.toUpperCase() === splited_value[0].toUpperCase()) {
      // found the first name
      let found_element = 0;

      splited_value.forEach((p, i) => {
        if (select_literals[index + i]) {
          if (p.toUpperCase() === select_literals[index + i].toUpperCase()) {
            // matched one
            found_element = found_element + 1;
          } else {
            found_element = 0;
          }
        } else {
          found_element = 0;
        }
      });

      if (found_element === splited_value.length) {
        // ok you can splice the element and replace with splited_value.join(" ")
        select_literals.splice(
          index,
          splited_value.length,
          splited_value.join(" ")
        );
      } else {
        continue;
      }
    }
  }

  return select_literals;
}

// take the select lterals and return  the select literals after combining all the SQL spaced reserved keyword together
// this mean ['GROUP', 'BY'] -> [ 'GROUP BY']
export function combine_all_sql_spaced_literals(select_literals: string[]) {
  let modified_select_literals: string[] = JSON.parse(
    JSON.stringify(select_literals)
  );

  breakpoints_literals.forEach((p) => {
    modified_select_literals = combine_sql_spaced_literals(
      p,
      modified_select_literals
    );
  });

  return modified_select_literals;
}

// will work for both single and multiple parts
export function parse_data_filter_and_manupulation_part(
  select_literals: string[]
) {
  // manage ORDER BY , GROUP BY,
  select_literals = combine_all_sql_spaced_literals(select_literals);
  let first_breaking_point_index = -1;
  const data_filter_and_manupulation_data = [];

  for (let index = 0; index < select_literals.length; index++) {
    // looping through all the elements in the array
    const literal = select_literals[index].toUpperCase();

    // check if the literal is (
    // if it is bracket then find it's closing patner )
    // after finding the closing patner set the index of this loop to index of the closing bracket
    // continue the loop
    if (literal === "(") {
      // find the closing bracket
      const closing_bracket_index = find_bracket_close_index(
        select_literals,
        "right",
        index + 1
      );
      if (closing_bracket_index > -1) {
        // found the closing bracket index
        // set the loop index to closing_bracket_index
        index = closing_bracket_index;
        continue;
      } else {
        throw new Error("missing closing bracket");
      }
    }

    // if the literal belong the array breakpoints_literals
    // then record the index and wait for other breakpoint literal
    // after finding the other breakpoint literal using this index slice the array and push to new one
    // if no other breakpoint is found then slice all array after recorded recorded index [ recorded index, final_index ]
    if (breakpoints_literals.includes(literal)) {
      // include the literal
      // found the braking point
      if (first_breaking_point_index === -1) {
        // set the breaking point index
        // we found the first breaking point literal
        first_breaking_point_index = index;
      } else {
        // already found a breaking point literal
        // this is the time to slice the array
        // we found the next breaking point
        data_filter_and_manupulation_data.push({
          type: select_literals[first_breaking_point_index],
          data: select_literals.slice(first_breaking_point_index, index),
        });
        first_breaking_point_index = index;
      }
    }

    if (index === select_literals.length - 1) {
      // end index
      // check if we already found the breaking point
      // if yes then slice the array
      if (first_breaking_point_index > -1) {
        // slice the array
        data_filter_and_manupulation_data.push({
          type: select_literals[first_breaking_point_index],
          data: select_literals.slice(first_breaking_point_index),
        });
      }
    }
  }

  return data_filter_and_manupulation_data;
}

export async function parse_select_clause(select_statement: string) {
  // generate the select literals  from the string
  const select_literals: string[] = make_select_literals(select_statement);
  // combine the spaced sql clause
  const combined_sql_spaced_literals: string[] =
    combine_all_sql_spaced_literals(select_literals);

  // extract the select columns parts
  const select_columns_parts = parse_columns_to_select(
    combined_sql_spaced_literals
  );

  // extract the select datasource
  const select_datasource = await parse_data_source(
    combined_sql_spaced_literals
  );

  // extract the data manupulation part
  const data_manipulation_parts = parse_data_filter_and_manupulation_part(
    combined_sql_spaced_literals
  );

  return {
    select_columns_parts,
    select_datasource,
    data_manipulation_parts,
  };
}

// main select query
export async function single_select(
  select_string: string,
  database_name_default: string
) {
  if (is_select_multiquery(make_select_literals(select_string))) {
    // multi query
    return await multiple_select(select_string, database_name_default);
  }

  const parsed_select: any = await parse_select_clause(select_string);

  // check the datasource
  if (parsed_select.select_datasource.type === "union") {
    // union type
    const sources = parsed_select.select_datasource.source_literals.filter(
      (p: any, i: any) => p.type === "source"
    );
    // create the promise of the source
    const source_promises: any[] = [];

    for (let index = 0; index < sources.length; index++) {
      const data = sources[index];
      const trimed_query = await generalTrim(
        (data.data as string[]).join(" "),
        "("
      );
      const source_promise = async () => {
        return single_select(trimed_query, database_name_default);
      };
      source_promises.push(source_promise);
    }

    // result set of the unions sets
    const resolved_data = (await Promise.all(source_promises)) as any[][];
    const unions_types = parsed_select.select_datasource.source_literals.filter(
      (p: any, i: any) => p.type === "union"
    );
    const final_result = await unite_sources(resolved_data, unions_types);
    // do data manupulation
    const manupulated_data = await do_data_modification(
      final_result,
      parsed_select.data_manipulation_parts
    );
    const extracted_columns = select_given_columns_from_dataset(
      manupulated_data,
      parsed_select.select_columns_parts
    );
    return extracted_columns;
  } else if (parsed_select.select_datasource.type === "single_select") {
    // single select
    const final_data = await single_select(
      parsed_select.select_datasource.source_literals.join(" "),
      database_name_default
    );
    // do data manupulation
    const manupulated_data = await do_data_modification(
      final_data,
      parsed_select.data_manipulation_parts
    );
    const extracted_columns = select_given_columns_from_dataset(
      manupulated_data,
      parsed_select.select_columns_parts
    );
    return extracted_columns;
  } else if (parsed_select.select_datasource.type === "base") {
    // base
    const table_name = parsed_select.select_datasource.table_name;
    const database_name = parsed_select.select_datasource.database_name
      ? parsed_select.select_datasource.database_name
      : database_name_default;
    const version_number = await retrive_next_database_version(database_name);
    // open the database connection to given database info
    const database = await openDB(
      database_name,
      version_number.current_version
    );
    const tnx = database.transaction(table_name, "readonly");
    const store = tnx.objectStore(table_name);
    const final_result = await store.getAll();
    await tnx.is_complete();
    database.close();

    // do data manupulation
    const manupulated_data = await do_data_modification(
      final_result,
      parsed_select.data_manipulation_parts
    );
    const extracted_columns = select_given_columns_from_dataset(
      manupulated_data,
      parsed_select.select_columns_parts
    );
    return extracted_columns;
  }
}

export async function do_data_modification(
  data: any,
  modification_arr: any,
  is_multi_query = false
) {
  modification_arr = modification_arr.map((p: any) => {
    switch (p.type) {
      case "WHERE":
        if (is_multi_query) {
          return {
            type: p.type,
            data: p.data.slice(1).join(" "),
          };
        } else {
          return {
            type: p.type,
            data: p.data
              .slice(1)
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
              .join(" "),
          };
        }

      case "GROUP BY":
        if (is_multi_query) {
          return {
            type: p.type,
            data: p.data[1],
          };
        } else {
          return {
            type: p.type,
            data: p.data[1].split(".")[p.data[1].split(".").length - 1],
          };
        }

      case "ORDER BY":
        return { type: p.type, data: parse_order_by(p.data, is_multi_query) };

      case "OFFSET":
        return { type: p.type, data: p.data[1] };

      case "LIMIT":
        return { type: p.type, data: p.data[1] };
    }
  });

  for (let index = 0; index < modification_arr.length; index++) {
    const modif = modification_arr[index];
    switch (modif.type) {
      case "WHERE":
        const final_where_datas: any[] = [];

        for (let innder_index = 0; innder_index < data.length; innder_index++) {
          const final_where_data = await single_table_where_clause(
            data[innder_index],
            modif.data
          );
          if (final_where_data) {
            final_where_datas.push(final_where_data);
          }
        }
        data = final_where_datas;
        break;

      case "GROUP BY":
        data = await group_by(data, modif.data);
        break;

      case "ORDER BY":
        data = sort_by(data, modif.data);
        break;

      case "OFFSET":
        data = data.slice(+modif.data);
        break;

      case "LIMIT":
        data = data.slice(0, +modif.data);
        break;
    }
  }

  return data;
}

export function select_given_columns_from_dataset(
  data: any[],
  columns: { original_column_name: string; final_column_name: string }[]
) {
  let is_error = false;
  for (let index = 0; index < data.length; index++) {
    const obj = data[index] as any;
    const new_object: any = {};
    for (let inner_index = 0; inner_index < columns.length; inner_index++) {
      const column = columns[inner_index];
      if (obj.hasOwnProperty(column.original_column_name)) {
        new_object[column.final_column_name] = obj[column.original_column_name];
      } else {
        is_error = true;
        break;
      }
    }

    if (is_error) {
      break;
    }

    data[index] = new_object;
  }

  if (is_error) {
    return null;
  } else {
    return data;
  }
}

export function is_select_multiquery(select_literals: string[]) {
  // try to find the the keyword JOIN
  // if  found then check the keyword at index - 1
  const initial_keyword = ["INNER", "OUTER"];
  let join_index = -1;

  for (let index = 0; index < select_literals.length; index++) {
    const literal = select_literals[index].toUpperCase();
    if (literal === "(") {
      // find the closing bracket
      const index_of_closing_bracket = find_bracket_close_index(
        select_literals,
        "right",
        index + 1
      );
      if (index_of_closing_bracket > -1) {
        index = index_of_closing_bracket;
        continue;
      } else {
        throw new Error(" missing closing bracket ");
      }
    }

    if (literal === "JOIN") {
      join_index = index;
      break;
    }
  }

  if (join_index > -1) {
    // found the join index there is possibility of the multi query
    if (
      initial_keyword.includes(select_literals[join_index - 1].toUpperCase())
    ) {
      return true;
    } else {
      return false;
    }
  } else {
    return false;
  }
}
