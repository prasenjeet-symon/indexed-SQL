import { openDB } from "indexed-pdb";
import { join_type } from "../main.interface";
import {
  do_data_modification,
  make_select_literals,
  parse_data_filter_and_manupulation_part,
  parse_union_datasource,
  select_given_columns_from_dataset,
  single_select,
} from "../single-table/select_clause";
import {
  find_bracket_close_index,
  generalTrim,
  retrive_next_database_version,
  unite_sources,
} from "../utils";
import { single_table_where_clause } from "../where_clause";

export function find_alies_column_name(col_literals: string[]) {
  // col literals is single column splited by single space
  // first element is original column name
  // there is final column starting from index 1
  // if found "" or [], single string then that is alies
  // if found AS then start searching from index 2
  const find_col_name = (ini_index: number) => {
    if (col_literals[ini_index] === '"') {
      // find the corresponding double quote
      const corresponding_double_quote_index = col_literals.indexOf(
        '"',
        ini_index + 1
      );
      if (corresponding_double_quote_index > -1) {
        const final_column_name = col_literals.slice(
          ini_index + 1,
          corresponding_double_quote_index
        );
        return final_column_name.join(" ").trim();
      } else {
        throw new Error("missing double quotes in the column parts");
      }
    }

    if (col_literals[ini_index] === "[") {
      const corres_closing_bracket_index = col_literals.indexOf("]", ini_index);
      if (corres_closing_bracket_index > -1) {
        const final_column_name = col_literals.slice(
          ini_index + 1,
          corres_closing_bracket_index
        );
        return final_column_name.join(" ").trim();
      } else {
        throw new Error("missing closing ]");
      }
    }

    return col_literals[ini_index].trim();
  };

  if (col_literals.length === 1) {
    return col_literals[0];
  } else if (col_literals.length >= 2) {
    if (col_literals[1].toUpperCase() === "AS") {
      return find_col_name(2);
    } else {
      return find_col_name(1);
    }
  }
}

export function extract_columns_info_of_multi_select(
  multi_select_literals: string[]
) {
  // anything that is between SELECT and FROM is column info
  // find the first index of SELECT keyword
  const SELECT_keyword_index = multi_select_literals.findIndex(
    (val) => val.toUpperCase() === "SELECT"
  );
  const FROM_keyword_index = multi_select_literals.findIndex(
    (val) => val.toUpperCase() === "FROM"
  );
  if (SELECT_keyword_index > -1 && FROM_keyword_index > -1) {
    // slice the columns parts
    const sliced_col_parts = multi_select_literals.slice(
      SELECT_keyword_index + 1,
      FROM_keyword_index
    );
    // join by the single space and then split by the ' , '
    const columns_info_list = sliced_col_parts.join(" ").trim().split(" , ");
    // alies can be "something bal", [something bal], or normal string
    return columns_info_list.map(function (val, i) {
      const splited_values = val.trim().split(" "); // converting string back to string literals
      const original_column_name = splited_values[0];
      // find the final_column value
      const final_column_name = find_alies_column_name(splited_values);
      return { original_column_name, final_column_name };
    });
  } else {
    throw new Error(
      "We need SELECT and FROM keyword in multi query statement."
    );
  }
}

export function multiple_select_data_source(multi_select_literals: string[]) {
  // we assume we only support INNER JOIN , LEFT OUTER JOIN, RIGHT OUTER JOIN
  // when we find the INNER , LEFT , RIGHT, ON then we extract the data
  // once we find the WHERE if any then we break the loop
  let data_sources: any[] = [];
  let found_type = "data_source";
  let found_index = -1;

  for (let index = 0; index < multi_select_literals.length; index++) {
    const literal = multi_select_literals[index].toUpperCase().trim();

    if (literal === "(") {
      // find the closing bracket
      const index_of_closing_bracket = find_bracket_close_index(
        multi_select_literals,
        "right",
        index + 1
      );
      if (index_of_closing_bracket > -1) {
        index = index_of_closing_bracket;
        continue;
      } else {
        throw new Error("missing closing )");
      }
    }

    if (literal === "FROM") {
      // we found the break point
      found_index = index + 1;
      continue;
    }

    if (literal === "INNER") {
      // find the JOIN after this
      if (multi_select_literals[index + 1].toUpperCase().trim() === "JOIN") {
        // we found the breaking point
        // slice the data source
        data_sources.push({
          type: found_type,
          data: multi_select_literals.slice(found_index, index),
        });
        data_sources.push({
          type: "join_type",
          data: multi_select_literals
            .slice(index, index + 2)
            .join(" ")
            .trim(),
        });
        found_index = index + 2;
        found_type = "data_source";
        index = index + 1;
        continue;
      } else {
        throw new Error(
          "missing join keyword , use INNER JOIN instead of INNER"
        );
      }
    }

    if (literal === "ON") {
      // extract the data source
      data_sources.push({
        type: found_type,
        data: multi_select_literals.slice(found_index, index),
      });
      found_index = index + 1;
      found_type = "on_condition";
      continue;
    }

    if (literal === "LEFT") {
      if (
        multi_select_literals[index + 1].toUpperCase().trim() === "OUTER" &&
        multi_select_literals[index + 2].toUpperCase().trim() === "JOIN"
      ) {
        data_sources.push({
          type: found_type,
          data: multi_select_literals.slice(found_index, index),
        });
        data_sources.push({
          type: "join_type",
          data: multi_select_literals
            .slice(index, index + 3)
            .join(" ")
            .trim(),
        });
        found_index = index + 3;
        found_type = "data_source";
        index = index + 2;
        continue;
      } else {
        throw new Error(
          "missing OUTER JOIN keyword , use LEFT OUTER JOIN instead of LEFT"
        );
      }
    }

    if (literal === "RIGHT") {
      if (
        multi_select_literals[index + 1].toUpperCase().trim() === "OUTER" &&
        multi_select_literals[index + 2].toUpperCase().trim() === "JOIN"
      ) {
        data_sources.push({
          type: found_type,
          data: multi_select_literals.slice(found_index, index),
        });
        data_sources.push({
          type: "join_type",
          data: multi_select_literals
            .slice(index, index + 3)
            .join(" ")
            .trim(),
        });
        found_index = index + 3;
        found_type = "data_source";
        index = index + 2;
        continue;
      } else {
        throw new Error(
          "missing OUTER JOIN keyword , use LEFT OUTER JOIN instead of LEFT"
        );
      }
    }

    if (literal === "WHERE") {
      data_sources.push({
        type: found_type,
        data: multi_select_literals.slice(found_index, index),
      });
      found_index = -1;
      break;
    }

    if (index === multi_select_literals.length - 1) {
      // last index
      // slice data
      data_sources.push({
        type: found_type,
        data: multi_select_literals.slice(found_index),
      });
      found_index = -1;
    }
  }

  // todo - INNER JOIN need no ON clause check for that
  // verify the data sources
  return data_sources;
}

export async function is_data_source_union(data_source: string[]) {
  let trimed_form: any[] = [];
  // trim the data with ()
  if (data_source[0].toUpperCase() === "(") {
    const matching_bracket_index = find_bracket_close_index(
      data_source,
      "right",
      1
    );
    if (matching_bracket_index > -1) {
      // slice the part
      // that part actual union
      const sliced_part = data_source.slice(1, matching_bracket_index);
      trimed_form = (
        await generalTrim(sliced_part.join(" ").trim(), "(")
      ).split(" ");
    } else {
      throw new Error("wrong statement missing ()");
    }
  } else {
    return false;
  }

  let is_union = false;

  for (let index = 0; index < trimed_form.length; index++) {
    const literal = trimed_form[index].toUpperCase().trim();

    if (literal === "(") {
      // find the closing bracket
      const closing_bracket_index = find_bracket_close_index(
        trimed_form,
        "right",
        index + 1
      );
      if (closing_bracket_index > -1) {
        index = closing_bracket_index;
        continue;
      } else {
        throw new Error("missing closing bracket");
      }
    }

    if (literal === "UNION ALL" || literal === "UNION") {
      is_union = true;
      break;
    }
  }

  return is_union;
}

export async function is_single_select_datasource(data_source: string[]) {
  let trimed_form: any[] = [];
  // trim the data with ()
  if (data_source[0] === "(") {
    const matching_bracket_index = find_bracket_close_index(
      data_source,
      "right",
      1
    );
    if (matching_bracket_index > -1) {
      // slice the part
      // that part actual union
      const sliced_part = data_source.slice(1, matching_bracket_index);
      trimed_form = (await generalTrim(sliced_part.join(" "), "(")).split(" ");
    } else {
      throw new Error("wrong statement missing ()");
    }
  } else {
    return false;
  }

  if (trimed_form[0].toUpperCase() === "SELECT") {
    return true;
  } else {
    return false;
  }
}

// todo - pending test
export async function retrive_single_select_datasource(
  datasource_literals: string[],
  database_name_default: string
) {
  let select_string: any;
  let alies_name: any;

  if (datasource_literals[0].toUpperCase() === "(") {
    const closing_bracket_index = find_bracket_close_index(
      datasource_literals,
      "right",
      1
    );
    if (closing_bracket_index > -1) {
      select_string = await generalTrim(
        datasource_literals.slice(1, closing_bracket_index).join(" "),
        "("
      );
      // find the alies
      if (
        datasource_literals[closing_bracket_index + 1].toUpperCase() === "AS"
      ) {
        alies_name = datasource_literals[closing_bracket_index + 2];
      } else {
        alies_name = datasource_literals[closing_bracket_index + 1];
      }
    } else {
      throw new Error("missing closing bracket");
    }
  }

  const final_result: any = await single_select(
    select_string,
    database_name_default
  );
  return final_result.map((val: any, i: any) => {
    const final_obj: any = {};
    Object.keys(val).forEach((key) => {
      final_obj[`${alies_name}.${key}`] = val[key];
    });
    return final_obj;
  });
}

export async function retrive_base_source_data(
  datasource_literals: string[],
  database_name_default: string
) {
  let database_name: any;
  let table_name: any;
  let alies_name: any;

  const database_table_name = datasource_literals[0];
  const splited_val = database_table_name.trim().split(".");
  if (splited_val.length === 2) {
    // there is database name given
    database_name = splited_val[0];
    table_name = splited_val[1];
  } else if (splited_val.length === 1) {
    // no database name
    table_name = splited_val[0];
  }

  if (datasource_literals.length === 1) {
    // there is no alies use this as alies
    alies_name = database_table_name;
  } else if (datasource_literals.length >= 2) {
    if (datasource_literals[1].toUpperCase() === "AS") {
      alies_name = datasource_literals[2];
    } else {
      alies_name = datasource_literals[1];
    }
  }

  database_name = database_name ? database_name : database_name_default;

  // retrive all the data from database
  const version_db = await retrive_next_database_version(database_name);
  const multi_database_connection = await openDB(
    database_name,
    version_db.current_version
  );
  const tnx = multi_database_connection.transaction(table_name);
  const final_result = await tnx.objectStore(table_name).getAll();
  await tnx.is_complete();
  multi_database_connection.close();

  return final_result.map((val, i) => {
    const final_obj: any = {};
    Object.keys(val).forEach((key) => {
      final_obj[`${alies_name}.${key}`] = val[key];
    });
    return final_obj;
  });
}

// todo - pending test
export async function retrive_union_datasource(
  datasource_literals: string[],
  database_name_default: string
) {
  // union datasource
  // extract the data source and alies
  let union_datasource_row: any;
  let alies_name: string;

  for (let index = 0; index < datasource_literals.length; index++) {
    const literal = datasource_literals[index].toUpperCase();
    if (literal === "(") {
      const closing_bracket_index = find_bracket_close_index(
        datasource_literals,
        "right",
        index + 1
      );
      if (closing_bracket_index > -1) {
        union_datasource_row = datasource_literals.slice(
          index + 1,
          closing_bracket_index
        );
        // find the alies
        if (
          datasource_literals[closing_bracket_index + 1].toUpperCase() === "AS"
        ) {
          alies_name = datasource_literals[closing_bracket_index + 2];
        } else {
          alies_name = datasource_literals[closing_bracket_index + 1];
        }
        break;
      } else {
        throw new Error("missing closing bracket ");
      }
    }
  }

  const trimed_value = (
    await generalTrim(union_datasource_row.join(" "), "(")
  ).split(" ");
  const union_datasource = await parse_union_datasource(trimed_value);
  const sources = union_datasource.filter((p, i) => p.type === "source");

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
  const unions_types = union_datasource.filter((p, i) => p.type === "union");
  const final_result = await unite_sources(resolved_data, unions_types);

  // add alies to final result set
  return final_result.map((val, i) => {
    const final_obj: any = {};
    Object.keys(val).forEach((key) => {
      final_obj[`${alies_name}.${key}`] = val[key];
    });
    return final_obj;
  });
}

//todo - need test
export async function retrive_source_data(
  data_source: string[],
  database_name_default: string
) {
  // check if the data source is union , single select or base
  // is data source union
  // note that result set will contain the alies column name
  if (await is_data_source_union(data_source)) {
    const union_data = await retrive_union_datasource(
      data_source,
      database_name_default
    );
    return union_data;
  } else if (await is_single_select_datasource(data_source)) {
    const single_select_data = await retrive_single_select_datasource(
      data_source,
      database_name_default
    );
    return single_select_data;
  } else {
    // base data source
    const base_data = await retrive_base_source_data(
      data_source,
      database_name_default
    );
    return base_data;
  }
}

export async function perform_join(
  left_table_source: any[],
  right_table_source: any[],
  join_type: join_type,
  on_subclause?: any
) {
  if (join_type === "INNER JOIN") {
    // there can't be a on clause
    // in such case ON is true bydefault
    // choose any table left table and start joining the table on right
    const final_result: any[] = [];

    for (
      let left_index = 0;
      left_index < left_table_source.length;
      left_index++
    ) {
      for (
        let right_index = 0;
        right_index < right_table_source.length;
        right_index++
      ) {
        const left_obj = left_table_source[left_index];
        const right_obj = right_table_source[right_index];
        const intermediate_obj = { ...left_obj, ...right_obj };
        if (!on_subclause) {
          // there is no on subclause
          // just add the object
          final_result.push(intermediate_obj);
          continue;
        }
        // use on clause to check if this can be in final result
        const result = await single_table_where_clause(
          intermediate_obj,
          on_subclause.join(" ").trim()
        );
        if (result) {
          final_result.push(result);
        }
      }
    }

    return final_result;
  } else if (join_type === "LEFT OUTER JOIN") {
    // left outer join
    // if there is no match in right table then include it as null object
    let final_result: any[] = [];

    for (
      let left_index = 0;
      left_index < left_table_source.length;
      left_index++
    ) {
      let is_there_matching_in_right_table = false;

      for (
        let right_index = 0;
        right_index < right_table_source.length;
        right_index++
      ) {
        const left_obj = left_table_source[left_index];
        const right_obj = right_table_source[right_index];
        const intermediate_obj = { ...left_obj, ...right_obj };
        const result = await single_table_where_clause(
          intermediate_obj,
          on_subclause.join(" ").trim()
        );
        if (result) {
          is_there_matching_in_right_table = true;
          final_result.push(result);
        }
      }

      if (!is_there_matching_in_right_table) {
        // no matching in the right table
        const right_table_object_structure = right_table_source[0];
        Object.keys(right_table_object_structure).forEach((key, i) => {
          right_table_object_structure[key] = null;
        });

        const final_object = {
          ...left_table_source[left_index],
          ...right_table_object_structure,
        };
        final_result.push(final_object);
      }
    }

    return final_result;
  } else if (join_type === "RIGHT OUTER JOIN") {
    let final_result: any[] = [];
    // right outer join
    // start with right table and search the left table
    // if no matching is found in the left table then null the object and join the object together
    for (
      let right_index = 0;
      right_index < right_table_source.length;
      right_index++
    ) {
      // is there matching in the left table
      let is_there_matching_in_left_table = false;

      // loop through the left table
      for (
        let left_index = 0;
        left_index < left_table_source.length;
        left_index++
      ) {
        const right_obj = right_table_source[right_index];
        const left_obj = left_table_source[left_index];
        const intermediate_obj = { ...left_obj, ...right_obj };
        const result = await single_table_where_clause(
          intermediate_obj,
          on_subclause.join(" ").trim()
        );
        if (result) {
          is_there_matching_in_left_table = true;
          final_result.push(result);
        }
      }

      // check if the matching found in the left table
      if (!is_there_matching_in_left_table) {
        // no matching in the left table
        const left_table_object_structure = left_table_source[0];
        Object.keys(left_table_object_structure).forEach((key, i) => {
          left_table_object_structure[key] = null;
        });
        const final_object = {
          ...right_table_source[right_index],
          ...left_table_object_structure,
        };
        final_result.push(final_object);
      }
    }

    return final_result;
  }
}

// main multi select
export async function multiple_select(
  select_string: string,
  database_name_default: string
) {
  const m_select_literals = make_select_literals(select_string);
  // extract the multi select datasource
  const multi_select_data_source =
    multiple_select_data_source(m_select_literals);
  // combine sql join data
  // data source -> join type -> data source -> on condition ( life cycle)
  // we assume there is on conditon always on the query

  let intermediate_data: any = [];

  for (let index = 0; index < multi_select_data_source.length; index++) {
    let left_table: any[] = [];
    let right_table: any[] = [];

    if (index === 0) {
      left_table = await retrive_source_data(
        multi_select_data_source[index].data,
        database_name_default
      );

      if (
        multi_select_data_source[index + 1].type === "join_type" &&
        multi_select_data_source[index + 2].type === "data_source"
      ) {
        if (multi_select_data_source[index + 3].type === "on_condition") {
          right_table = await retrive_source_data(
            multi_select_data_source[index + 2].data,
            database_name_default
          );
          intermediate_data = await perform_join(
            left_table,
            right_table,
            multi_select_data_source[index + 1].data,
            multi_select_data_source[index + 3].data
          );
          index = 3;
          continue;
        } else if (multi_select_data_source[index + 1].data === "INNER JOIN") {
          right_table = await retrive_source_data(
            multi_select_data_source[index + 2].data,
            database_name_default
          );
          intermediate_data = await perform_join(
            left_table,
            right_table,
            multi_select_data_source[index + 1].data
          );
          index = 2;
          continue;
        } else {
          throw new Error("missing on subclause");
        }
      } else {
        throw new Error("query problem");
      }
    } else {
      left_table = intermediate_data;
      if (
        multi_select_data_source[index].type === "join_type" &&
        multi_select_data_source[index + 1].type === "data_source"
      ) {
        if (multi_select_data_source[index + 2].type === "on_condition") {
          right_table = await retrive_source_data(
            multi_select_data_source[index + 1].data,
            database_name_default
          );
          intermediate_data = await perform_join(
            left_table,
            right_table,
            multi_select_data_source[index].data,
            multi_select_data_source[index + 2].data
          );
          index = index + 2;
          continue;
        } else if (multi_select_data_source[index].data === "INNER JOIN") {
          right_table = await retrive_source_data(
            multi_select_data_source[index + 1].data,
            database_name_default
          );
          intermediate_data = await perform_join(
            left_table,
            right_table,
            multi_select_data_source[index].data
          );
          index = index + 1;
          continue;
        } else {
          throw new Error("missing on subclause");
        }
      } else {
        throw new Error("query problem");
      }
    }
  }

  // now intermediate_data is holding the joined data
  // do data modification
  const data_manupulaton_config =
    parse_data_filter_and_manupulation_part(m_select_literals);
  const manupulated_data = await do_data_modification(
    intermediate_data,
    data_manupulaton_config,
    true
  );
  const multi_column_info: any =
    extract_columns_info_of_multi_select(m_select_literals);
  const extracted_columns = select_given_columns_from_dataset(
    manupulated_data,
    multi_column_info
  );
  return extracted_columns;
}
