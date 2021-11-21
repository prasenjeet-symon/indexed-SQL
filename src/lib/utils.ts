import BigNumber from "bignumber.js";
import { openDB } from "indexed-pdb";
import moment from "moment";
import {
  arithmatic_operator,
  comparision_operator,
  generalTrimOption,
} from "./main.interface";

const arithmatic_operators = ["/", "*", "+", "-"];
export const comparision_operators = ["!=", "<", ">", "<=", ">=", "="];
export const arithmatic_operators_with_modu = ["+", "-", "*", "/", "%"];
export const logical_operators = ["AND", "OR"];
export const breakpoints_literals = [
  "ORDER BY",
  "GROUP BY",
  "WHERE",
  "OFFSET",
  "LIMIT",
];
export const indexed_sql_config_database = "indexed-slq-config"; // config database name
export const indexed_sql_config_schema_name = "schema_config"; // table info table
export const indexed_sql_config_versions = "indexed-sql-config-version"; // databases versions table

export function is_number(str: string) {
  return !isNaN(Number(str));
}

export function is_pure_sql_string(str: string) {
  str = str.trim();
  if (str.slice(0, 1) === "'" && str.slice(str.length - 1) === "'") {
    return true;
  } else {
    return false;
  }
}

export function do_arithmatics(arithmatic_literals: any[]) {
  const do_maths = (
    left_operand: BigNumber,
    right_operand: BigNumber,
    type: arithmatic_operator
  ) => {
    switch (type) {
      case "/":
        return left_operand.dividedBy(right_operand);

      case "*":
        return left_operand.multipliedBy(right_operand);

      case "+":
        return left_operand.plus(right_operand);

      case "-":
        return left_operand.minus(right_operand);
    }
  };

  const manage_multiply_and_divide = (
    num: any[],
    cb: (result: BigNumber[]) => void
  ) => {
    for (let index = 0; index < num.length; index++) {
      const operator = num[index];
      if (operator === "*" || operator === "/") {
        const left_operand = num[index - 1] as BigNumber;
        const right_operand = num[index + 1] as BigNumber;
        const result = do_maths(left_operand, right_operand, operator);
        num.splice(index - 1, 3, result);
        break;
      }
    }

    if (num.some((val) => ["*", "/"].includes(val))) {
      manage_multiply_and_divide(num, cb);
    } else {
      cb(num);
    }
  };

  const manage_addition_and_substraction = (
    num: any[],
    cb: (result: BigNumber[]) => void
  ) => {
    for (let index = 0; index < num.length; index++) {
      const operator = num[index];
      if (operator === "+" || operator === "-") {
        const left_operand = num[index - 1] as BigNumber;
        const right_operand = num[index + 1] as BigNumber;
        const result = do_maths(left_operand, right_operand, operator);
        num.splice(index - 1, 3, result);
        break;
      }
    }

    if (num.some((val) => ["+", "-"].includes(val))) {
      manage_addition_and_substraction(num, cb);
    } else {
      cb(num);
    }
  };

  return new Promise<BigNumber>((resolve, reject) => {
    manage_multiply_and_divide(arithmatic_literals, (result) => {
      manage_addition_and_substraction(result, (data) => {
        resolve(data[0]);
      });
    });
  });
}

export const manage_of = (data: any[]) => {
  const extended_arithmatic_operators = [...arithmatic_operators, "(", ")"];
  const final_data: string[] = [];

  data.forEach((p, i) => {
    if (p === "(") {
      const left_value = data[i - 1];
      if (left_value) {
        // check if arithmatic operator
        if (!extended_arithmatic_operators.includes(left_value)) {
          final_data.push(...["*", p]);
        } else {
          final_data.push(p);
        }
      } else {
        final_data.push(p);
      }
    } else if (p === ")") {
      const right_value = data[i + 1];
      if (right_value) {
        if (!extended_arithmatic_operators.includes(right_value)) {
          final_data.push(...[p, "*"]);
        } else {
          final_data.push(p);
        }
      } else {
        final_data.push(p);
      }
    } else {
      final_data.push(p);
    }
  });

  return final_data;
};

export function find_bracket_close_index(
  arr: any[],
  direction: "right" | "left",
  start_index: number
) {
  if (direction === "right") {
    let found_final_index = -1;
    let found_initial_index = 0;

    for (let index = start_index; index < arr.length; index++) {
      const literal = arr[index];
      if (literal === "(") {
        found_initial_index = found_initial_index + 1;
      } else if (literal === ")") {
        if (found_initial_index === 0) {
          found_final_index = index;
          break;
        } else {
          found_initial_index = found_initial_index - 1;
        }
      }
    }

    return found_final_index;
  } else {
    let found_final_index = -1;
    let found_initial_index = 0;

    for (let index = start_index; index >= 0; index--) {
      const literal = arr[index];
      if (literal === ")") {
        found_initial_index = found_initial_index + 1;
      } else if (literal === "(") {
        if (found_initial_index === 0) {
          found_final_index = index;
          break;
        } else {
          found_initial_index = found_initial_index - 1;
        }
      }
    }

    return found_final_index;
  }
}

export function findIndexDir<T>(
  arr: T[],
  search_element: T,
  direction: "right" | "left",
  start_index: number
) {
  let found_index = -1;
  if (direction === "right") {
    found_index = arr.indexOf(search_element, start_index);
  } else {
    for (let index = start_index; index >= 0; index--) {
      const value = arr[index];
      if (value === search_element) {
        found_index = index;
        break;
      }
    }
  }

  return found_index;
}

export function found_number_of_comparision_operators(
  where_literals: string[]
) {
  let number_of_comparision_operators_in_where_array = 0;
  where_literals.forEach((p) => {
    if (comparision_operators.includes(p)) {
      number_of_comparision_operators_in_where_array =
        number_of_comparision_operators_in_where_array + 1;
    }
  });

  return number_of_comparision_operators_in_where_array;
}

// if the string contain number starting from first position then extract that number
// i.e 123Hasjkhka , 34234jsdkajdkla -> 123, 34234
// invalid - khkdsakh34343kjh
export function extract_number_from_string(str: string) {
  // chck if the str is pure number
  // if pure number then just return the pure number
  if (is_number(str)) {
    // pure number
    return new BigNumber(str);
  } else {
    // not a number
    const splited_value = str.split("");
    // will hold the number found in the string
    const number_found: string[] = [];

    for (let index = 0; index < splited_value.length; index++) {
      const value = splited_value[index];
      if (is_number(value)) {
        // number pure
        number_found.push(value);
        continue;
      } else {
        // string found
        break;
      }
    }

    if (number_found.length !== 0) {
      // there was number in the string
      return new BigNumber(number_found.join(""));
    } else {
      // str is pure string
      return new BigNumber(0);
    }
  }
}

export function is_string_date(date_string: string) {
  const array_date_data = date_string
    .trim()
    .split("-")
    .filter((p) => p !== "");
  const array_time_data = date_string
    .trim()
    .split(":")
    .filter((p) => p !== "");
  if (array_date_data.length === 3) {
    // then check for the value type
    if (array_time_data.length === 3) {
      // there is time part too
      const number_data_first_2 = array_date_data
        .slice(0, 2)
        .filter((d) => !isNaN(Number(d)));
      const time_part = date_string
        .trim()
        .split(" ")
        .filter((p) => p !== "")[1]
        .split(":")
        .filter((d) => !isNaN(Number(d)));
      if (time_part.length === 3 && number_data_first_2.length === 2) {
        return true;
      } else {
        return false;
      }
    } else {
      const number_data_first_3 = array_date_data.filter(
        (d) => !isNaN(Number(d))
      );
      if (number_data_first_3.length === 3) {
        return true;
      } else {
        return false;
      }
    }
  } else {
    return false;
  }
}

export function is_comparing_dates(
  left_operand: string,
  right_operand: string,
  operator: comparision_operator
) {
  if (is_string_date(left_operand) && is_string_date(right_operand)) {
    return true;
  } else {
    return false;
  }
}

export function compare_dates(
  left_operand: string,
  right_operand: string,
  operator: comparision_operator
) {
  const left_date = new Date(left_operand);
  const right_date = new Date(right_operand);

  switch (operator) {
    case "=":
      return +left_date === +right_date;
      break;

    case "!=":
      return +left_date !== +right_date;
      break;

    case "<":
      return +left_date < +right_date;
      break;

    case ">":
      return +left_date > +right_date;
      break;

    case "<=":
      return +left_date <= +right_date;
      break;

    case ">=":
      return +left_date >= +right_date;
      break;
  }
}

export function is_comparing_string(
  left_operand: any,
  right_operand: any,
  operator: comparision_operator
) {
  const left_operand_type = typeof left_operand;
  const right_operand_type = typeof right_operand;

  if (left_operand_type === "string" && right_operand_type === "string") {
    return true;
  } else {
    return false;
  }
}

export function compare_strings(
  left_operand: string,
  right_operand: string,
  operator: comparision_operator
) {
  switch (operator) {
    case "=":
      return left_operand === right_operand;
      break;

    case "!=":
      return left_operand !== right_operand;
      break;

    case "<":
      return left_operand < right_operand;
      break;

    case ">":
      return left_operand > right_operand;
      break;

    case "<=":
      return left_operand <= right_operand;
      break;

    case ">=":
      return left_operand >= right_operand;
      break;
  }
}

export function is_comparing_numbers(
  left_operand: any,
  right_operand: any,
  operator: comparision_operator
) {
  const left_operand_type = left_operand instanceof BigNumber;
  const right_operand_type = right_operand instanceof BigNumber;

  if (left_operand_type && right_operand_type) {
    return true;
  } else {
    return false;
  }
}

export function compare_numbers(
  left_operand: BigNumber,
  right_operand: BigNumber,
  operator: comparision_operator
) {
  switch (operator) {
    case "=":
      return left_operand.isEqualTo(right_operand);
      break;

    case "!=":
      return !left_operand.isEqualTo(right_operand);
      break;

    case "<":
      return left_operand.isLessThan(right_operand);
      break;

    case ">":
      return left_operand.isGreaterThan(right_operand);
      break;

    case ">=":
      return left_operand.isGreaterThanOrEqualTo(right_operand);
      break;

    case "<=":
      return left_operand.isLessThanOrEqualTo(right_operand);
      break;
  }
}

export function is_comparing_string_and_number(
  left_operand: any,
  right_operand: any,
  operator: comparision_operator
) {
  const left_operand_type = typeof left_operand;
  const right_operand_type = typeof right_operand;
  if (left_operand_type === "object" && right_operand_type === "string") {
    return true;
  } else if (
    left_operand_type === "string" &&
    right_operand_type === "object"
  ) {
    return true;
  } else {
    return false;
  }
}

export function compare_string_and_number(
  left_operand: any,
  right_operand: any,
  operator: comparision_operator
) {
  switch (operator) {
    case "=":
      return false;
      break;

    case "!=":
      return true;
      break;

    case "<":
      if (typeof left_operand === "string") {
        return false;
      } else {
        return true;
      }
      break;

    case ">":
      if (typeof left_operand === "string") {
        return true;
      } else {
        return false;
      }
      break;

    case "<=":
      if (typeof left_operand === "string") {
        return false;
      } else {
        return true;
      }
      break;

    case ">=":
      if (typeof left_operand === "string") {
        return true;
      } else {
        return false;
      }
      break;
  }
}

export function resolve_compare(
  left_operand: any,
  right_operand: any,
  operator: comparision_operator
) {
  if (is_comparing_string(left_operand, right_operand, operator)) {
    // is comparing the dates
    if (is_comparing_dates(left_operand, right_operand, operator)) {
      // comparing the dates
      return compare_dates(left_operand, right_operand, operator);
    } else {
      return compare_strings(left_operand, right_operand, operator);
    }
  } else if (is_comparing_numbers(left_operand, right_operand, operator)) {
    // use bignumber.js to compare the number
    return compare_numbers(left_operand, right_operand, operator);
  } else if (
    is_comparing_string_and_number(left_operand, right_operand, operator)
  ) {
    return compare_string_and_number(left_operand, right_operand, operator);
  }
}

export function remove_char_form_end(
  start_char: string,
  end_char: string,
  arr: string[],
  cb: (result: string[]) => void
) {
  const found_final_index = (
    start_c: string,
    end_c: string,
    str_arr: string[]
  ) => {
    let found_initial_index: number = 0;
    let found_final_indexx: any = null;

    for (let index = 1; index < str_arr.length; index++) {
      const literal = str_arr[index];
      if (literal === start_c) {
        found_initial_index = found_initial_index + 1;
      }
      if (literal === end_c) {
        if (found_initial_index === 0) {
          if (index === str_arr.length - 1) {
            found_final_indexx = index;
            break;
          }
        } else {
          found_initial_index = found_initial_index - 1;
        }
      }
    }

    return found_final_indexx;
  };

  if (arr[0] === start_char) {
    // need to remove this
    const final_index = found_final_index(start_char, end_char, arr);
    // remove the char
    if (final_index !== null) {
      arr.splice(final_index, 1);
      // remove the first char
      arr.splice(0, 1);
      //  call this function again
      remove_char_form_end(start_char, end_char, arr, cb);
    } else {
      cb(arr);
    }
  } else {
    cb(arr);
  }
}

// trim the chracter from the ends of the string
export async function generalTrim(str: string, character: generalTrimOption) {
  const removed_arr = (end_character: string) => {
    return new Promise<string[]>((resolve, reject) => {
      remove_char_form_end(
        character,
        end_character,
        str.trim().split(""),
        (result) => resolve(result)
      );
    });
  };

  switch (character) {
    case "(":
      // remove all the ()
      return (await removed_arr(")")).join("").trim();

    case "[":
      return (await removed_arr("]")).join("").trim();

    case "{":
      return (await removed_arr("}")).join("").trim();
  }
}

export function is_two_obejct_type_equal(obj_1: any, obj_2: any) {
  let is_object_different = false;
  if (Object.keys(obj_1).length === Object.keys(obj_2).length) {
    for (let key in obj_1) {
      if (obj_1.hasOwnProperty(key) && obj_2.hasOwnProperty(key)) {
        if (typeof obj_1[key] === typeof obj_2[key]) {
          is_object_different = false;
        } else {
          is_object_different = true;
          break;
        }
      } else {
        is_object_different = true;
        break;
      }
    }
  } else {
    is_object_different = true;
  }

  return is_object_different ? false : true;
}

export function is_two_obejct_equal(obj_1: any, obj_2: any) {
  let is_object_different = false;

  if (Object.keys(obj_1).length === Object.keys(obj_2).length) {
    for (let key in obj_1) {
      if (obj_2.hasOwnProperty(key) && obj_1.hasOwnProperty(key)) {
        // both object contain this key
        const value_1 = obj_1[key];
        const value_2 = obj_2[key];
        if (value_1 === value_2) {
          is_object_different = false;
        } else {
          is_object_different = true;
          break;
        }
      } else {
        is_object_different = true;
        break;
      }
    }
  } else {
    is_object_different = true;
  }

  return is_object_different ? false : true;
}

export function filter_unique_object_in_array(data: any[]) {
  const unique_objects: any[] = [];

  const find_unique = (arr: any[], cb: () => void) => {
    const copy_object = JSON.parse(JSON.stringify(arr[0]));

    for (let index = 0; index < arr.length; index++) {
      if (is_two_obejct_equal(copy_object, arr[index])) {
        arr[index] = null;
      }
    }

    arr = arr.filter((p) => p !== null);

    unique_objects.push(copy_object);

    if (arr.length !== 0) {
      find_unique(arr, cb);
    } else {
      cb();
    }
  };

  return new Promise<any[]>((resolve, reject) => {
    find_unique(data, () => resolve(unique_objects));
  });
}

export async function union_maker(
  upper_data_i: any[],
  lower_data_i: any[],
  union_type: "union_all" | "union"
) {
  // The UNION operator selects only distinct values by default. To allow duplicate values, use UNION ALL

  // we need make the key name same for all the object lower plus upper
  // use reference object which is upper_data[0]

  const reference_object = upper_data_i[0];
  // modify the upper_data
  const upper_data = upper_data_i.map((val) => {
    if (Object.keys(val).length === Object.keys(reference_object).length) {
      const keys_of_val = Object.keys(val);
      const final_object: any = {};
      Object.keys(reference_object).forEach((key_ref, ref_index) => {
        final_object[key_ref] = val[keys_of_val[ref_index]];
      });
      return final_object;
    } else {
      // throw the error
      throw new Error(
        "Object type and length should be equal when using union"
      );
    }
  });

  const lower_data = lower_data_i.map((val) => {
    if (Object.keys(val).length === Object.keys(reference_object).length) {
      const keys_of_val = Object.keys(val);
      const final_object: any = {};
      Object.keys(reference_object).forEach((key_ref, ref_index) => {
        final_object[key_ref] = val[keys_of_val[ref_index]];
      });
      return final_object;
    } else {
      // throw the error
      throw new Error(
        "Object type and length should be equal when using union"
      );
    }
  });

  if (upper_data.length !== 0 && lower_data.length !== 0) {
    // both upper and lower data is not empty
    if (union_type === "union") {
      if (is_two_obejct_type_equal(upper_data[0], lower_data[0])) {
        const uniue_objects = await filter_unique_object_in_array([
          ...upper_data,
          ...lower_data,
        ]);
        return uniue_objects;
      } else {
        throw Error("union problem");
      }
    } else if (union_type === "union_all") {
      if (is_two_obejct_type_equal(upper_data[0], lower_data[0])) {
        return [...upper_data, ...lower_data];
      } else {
        throw Error("union problem");
      }
    }
  } else if (upper_data.length !== 0 && lower_data.length === 0) {
    if (union_type === "union") {
      const uniue_objects = await filter_unique_object_in_array([
        ...upper_data,
        ...lower_data,
      ]);
      return uniue_objects;
    } else if (union_type === "union_all") {
      return [...upper_data, ...lower_data];
    }
  } else if (upper_data.length === 0 && lower_data.length !== 0) {
    if (union_type === "union") {
      const uniue_objects = await filter_unique_object_in_array([
        ...upper_data,
        ...lower_data,
      ]);
      return uniue_objects;
    } else if (union_type === "union_all") {
      return [...upper_data, ...lower_data];
    }
  } else if (upper_data.length === 0 && lower_data.length === 0) {
    return [];
  } else {
    throw Error("union error");
  }
}

export function group_by<T>(data: T[], key: string) {
  const grouped_values: T[] = [];

  const group_now = (
    arr: any[],
    key_name: string,
    cb: (result: any[]) => void
  ) => {
    // make copy of the first element
    const initial_copy = JSON.parse(JSON.stringify(arr[0]));
    // loop through the arr to find the match
    for (let index = 0; index < arr.length; index++) {
      const obj = arr[index];
      if (
        obj.hasOwnProperty(key_name) &&
        initial_copy.hasOwnProperty(key_name)
      ) {
        // chekc of the value is same
        if (obj[key_name] === initial_copy[key_name]) {
          // matched the value
          arr[index] = null;
        }
      }
    }

    arr = arr.filter((p) => p !== null);
    grouped_values.push(initial_copy);

    if (arr.length !== 0) {
      // call func again
      group_now(arr, key_name, cb);
    } else {
      cb(grouped_values);
    }
  };

  return new Promise<T[]>((resolve, reject) => {
    group_now(data, key, (result) => resolve(result));
  });
}

// sort the array

export function sort_array_with_obj(
  arr: any,
  key_name: string,
  type: "ASC" | "DESC"
) {
  if (arr.length === 0) {
    return;
  }

  if (type === "ASC") {
    const first_obj = arr[0];
    if (typeof first_obj[key_name] === "number") {
      return arr.sort((a: any, b: any) => a[key_name] - b[key_name]);
    } else if (typeof first_obj[key_name] === "string") {
      // check if the string is date
      if (is_string_date(first_obj[key_name])) {
        // string is date
        return arr.sort(
          (a: any, b: any) => +new Date(a[key_name]) - +new Date(b[key_name])
        );
      } else {
        return arr.sort((a: any, b: any) => a[key_name].localeCompare(b[key_name]));
      }
    } else {
      return arr;
    }
  } else if (type === "DESC") {
    const first_obj = arr[0];
    if (typeof first_obj[key_name] === "number") {
      return arr.sort((a: any, b : any) => b[key_name] - a[key_name]);
    } else if (typeof first_obj[key_name] === "string") {
      // check if the string is date
      if (is_string_date(first_obj[key_name])) {
        // string is date
        return arr.sort(
          (a: any, b: any) => +new Date(b[key_name]) - +new Date(a[key_name])
        );
      } else {
        return arr.sort((a: any, b: any) => b[key_name].localeCompare(a[key_name]));
      }
    } else {
      return arr;
    }
  }
}

export function sort_similar_obj_in_array(
  arr: any[],
  similar_key: string,
  key_name: string,
  type: "ASC" | "DESC"
) {
  let initial_index = -1;
  const similar_ele: any[] = [];

  for (let index = 0; index < arr.length; index++) {
    const obj = arr[index];
    if (similar_ele.length === 0) {
      similar_ele.push(obj);
      initial_index = index;
    } else if (
      similar_ele[similar_ele.length - 1][similar_key] === obj[similar_key]
    ) {
      similar_ele.push(obj);
    } else {
      if (similar_ele.length === 1) {
        similar_ele.push(obj);
        initial_index = index;
      } else {
        break;
      }
    }
  }

  if (similar_ele.length !== 0 && initial_index > -1) {
    // found similar sort this
    const sorted_items: any = sort_array_with_obj(similar_ele, key_name, type);
    // remove and replace the sorted array in original
    arr.splice(initial_index, similar_ele.length, ...sorted_items);
    return arr;
  } else {
    return arr;
  }
}

export function sort_by(
  data: any,
  keys: { key_name: string; type: "ASC" | "DESC" }[]
) {
  for (let index = 0; index < keys.length; index++) {
    const key = keys[index];

    if (index === 0) {
      data = sort_array_with_obj(data, key.key_name, key.type);
    } else {
      const prev_key = keys[index - 1];
      data = sort_similar_obj_in_array(
        data,
        prev_key.key_name,
        key.key_name,
        key.type
      );
    }
  }

  return data;
}

// unions
export async function unite_sources(sources_data: any[][], union_types: any[]) {
  const combine = async (
    sources: any,
    unions: any[],
    cb: (result: any[]) => void
  ) => {
    if (sources.length - 1 === unions.length) {
      if (sources.length === 1) {
        cb(sources[0]);
      } else {
        const upper_data = sources[0];
        const lower_data = sources[1];
        const type = unions[0] as {
          type: "union";
          data: "UNION ALL" | "UNION";
        };
        // find the result of union
        const united_data = await union_maker(
          upper_data,
          lower_data,
          type.data.toLowerCase().split(" ").join("_") as any
        );
        sources.splice(0, 2, united_data);
        unions.splice(0, 1);
        // call this func again
        combine(sources, unions, cb);
      }
    } else {
      throw Error("union problem");
    }
  };

  return new Promise<any[]>((resolve, reject) =>
    combine(sources_data, union_types, (result) => resolve(result))
  );
}

// parse order by

export function parse_order_by(
  order_by_literals: string[],
  is_multi_query = false
) {
  const literals = order_by_literals.slice(1);
  const sort_by_types = ["ASC", "DESC"];

  return literals
    .join(" ")
    .split(" , ")
    .map((p, i) => p.split(" "))
    .map((p, i) => {
      if (p.length === 1) {
        if (is_multi_query) {
          // multi data source
          const column_name = p[0].trim(); // this is column name
          if (
            !sort_by_types.includes(column_name.toUpperCase()) &&
            typeof column_name === "string"
          ) {
            return { key_name: column_name, type: "ASC" };
          } else {
            return null;
          }
        } else {
          // for single data source query need to remove any . alies
          const column_name_splited = p[0].trim().split("."); // this is column name
          const column_name =
            column_name_splited[column_name_splited.length - 1];
          if (
            !sort_by_types.includes(column_name.toUpperCase()) &&
            typeof column_name === "string"
          ) {
            return { key_name: column_name, type: "ASC" };
          } else {
            return null;
          }
        }
      } else if (p.length === 2) {
        if (is_multi_query) {
          const column_name = p[0].trim();
          const sort_type = p[1].trim().toUpperCase();
          if (
            sort_by_types.includes(sort_type) &&
            typeof column_name === "string"
          ) {
            return { key_name: column_name, type: sort_type };
          } else {
            return null;
          }
        } else {
          const column_name_splited = p[0].trim().split(".");
          const column_name =
            column_name_splited[column_name_splited.length - 1];
          const sort_type = p[1].trim().toUpperCase();
          if (
            sort_by_types.includes(sort_type) &&
            typeof column_name === "string"
          ) {
            return { key_name: column_name, type: sort_type };
          } else {
            return null;
          }
        }
      } else {
        return null;
      }
    })
    .filter((p) => p !== null);
}

export function convert_to_number(str: string) {
  if (isNaN(Number(str))) {
    // pure string
    return str;
  } else {
    return new BigNumber(str);
  }
}

export function is_time_string(str: string) {
  const time_arr = str
    .trim()
    .split(":")
    .filter((p) => p !== "")
    .filter((p) => !isNaN(Number(p)));
  if (time_arr.length === 3) {
    return true;
  } else {
    return false;
  }
}

export function is_year_string(str: string) {
  if (str.length === 4 && !isNaN(Number(str))) {
    return true;
  } else {
    return false;
  }
}

export function table_schema_unique_string(
  database_name: string,
  table_name: string
) {
  return `${database_name}($_$)${table_name}`;
}

export async function retrive_next_database_version(database_name: string) {
  try {
    const database_connection = await openDB(indexed_sql_config_database, 1);
    const tnx = database_connection.transaction(
      indexed_sql_config_versions,
      "readonly"
    );
    const current_version = await tnx
      .objectStore(indexed_sql_config_versions)
      .get(database_name);
    await tnx.is_complete();
    database_connection.close();
    if (current_version) {
      return {
        current_version: +current_version,
        next_version: +current_version + 1,
      };
    } else {
      throw new Error("nothing in db version");
    }
  } catch (error) {
    return {
      current_version: 1,
      next_version: 1,
    };
  }
}

export async function upgrade_database_version(
  database_name: string,
  version: number
) {
  const database_connection = await openDB(indexed_sql_config_database, 1);
  const tnx = database_connection.transaction(
    indexed_sql_config_versions,
    "readwrite"
  );
  await tnx
    .objectStore(indexed_sql_config_versions)
    .put(version, database_name);
  await tnx.is_complete();
  database_connection.close();
  return "OK";
}

export async function setup_initial_config_tables() {
  // setup the
  const connection_db = await openDB(
    indexed_sql_config_database,
    1,
    (upgradeDB) => {
      if (
        !upgradeDB.objectStoreNames.contains(indexed_sql_config_schema_name)
      ) {
        // create the indexed_sql_config_schema_name table
        upgradeDB.createObjectStore(indexed_sql_config_schema_name);
      }

      if (!upgradeDB.objectStoreNames.contains(indexed_sql_config_versions)) {
        // create new indexed_sql_config_versions table
        upgradeDB.createObjectStore(indexed_sql_config_versions);
      }
    }
  );

  connection_db.close();
}

export async function detect_query_type(query: string) {
  const trimed_value = await generalTrim(query.trim(), "(");
  const first_element = trimed_value.trim().split(" ")[0].toUpperCase();

  switch (first_element) {
    case "SELECT":
      return "SELECT";

    case "UPDATE":
      return "UPDATE";

    case "DELETE":
      return "DELETE";

    case "INSERT":
      return "INSERT";

    case "CREATE":
      return "CREATE";
  }
}

export function convert_sql_string_to_string(sql_str: string) {
  return sql_str.slice(1, sql_str.length - 1);
}

export const convert_the_normal_value_to_sql_value = (value: any) => {
  if (
    value === null ||
    value === "NULL" ||
    value === undefined ||
    value === "null"
  ) {
    return "NULL";
  } else if (is_number(value)) {
    return new BigNumber(value);
  } else if (typeof value === "string") {
    if (is_string_date(value.split("T")[0])) {
      // string is date
      return `'${moment(value).format("YYYY-MM-DD HH:mm:ss")}'`;
    } else {
      return `'${value}'`;
    }
  }
};

export const convert_sql_object_to_normal_object = (obj_sql: any) => {
  const normal_object: any = {};
  Object.keys(obj_sql).forEach((k) => {
    const value = obj_sql[k];
    if (value === "NULL") {
      normal_object[k] = null;
    } else if (is_number(value)) {
      normal_object[k] = +value;
    } else if (is_pure_sql_string(value)) {
      normal_object[k] = convert_sql_string_to_string(value);
    }
  });
  return normal_object;
};

export const convert_sql_data_to_normal_data = (data: any) => {
  return data.map((p: any) => convert_sql_object_to_normal_object(p));
};
